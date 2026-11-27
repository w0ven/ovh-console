import assert from "node:assert/strict";
import { after, test } from "node:test";
import { fileURLToPath } from "node:url";
import { createElement, act } from "react";
import { createServer } from "vite";
import { Window } from "happy-dom";

// 必须在首次加载 @tanstack/react-query、@radix-ui 与 react-dom/client 之前挂载 DOM 全局环境：
// @tanstack/query-core 在模块顶层求值 `isServer = typeof window === "undefined"`，
// 若静态 import 早于挂载 window，会导致 environmentManager.isServer() 永久为 true 而禁用所有 refetchInterval 定时器。
const win = new Window({ url: "https://ovh.example.com/" });
const domGlobals = {
  window: win,
  document: win.document,
  HTMLElement: win.HTMLElement,
  HTMLInputElement: win.HTMLInputElement,
  HTMLButtonElement: win.HTMLButtonElement,
  SVGElement: win.SVGElement,
  Event: win.Event,
  CustomEvent: win.CustomEvent,
  MouseEvent: win.MouseEvent,
  KeyboardEvent: win.KeyboardEvent,
  FocusEvent: win.FocusEvent,
  Node: win.Node,
  NodeFilter: win.NodeFilter,
  Element: win.Element,
  DocumentFragment: win.DocumentFragment,
  MutationObserver: win.MutationObserver,
  ResizeObserver: class {
    observe() {}
    unobserve() {}
    disconnect() {}
  },
  getComputedStyle: win.getComputedStyle.bind(win),
  IS_REACT_ACT_ENVIRONMENT: true,
};
Object.assign(globalThis, domGlobals);

const { QueryClient, QueryClientProvider } = await import("@tanstack/react-query");
const { createRoot } = await import("react-dom/client");

const server = await createServer({
  configFile: false,
  root: fileURLToPath(new URL("..", import.meta.url)),
  resolve: { alias: { "@": fileURLToPath(new URL("../src", import.meta.url)) } },
  server: { middlewareMode: true, hmr: false, ws: false },
  appType: "custom",
});

after(async () => {
  await server.close();
  await win.happyDOM.close();
});

test("mounted UpdateButton and VersionUpdateTrigger auto-poll after first-fetch failure, never render stale cached progress, and allocate distinct global runIds", async () => {
  const { UpdateButton } = await server.ssrLoadModule("/src/components/common/UpdateButton.tsx");
  const { VersionUpdateTrigger, resolveUpdateProgressTransition } = await server.ssrLoadModule(
    "/src/components/common/VersionUpdateTrigger.tsx",
  );
  const { api } = await server.ssrLoadModule("/src/lib/api.ts");
  const { default: i18n } = await server.ssrLoadModule("/src/i18n/index.ts");

  assert.equal(resolveUpdateProgressTransition({ started: false, waitingRestart: false, phase: "restarting" }), "none");
  assert.equal(resolveUpdateProgressTransition({ started: true, waitingRestart: false, phase: "restarting" }), "enter_waiting");
  assert.equal(resolveUpdateProgressTransition({ started: true, waitingRestart: false, phase: "idle" }), "enter_waiting");
  assert.equal(resolveUpdateProgressTransition({ started: true, waitingRestart: false, phase: "done" }), "enter_waiting");
  assert.equal(resolveUpdateProgressTransition({ started: true, waitingRestart: false, phase: "failed" }), "enter_failed");

  const originalAdapter = api.defaults.adapter;
  const originalLang = i18n.language;
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

  let activeRoots = [];
  const mount = (el) => {
    const root = createRoot(el);
    activeRoots.push({ root, el });
    return root;
  };
  const unmount = async (root, el) => {
    await act(async () => {
      root.unmount();
    });
    el.remove();
    activeRoots = activeRoots.filter((r) => r.root !== root);
  };

  let statusBehavior = async () => ({
    phase: "failed",
    message: "aborted",
    percent: 0,
    version: "0.4.38",
    error: "第一轮排空超时中止",
  });

  // 整个测试模拟生产唯一共享 QueryClient，绝不手动调用 sharedQC.refetchQueries()，
  // 真实依赖 useUpdateProgress 自身的 refetchInterval（由 progressPollMs=30 注入）驱动首次失败后的自动轮询恢复。
  // queries.gcTime=Infinity 保证预填缓存在测试期间保留，mutations.gcTime=Infinity 避免 300s Mutation GC 定时器挂住 Node，
  // finally 中通过 sharedQC.clear() 立即释放全部缓存与定时器。
  const sharedQC = new QueryClient({
    defaultOptions: {
      queries: { retry: false, gcTime: Infinity },
      mutations: { gcTime: Infinity },
    },
  });
  const activeRunId = () => {
    const active = sharedQC
      .getQueryCache()
      .getAll()
      .find(
        (q) =>
          Array.isArray(q.queryKey) &&
          q.queryKey[0] === "app" &&
          q.queryKey[1] === "update-progress" &&
          typeof q.queryKey[2] === "number" &&
          q.queryKey[2] > 0 &&
          q.getObserversCount() > 0,
      );
    return active ? active.queryKey[2] : null;
  };

  try {
    await i18n.changeLanguage("zh");
    const check = {
      current: "0.4.37",
      latest: "0.4.38",
      tag: "v0.4.38",
      name: "v0.4.38",
      hasUpdate: true,
      url: "https://github.com/w0ven/ovh-console/releases/tag/v0.4.38",
      publishedAt: "2026-11-22T00:00:00Z",
      body: "Release notes",
      prerelease: false,
      checkedAt: "2026-11-22T00:00:00Z",
      inContainer: false,
    };

    sharedQC.setQueryData(["app", "version"], "0.4.37");
    sharedQC.setQueryData(["app", "update-check"], check);
    sharedQC.setQueryData(["app", "queue"], []);

    api.defaults.adapter = async (config) => {
      const url = config.url || "";
      if (url === "/version/update" && config.method === "post") {
        return { data: { success: true, latest: "0.4.38", message: "started" }, status: 200, statusText: "OK", headers: {}, config };
      }
      if (url === "/version/update/status") {
        const data = await statusBehavior();
        return { data, status: 200, statusText: "OK", headers: {}, config };
      }
      if (url === "/version") {
        return { data: { version: "0.4.37" }, status: 200, statusText: "OK", headers: {}, config };
      }
      if (url === "/version/check-update") {
        return { data: check, status: 200, statusText: "OK", headers: {}, config };
      }
      if (url === "/queue") {
        return { data: [], status: 200, statusText: "OK", headers: {}, config };
      }
      return { data: {}, status: 200, statusText: "OK", headers: {}, config };
    };

    // === [A] 入口 1：UpdateButton 在共享 QueryClient 上完成首轮启动并失败（记录实际 runIdA） ===
    const container1 = win.document.createElement("div");
    win.document.body.appendChild(container1);
    const root1 = mount(container1);
    await act(async () => {
      root1.render(
        createElement(QueryClientProvider, { client: sharedQC }, createElement(UpdateButton, { check, progressPollMs: 30 })),
      );
    });
    const startBtn = [...container1.querySelectorAll("button")].find((b) =>
      (b.textContent || "").includes("立即更新"),
    );
    assert.ok(startBtn, "UpdateButton should render update button");
    await act(async () => {
      startBtn.click();
    });
    let runIdA = null;
    for (let i = 0; i < 30; i++) {
      if (runIdA === null) runIdA = activeRunId();
      if ((container1.textContent || "").includes("第一轮排空超时中止")) break;
      await act(async () => {
        await sleep(15);
      });
    }
    assert.equal(runIdA, 1, "first run must allocate global runId=1");
    assert.match(container1.textContent || "", /第一轮排空超时中止/);
    await unmount(root1, container1);

    // 刻意在 sharedQC 中为即将分配的 runId=2 预填旧的 percent=91% + 旧 runId=1 的 failed 缓存
    // （用 Infinity gcTime 保留至组件挂载，避免 gcTime: 0 在无 observer 时瞬间回收，finally 中 sharedQC.clear() 统一释放）
    sharedQC.setQueryData(["app", "update-progress", 1], {
      phase: "failed",
      message: "aborted",
      percent: 0,
      version: "0.4.38",
      error: "旧入口 runId=1 的残留失败",
    });
    sharedQC.setQueryData(["app", "update-progress", 2], {
      phase: "downloading",
      message: "stale 91% cache",
      percent: 91,
      version: "0.4.38",
    });

    // === [B] 入口 2：VersionUpdateTrigger 在共享 QueryClient 上首次启动（必须分配 runIdB=2） ===
    // 门闸 1（firstPollGate）先挂起首请求，断言 pending 期明确显示 0% 且不渲染 91%；
    // 放行门闸 1 后首请求抛错，门闸 2（secondPollGate）挂起第二次自动轮询，断言错误期仍明确显示 0% 且不渲染 91%；
    // 放行门闸 2 后不调用 refetchQueries，完全依靠 refetchInterval 自动恢复到 68%。
    let triggerStatusCalls = 0;
    let releaseFirstPoll;
    const firstPollGate = new Promise((r) => {
      releaseFirstPoll = r;
    });
    let releaseSecondPoll;
    const secondPollGate = new Promise((r) => {
      releaseSecondPoll = r;
    });
    statusBehavior = async () => {
      triggerStatusCalls += 1;
      if (triggerStatusCalls === 1) {
        await firstPollGate;
        throw new Error("transient network error on first status poll");
      }
      await secondPollGate;
      return { phase: "downloading", message: "downloading", percent: 68, version: "0.4.38" };
    };

    const container2 = win.document.createElement("div");
    win.document.body.appendChild(container2);
    const root2 = mount(container2);
    const findButtonByText = (textRe) =>
      Array.from(win.document.querySelectorAll("button")).find((b) => textRe.test(b.textContent || ""));

    await act(async () => {
      root2.render(
        createElement(QueryClientProvider, { client: sharedQC }, createElement(VersionUpdateTrigger, { progressPollMs: 30 })),
      );
      await sleep(20);
    });
    assert.equal(container2.querySelectorAll(".animate-spin").length, 0);

    await act(async () => {
      container2.querySelector("button").click();
      await sleep(30);
    });
    await act(async () => {
      findButtonByText(/更新到 v0\.4\.38/).click();
      await sleep(15);
    });
    await act(async () => {
      findButtonByText(/立即更新/).click();
      await sleep(20);
    });

    const runIdB = activeRunId();
    assert.equal(runIdB, 2, "second run in VersionUpdateTrigger must allocate global runId=2");
    assert.equal(triggerStatusCalls, 1, "first poll must be in-flight at pending gate");
    // 时间点 1（首请求挂起 pending 期）：必须明确显示 0%，绝不渲染预填的 91%
    assert.match(win.document.body.textContent || "", /下载中\s*0%/);
    assert.doesNotMatch(win.document.body.textContent || "", /91%/);
    assert.doesNotMatch(win.document.body.textContent || "", /旧入口 runId=1 的残留失败/);
    assert.doesNotMatch(win.document.body.textContent || "", /正在重启/);

    // 放行首请求使其抛错，显式等待该 runId 的 QueryState 真实变为 status === "error"（绝不依赖固定 20ms sleep）
    releaseFirstPoll();
    for (let i = 0; i < 40; i++) {
      if (sharedQC.getQueryState(["app", "update-progress", runIdB])?.status === "error") break;
      await act(async () => {
        await sleep(10);
      });
    }
    assert.equal(
      sharedQC.getQueryState(["app", "update-progress", runIdB])?.status,
      "error",
      "VersionUpdateTrigger progress query must have deterministically transitioned to error state",
    );
    // 时间点 2（已确立处于 error 状态、且第二次响应仍被门闸阻塞）：必须仍明确显示 0%，绝不渲染 91%
    assert.match(win.document.body.textContent || "", /下载中\s*0%/);
    assert.doesNotMatch(win.document.body.textContent || "", /91%/);
    assert.doesNotMatch(win.document.body.textContent || "", /旧入口 runId=1 的残留失败/);
    assert.doesNotMatch(win.document.body.textContent || "", /正在重启/);

    // 放行第 2 次真实自动轮询（不调用任何 refetchQueries），验证 refetchInterval 自动恢复到 68%
    releaseSecondPoll();
    for (let i = 0; i < 40; i++) {
      if ((win.document.body.textContent || "").includes("68%")) break;
      await act(async () => {
        await sleep(20);
      });
    }
    assert.ok(triggerStatusCalls >= 2, "refetchInterval must automatically fire second poll after first failure");
    assert.match(win.document.body.textContent || "", /68%/);
    await unmount(root2, container2);

    // === [C] 重挂载 UpdateButton 在同一 sharedQC 上启动（必须分配 runIdC=3） ===
    // 为即将使用的 runId=3 预填 percent=91% 缓存，同时设置首请求挂起门闸与第二次自动轮询门闸：
    // 验证时间点 1（首请求 pending）与时间点 2（首请求失败后等待第 2 次）均明确显示 0% 且绝不显示 91%，
    // 放行后由 refetchInterval 自动恢复到 55%。
    sharedQC.setQueryData(["app", "update-progress", 3], {
      phase: "downloading",
      message: "stale 91% cache for UpdateButton",
      percent: 91,
      version: "0.4.38",
    });
    let remountStatusCalls = 0;
    let releaseRemountFirstPoll;
    const remountFirstGate = new Promise((r) => {
      releaseRemountFirstPoll = r;
    });
    let releaseRemountSecondPoll;
    const remountSecondGate = new Promise((r) => {
      releaseRemountSecondPoll = r;
    });
    statusBehavior = async () => {
      remountStatusCalls += 1;
      if (remountStatusCalls === 1) {
        await remountFirstGate;
        throw new Error("transient 502 on remount first poll");
      }
      await remountSecondGate;
      return { phase: "downloading", message: "downloading", percent: 55, version: "0.4.38" };
    };

    const container3 = win.document.createElement("div");
    win.document.body.appendChild(container3);
    const root3 = mount(container3);
    await act(async () => {
      root3.render(
        createElement(QueryClientProvider, { client: sharedQC }, createElement(UpdateButton, { check, progressPollMs: 30 })),
      );
    });
    const startBtn3 = [...container3.querySelectorAll("button")].find((b) =>
      (b.textContent || "").includes("立即更新"),
    );
    assert.ok(startBtn3, "expected UpdateButton immediate-update button in phase 3");
    await act(async () => {
      startBtn3.click();
      await sleep(20);
    });

    const runIdC = activeRunId();
    assert.equal(runIdC, 3, "third run after remount must allocate global runId=3");
    assert.equal(new Set([runIdA, runIdB, runIdC]).size, 3, "all three runs must have globally distinct query keys");
    assert.equal(remountStatusCalls, 1, "remount first poll must be in-flight at pending gate");
    // 时间点 1（首请求挂起 pending 期）：明确断言显示 0% 且绝不渲染 91%
    assert.match(container3.textContent || "", /下载中\s*0%/);
    assert.doesNotMatch(container3.textContent || "", /91%/);
    assert.doesNotMatch(container3.textContent || "", /旧入口 runId=1 的残留失败/);
    assert.doesNotMatch(container3.textContent || "", /正在重启/);

    // 放行首请求使其抛错，显式等待该 runId 的 QueryState 真实变为 status === "error"（绝不依赖固定 20ms sleep）
    releaseRemountFirstPoll();
    for (let i = 0; i < 40; i++) {
      if (sharedQC.getQueryState(["app", "update-progress", runIdC])?.status === "error") break;
      await act(async () => {
        await sleep(10);
      });
    }
    assert.equal(
      sharedQC.getQueryState(["app", "update-progress", runIdC])?.status,
      "error",
      "UpdateButton progress query must have deterministically transitioned to error state",
    );
    // 时间点 2（已确立处于 error 状态、且第二次响应仍被门闸阻塞）：必须仍明确显示 0%，绝不渲染 91%
    assert.match(container3.textContent || "", /下载中\s*0%/);
    assert.doesNotMatch(container3.textContent || "", /91%/);
    assert.doesNotMatch(container3.textContent || "", /旧入口 runId=1 的残留失败/);
    assert.doesNotMatch(container3.textContent || "", /正在重启/);

    // 放行自动轮询，绝不调用 refetchQueries，断言 refetchInterval 自动恢复渲染 55%
    releaseRemountSecondPoll();
    for (let i = 0; i < 40; i++) {
      if ((container3.textContent || "").includes("55%")) break;
      await act(async () => {
        await sleep(20);
      });
    }
    assert.ok(remountStatusCalls >= 2, "UpdateButton refetchInterval must automatically recover after first failure");
    assert.match(container3.textContent || "", /55%/);
    await unmount(root3, container3);

    // —— 阶段 4：持续失败自动转入 waitingRestart，并严格验证“绝对墙钟截止时间”能中断慢/挂起的在途探测 ——
    // 模拟 POST /version/update 成功后：
    // 1) /version/update/status 连续 3 次报错 -> 自动转入 waitingRestart（显示“正在重启…”）
    // 2) 进入 waitingRestart 后，探针的 /version/update/status 请求刻意永不主动返回（仅监听 signal.abort），
    //    配合 restartDeadlineMs=260ms、restartPollIntervalMs=20ms，严格断言在绝对截止时间到期后
    //    立刻中止挂起的网络探测（abortedCount >= 1）并退出 waitingRestart 显示超时错误，总耗时远小于 90 次串行轮询！
    let failCount = 0;
    let slowProbeStarted = 0;
    let slowProbeAborted = 0;
    const t0 = Date.now();
    api.defaults.adapter = async (config) => {
      const url = config.url || "";
      if (url === "/version/update" && config.method === "post") {
        return { data: { success: true, latest: "0.4.39", message: "started" }, status: 200, statusText: "OK", headers: {}, config };
      }
      if (url === "/version/update/status") {
        if (failCount < 3) {
          failCount += 1;
          throw new Error("ECONNRESET during fast restart");
        }
        // 进入 waitingRestart 后的慢/永挂探测：只响应 AbortSignal
        slowProbeStarted += 1;
        return await new Promise((_, reject) => {
          const sig = config.signal;
          if (sig?.aborted) {
            slowProbeAborted += 1;
            reject(new Error("aborted"));
            return;
          }
          sig?.addEventListener(
            "abort",
            () => {
              slowProbeAborted += 1;
              reject(new Error("aborted by deadline"));
            },
            { once: true },
          );
        });
      }
      if (url === "/version") {
        return { data: { version: "0.4.37" }, status: 200, statusText: "OK", headers: {}, config };
      }
      return { data: {}, status: 200, statusText: "OK", headers: {}, config };
    };
    const container4 = win.document.createElement("div");
    win.document.body.appendChild(container4);
    const root4 = mount(container4);
    const check4 = { ...check, latest: "0.4.39", tag: "v0.4.39", name: "v0.4.39" };
    await act(async () => {
      root4.render(
        createElement(
          QueryClientProvider,
          { client: sharedQC },
          createElement(UpdateButton, {
            check: check4,
            progressPollMs: 25,
            restartDeadlineMs: 450,
            restartPollIntervalMs: 20,
          }),
        ),
      );
    });
    const btn4 = [...container4.querySelectorAll("button")].find((b) =>
      (b.textContent || "").includes("立即更新"),
    );
    assert.ok(btn4, "expected UpdateButton button in phase 4");
    await act(async () => {
      btn4.click();
      await sleep(10);
    });
    let sawRestarting = false;
    let sawTimeoutError = false;
    for (let i = 0; i < 80; i++) {
      const txt = container4.textContent || "";
      if (txt.includes("正在重启")) sawRestarting = true;
      if (txt.includes("服务重启超时") || txt.includes("timed out")) {
        sawTimeoutError = true;
        break;
      }
      await act(async () => {
        await sleep(15);
      });
    }
    const elapsedMs = Date.now() - t0;
    assert.ok(failCount >= 3, `expected >=3 consecutive status failures before fallback, got ${failCount}`);
    assert.ok(sawRestarting, "expected UpdateButton to transition into waitingRestart after 3 status errors");
    assert.ok(slowProbeStarted >= 1, "expected waitingRestart to dispatch a slow probe request");
    assert.ok(slowProbeAborted >= 1, "expected absolute wall-clock deadline to abort the hanging in-flight probe");
    assert.ok(sawTimeoutError, `expected restart timeout error UI after deadline, got: ${container4.textContent}`);
    assert.ok(elapsedMs < 1500, `expected bounded deadline exit well under 1500ms, got ${elapsedMs}ms`);
    await unmount(root4, container4);
  } finally {
    for (const { root, el } of activeRoots) {
      try {
        await act(async () => {
          root.unmount();
        });
      } catch {}
      el.remove();
    }
    sharedQC.clear();
    api.defaults.adapter = originalAdapter;
    await i18n.changeLanguage(originalLang);
  }
});
