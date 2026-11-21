import { useEffect, useRef, useState } from "react";
import {
  AlertTriangle,
  ArrowRight,
  Check,
  Copy,
  Download,
  ExternalLink,
  Loader2,
  RefreshCw,
  Sparkles,
} from "lucide-react";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { api } from "@/lib/api";
import { cn } from "@/lib/utils";
import {
  allocateUpdateProgressRunId,
  useAppVersion,
  useSelfUpdate,
  useUpdateCheck,
  useUpdateProgress,
  type UpdateCheck,
  type UpdateProgress,
} from "@/hooks/use-system-metrics";
import { useQueueList } from "@/hooks/use-queue";

const CONTAINER_LATEST_CMD = "docker compose pull && docker compose up -d";

export type UpdateCheckViewState = "loading" | "error" | "staleError" | "available" | "upToDate" | "unknown";

export function deriveUpdateViewState(params: {
  isPending: boolean;
  isError: boolean;
  check?: UpdateCheck;
}): UpdateCheckViewState {
  if (params.isPending && !params.check) return "loading";
  const hasRefreshErr = params.isError || Boolean(params.check?.lastError);
  if (hasRefreshErr) {
    // 已有上次成功快照时不得把失败静默伪装成“已是最新”，明确返回 staleError 让界面同时展示失败提示与上次快照
    if (params.check?.latest) return "staleError";
    return "error";
  }
  if (!params.check || !params.check.latest) return "unknown";
  return params.check.hasUpdate ? "available" : "upToDate";
}

export interface RestartPollState {
  phase: "waiting" | "updated" | "failed" | "timeout";
  version?: string;
  error?: string;
}

export const DEFAULT_RESTART_DEADLINE_MS = 90_000;
export const DEFAULT_RESTART_POLL_INTERVAL_MS = 1_000;

/**
 * 串行单步重启探针（纯异步决策函数）：
 * - 优先判断目标版本是否已上线（即使进程在两次轮询间秒级完成替换，`/version` 已回报新版本、`/status` 已重置为 `idle`，
 *   也能立即识别完成，绝不卡死在“更新中”）；
 * - 若 `/version/update/status` 明确回报 `failed`，立刻呈现后端中止/失败原因；
 * - 区分“单端点瞬时错误（如反代对 /status 偶发 502 但 /version 仍通）”与“真正服务失联（双端点均不可达）”，
 *   单端点错误绝不置 `nextSawDisconnect: true`，杜绝后续读到 `idle + 旧版` 时误报回滚；
 * - 若 `/version/update/status` 仍处于活跃更新阶段（downloading/verifying/installing/restarting），继续等待；
 * - 只有确认曾真正双端点失联、恢复后状态回到非活跃态却仍返回旧版本时，才判定为明确回滚证据。
 * - 支持传入 AbortSignal：当本轮绝对截止时间（90s）到期或组件卸载时立即中止在途网络请求。
 */
export async function probeRestartStep(opts: {
  targetVersion: string;
  sawDisconnect: boolean;
  signal?: AbortSignal;
  fetchVersion: (signal?: AbortSignal) => Promise<string | undefined>;
  fetchStatus: (signal?: AbortSignal) => Promise<UpdateProgress | undefined>;
}): Promise<{ nextSawDisconnect: boolean; outcome: RestartPollState }> {
  let status: UpdateProgress | undefined;
  let statusErr = false;
  try {
    status = await opts.fetchStatus(opts.signal);
  } catch {
    statusErr = true;
  }
  if (opts.signal?.aborted) {
    return { nextSawDisconnect: opts.sawDisconnect, outcome: { phase: "waiting" } };
  }

  if (status?.phase === "failed") {
    return {
      nextSawDisconnect: opts.sawDisconnect,
      outcome: { phase: "failed", error: status.error || status.message || "Update failed" },
    };
  }

  let now: string | undefined;
  let versionErr = false;
  try {
    now = await opts.fetchVersion(opts.signal);
  } catch {
    versionErr = true;
  }
  if (opts.signal?.aborted) {
    return { nextSawDisconnect: opts.sawDisconnect, outcome: { phase: "waiting" } };
  }

  // 新版本已上线：无论是否采样到 restarting/断连，第一时间确认更新成功
  if (now && (!opts.targetVersion || now === opts.targetVersion)) {
    return {
      nextSawDisconnect: opts.sawDisconnect,
      outcome: { phase: "updated", version: now },
    };
  }

  // 仅当双端点均不可达时才视为真实服务失联（进程已停服切换映像）；
  // 单端点偶发错误（例如 /status 502 而 /version 正常响应）保持原 sawDisconnect 不变并继续等待。
  if (statusErr && versionErr) {
    return { nextSawDisconnect: true, outcome: { phase: "waiting" } };
  }
  if (statusErr || versionErr) {
    return { nextSawDisconnect: opts.sawDisconnect, outcome: { phase: "waiting" } };
  }

  // 旧进程仍在主动执行更新/优雅停机阶段：绝不因为此前任何异常误判回滚
  const activePhases = new Set(["downloading", "verifying", "installing", "restarting"]);
  if (status && activePhases.has(status.phase)) {
    return { nextSawDisconnect: opts.sawDisconnect, outcome: { phase: "waiting" } };
  }

  // 曾真实双端点断连重启且恢复后状态回到 idle/done，但版本仍是旧版：存在明确回滚证据
  if (now && opts.targetVersion && now !== opts.targetVersion && opts.sawDisconnect) {
    return {
      nextSawDisconnect: true,
      outcome: { phase: "failed", version: now },
    };
  }

  return { nextSawDisconnect: opts.sawDisconnect, outcome: { phase: "waiting" } };
}

/**
 * 统一的绝对墙钟截止重启等待循环：
 * 以本轮更新起点 + deadlineMs（默认 90s）作为硬性绝对截止点，通过 AbortController 取消挂起的探测请求，
 * 确保即使每次网络探测耗时数秒甚至永不返回，总等待时长也严格不超过 deadlineMs。
 */
export async function runBoundedRestartWait(opts: {
  deadlineAt: number;
  intervalMs: number;
  targetVersion: string;
  sawDisconnectRef: { current: boolean };
  externalSignal: AbortSignal;
  fetchVersion: (signal?: AbortSignal) => Promise<string | undefined>;
  fetchStatus: (signal?: AbortSignal) => Promise<UpdateProgress | undefined>;
}): Promise<{ phase: "updated" | "failed" | "timeout" | "cancelled"; version?: string; error?: string }> {
  const ac = new AbortController();
  let deadlineTriggered = false;

  const onExternalAbort = () => ac.abort();
  if (opts.externalSignal.aborted) {
    return { phase: "cancelled" };
  }
  opts.externalSignal.addEventListener("abort", onExternalAbort, { once: true });

  const remainingStart = opts.deadlineAt - Date.now();
  if (remainingStart <= 0) {
    opts.externalSignal.removeEventListener("abort", onExternalAbort);
    return { phase: "timeout" };
  }

  let deadlineTimer: number | undefined;
  const deadlinePromise = new Promise<"timeout">((resolve) => {
    deadlineTimer = window.setTimeout(() => {
      deadlineTriggered = true;
      ac.abort();
      resolve("timeout");
    }, remainingStart);
  });

  const sleepBounded = (ms: number): Promise<void> => {
    const rem = opts.deadlineAt - Date.now();
    const wait = Math.max(0, Math.min(ms, rem));
    if (wait === 0 || ac.signal.aborted) return Promise.resolve();
    return new Promise<void>((resolve) => {
      const t = window.setTimeout(() => {
        ac.signal.removeEventListener("abort", onAbort);
        resolve();
      }, wait);
      const onAbort = () => {
        window.clearTimeout(t);
        resolve();
      };
      ac.signal.addEventListener("abort", onAbort, { once: true });
    });
  };

  try {
    while (!ac.signal.aborted && Date.now() < opts.deadlineAt) {
      await sleepBounded(opts.intervalMs);
      if (ac.signal.aborted || Date.now() >= opts.deadlineAt) break;

      const stepOrTimeout = await Promise.race([
        probeRestartStep({
          targetVersion: opts.targetVersion,
          sawDisconnect: opts.sawDisconnectRef.current,
          signal: ac.signal,
          fetchVersion: opts.fetchVersion,
          fetchStatus: opts.fetchStatus,
        }),
        deadlinePromise,
      ]);

      if (stepOrTimeout === "timeout" || deadlineTriggered || Date.now() >= opts.deadlineAt) {
        return { phase: "timeout" };
      }
      if (opts.externalSignal.aborted) {
        return { phase: "cancelled" };
      }
      opts.sawDisconnectRef.current = stepOrTimeout.nextSawDisconnect;
      if (stepOrTimeout.outcome.phase === "updated") {
        return { phase: "updated", version: stepOrTimeout.outcome.version };
      }
      if (stepOrTimeout.outcome.phase === "failed") {
        return {
          phase: "failed",
          version: stepOrTimeout.outcome.version,
          error: stepOrTimeout.outcome.error,
        };
      }
    }
    if (opts.externalSignal.aborted && !deadlineTriggered) {
      return { phase: "cancelled" };
    }
    return { phase: "timeout" };
  } finally {
    if (deadlineTimer !== undefined) {
      window.clearTimeout(deadlineTimer);
    }
    opts.externalSignal.removeEventListener("abort", onExternalAbort);
  }
}

/**
 * 根据当前是否处于本次活跃更新（started）与轮询到的阶段，决定是否切入重启探测或报错收尾。
 * 关键保护：当 started 为 false（例如上一轮探针刚报告 failed/timeout 并清零了 started），
 * 即使 React Query 缓存中仍残存旧的 phase === "restarting"，也绝不得重新开启 waitingRestart。
 */
export function resolveUpdateProgressTransition(params: {
  started: boolean;
  waitingRestart: boolean;
  phase?: UpdateProgress["phase"];
}): "none" | "enter_waiting" | "enter_failed" {
  if (!params.started) return "none";
  if (params.phase === "failed") return "enter_failed";
  if (params.waitingRestart) return "none";
  if (params.phase === "restarting" || params.phase === "idle" || params.phase === "done") {
    return "enter_waiting";
  }
  return "none";
}

function formatPublishedAt(iso?: string): string {
  if (!iso) return "";
  const ts = Date.parse(iso);
  if (Number.isNaN(ts)) return iso;
  try {
    return new Date(ts).toLocaleString();
  } catch {
    return iso;
  }
}

/**
 * 全局版本徽章 + 「版本与更新」弹窗（参考 OpenBear 更新策略）：
 * - 常驻顶栏，全站任意页面均可第一时间看到新版本高亮胶囊与琥珀圆点
 * - 精确区分 检查中 / 失败 / 未知 / 已是最新 / 有可用更新 五种状态，不会把失败误显示成“已是最新”
 * - 复用已鉴权队列状态（useQueue）统计运行中任务数，公共版本接口零敏感信息暴露
 * - 容器部署清晰区分浮动 tag 与固定版本/digest 两种更新步骤，并提供一键复制命令
 * - 裸机单二进制部署支持弹窗内二次确认触发自更新、进度条与串行重启探测刷新
 */
export function VersionUpdateTrigger({
  initialOpen = false,
  progressPollMs = 1000,
  restartDeadlineMs = DEFAULT_RESTART_DEADLINE_MS,
  restartPollIntervalMs = DEFAULT_RESTART_POLL_INTERVAL_MS,
}: {
  initialOpen?: boolean;
  progressPollMs?: number;
  restartDeadlineMs?: number;
  restartPollIntervalMs?: number;
}) {
  const { t } = useTranslation();
  const { data: appVersion } = useAppVersion();
  const update = useUpdateCheck();
  const queue = useQueueList();
  const mut = useSelfUpdate();

  const [open, setOpen] = useState(initialOpen);
  const [confirming, setConfirming] = useState(false);
  const [started, setStarted] = useState(false);
  const [waitingRestart, setWaitingRestart] = useState(false);
  const [updateRunId, setUpdateRunId] = useState(0);
  const [pinnedTarget, setPinnedTarget] = useState("");
  const [failed, setFailed] = useState<string | null>(null);
  const [copied, setCopied] = useState<"" | "latest" | "pinned">("");
  const sawDisconnectRef = useRef(false);
  const consecutiveErrsRef = useRef(0);
  const restartDeadlineRef = useRef(0);

  const check = update.data;
  const viewState = deriveUpdateViewState({
    isPending: update.isPending,
    isError: update.isError,
    check,
  });
  const currentVersion = check?.current || appVersion || "";
  const latestVersion = check?.latest || "";
  const hasUpdate = Boolean(check?.hasUpdate);
  const refreshFailed = update.isError || Boolean(check?.lastError);
  const activeTasks = (queue.data ?? []).filter((it) => it.status === "running").length;
  const pinnedUpdateCmd = latestVersion
    ? `# 1) 从 Docker Actions 构建摘要获取多架构 digest，或执行下方命令查询：\ndocker buildx imagetools inspect ghcr.io/w0ven/ovh-console:${latestVersion} | grep Digest\n# 2) 在 docker-compose.yml 中固定镜像 digest（示例）：\n#    image: ghcr.io/w0ven/ovh-console@sha256:<digest>\n# 3) 拉取并重建容器：\ndocker compose pull && docker compose up -d`
    : CONTAINER_LATEST_CMD;

  const progress = useUpdateProgress(started && !waitingRestart, updateRunId, progressPollMs);
  // 只有本轮成功收到一次真实服务端响应后，才采信并渲染 progress.data；首轮尚未成功或失败期间绝不渲染残留缓存
  const hasFreshProgress =
    progress.status === "success" && progress.isFetchedAfterMount && progress.dataUpdatedAt > 0;
  const freshProgress = hasFreshProgress ? progress.data : undefined;

  // 若 /version/update/status 连续失败达到阈值（例如服务已切入重启断连或状态端点持续失败），
  // 自动转入带次数上限的 waitingRestart 探针（探测 /version 新版本号或 90s 超时报错），绝不无限停在“下载中 0%”。
  useEffect(() => {
    if (!started || waitingRestart) {
      consecutiveErrsRef.current = 0;
      return;
    }
    if (hasFreshProgress) {
      consecutiveErrsRef.current = 0;
      return;
    }
    if (progress.status === "error" && progress.errorUpdateCount > 0) {
      consecutiveErrsRef.current += 1;
      if (consecutiveErrsRef.current >= 3) {
        consecutiveErrsRef.current = 0;
        sawDisconnectRef.current = false;
        setWaitingRestart(true);
      }
    }
  }, [started, waitingRestart, hasFreshProgress, progress.status, progress.errorUpdateCount]);

  useEffect(() => {
    // 仅消费本次全局唯一 updateRunId 成功拉回的数据（status === "success" && isFetchedAfterMount && dataUpdatedAt > 0），
    // 若本轮首次进度请求失败，绝不把 isFetchedAfterMount 误当作本轮有效数据来触发状态迁移。
    if (!hasFreshProgress) return;
    const action = resolveUpdateProgressTransition({
      started,
      waitingRestart,
      phase: freshProgress?.phase,
    });
    if (action === "enter_waiting") {
      sawDisconnectRef.current = false;
      setWaitingRestart(true);
      return;
    }
    if (action === "enter_failed") {
      setFailed(freshProgress?.error || t("commons.update.failed"));
      setStarted(false);
      setWaitingRestart(false);
    }
  }, [
    started,
    waitingRestart,
    hasFreshProgress,
    freshProgress?.phase,
    freshProgress?.error,
    t,
  ]);

  // 串行轮询等待重启：共享本轮更新的绝对墙钟截止时间（默认 90s），超时立即中止在途探测，绝不被慢请求拉长至数分钟
  useEffect(() => {
    if (!waitingRestart) return;
    const ac = new AbortController();
    const target = pinnedTarget || check?.latest || "";
    const deadlineAt = restartDeadlineRef.current > 0 ? restartDeadlineRef.current : Date.now() + restartDeadlineMs;

    const loop = async () => {
      const res = await runBoundedRestartWait({
        deadlineAt,
        intervalMs: restartPollIntervalMs,
        targetVersion: target,
        sawDisconnectRef,
        externalSignal: ac.signal,
        fetchVersion: async (signal) => (await api.get<{ version: string }>("/version", { signal })).data?.version,
        fetchStatus: async (signal) => (await api.get<UpdateProgress>("/version/update/status", { signal })).data,
      });
      if (ac.signal.aborted || res.phase === "cancelled") return;
      if (res.phase === "updated") {
        toast.success(t("commons.update.updatedTo", { version: res.version || target }));
        window.setTimeout(() => window.location.reload(), 800);
        return;
      }
      setWaitingRestart(false);
      setStarted(false);
      if (res.phase === "failed") {
        setFailed(
          res.error ||
            t("commons.update.rollback", { target, current: res.version || currentVersion }),
        );
        return;
      }
      setFailed(t("commons.update.restartTimeout"));
    };

    void loop();
    return () => {
      ac.abort();
    };
  }, [waitingRestart, pinnedTarget, check?.latest, currentVersion, restartDeadlineMs, restartPollIntervalMs, t]);

  const handleOpen = () => {
    setConfirming(false);
    setOpen(true);
    void update.refetch();
  };

  const handleCopy = async (kind: "latest" | "pinned", text: string) => {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(kind);
      toast.success(t("commons.update.copiedCmd"));
      setTimeout(() => setCopied(""), 1800);
    } catch {
      toast.info(text);
    }
  };

  const handleStartUpdate = async () => {
    setFailed(null);
    setConfirming(false);
    setWaitingRestart(false);
    sawDisconnectRef.current = false;
    consecutiveErrsRef.current = 0;
    restartDeadlineRef.current = Date.now() + restartDeadlineMs;
    setPinnedTarget(latestVersion);
    try {
      const res = await mut.mutateAsync();
      if (!res.success) {
        setFailed(res.error || t("commons.update.cannotStart"));
        return;
      }
      if (res.latest) setPinnedTarget(res.latest);
      setUpdateRunId(allocateUpdateProgressRunId());
      setStarted(true);
      toast.info(res.message || t("commons.update.started"));
    } catch (e: any) {
      setFailed(e?.response?.data?.error || t("commons.update.cannotStart"));
    }
  };

  const pillTitle = hasUpdate
    ? t("commons.update.pillTitleUpdate", { version: latestVersion })
    : t("commons.update.pillTitleCurrent");

  const statusChipText =
    viewState === "available"
      ? t("commons.update.availableChip")
      : viewState === "upToDate"
        ? t("commons.update.latestChip")
        : viewState === "loading"
          ? t("commons.update.checking")
          : viewState === "error" || viewState === "staleError"
            ? t("commons.update.checkErrorChip")
            : t("commons.update.unknownChip");

  return (
    <>
      <button
        type="button"
        onClick={handleOpen}
        title={pillTitle}
        aria-label={pillTitle}
        className={cn(
          "inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-[11px] font-medium leading-none transition-colors cursor-pointer shrink-0",
          hasUpdate
            ? "border-amber-400/70 bg-amber-500/12 text-amber-700 dark:text-amber-300 hover:bg-amber-500/20"
            : "border-border/80 bg-background/70 text-muted-foreground hover:bg-muted/80 hover:text-foreground",
        )}
      >
        {waitingRestart || started ? (
          <Loader2 className="w-3 h-3 animate-spin" />
        ) : hasUpdate ? (
          <Sparkles className="w-3 h-3" />
        ) : null}
        <span className="font-mono tnum">v{currentVersion || "…"}</span>
        {hasUpdate && (
          <span
            className="w-1.5 h-1.5 rounded-full bg-amber-500 shadow-[0_0_0_3px_rgba(245,158,11,0.22)]"
            aria-hidden="true"
          />
        )}
      </button>

      <Dialog open={open} onOpenChange={(next) => { setOpen(next); if (!next) setConfirming(false); }}>
        <DialogContent className="sm:max-w-xl" aria-describedby={undefined}>
          <DialogHeader>
            <div className="flex items-center justify-between gap-3 pr-8">
              <div>
                <div className="text-[11px] font-medium text-muted-foreground">{t("nav.appName")}</div>
                <DialogTitle className="mt-0.5 text-base">{t("commons.update.dialogTitle")}</DialogTitle>
              </div>
              <span
                className={cn(
                  "inline-flex items-center gap-1 rounded-full px-2.5 py-1 text-[11px] font-medium shrink-0 border",
                  viewState === "available"
                    ? "bg-amber-500/15 text-amber-700 dark:text-amber-300 border-amber-400/40"
                    : viewState === "error" || viewState === "staleError"
                      ? "bg-destructive/10 text-destructive border-destructive/40"
                      : "bg-muted text-muted-foreground border-border",
                )}
              >
                {statusChipText}
              </span>
            </div>
          </DialogHeader>

          <div className="space-y-4">
            {/* 版本对比卡片：当前 → 最新 */}
            <div className="grid grid-cols-[1fr_auto_1fr] items-center gap-3 rounded-xl border border-border bg-muted/35 px-4 py-3">
              <div className="min-w-0">
                <div className="text-[11px] text-muted-foreground">{t("commons.update.currentLabel")}</div>
                <div className="mt-0.5 font-mono text-base font-semibold tnum">
                  v{currentVersion || "…"}
                </div>
              </div>
              <ArrowRight className="w-4 h-4 text-muted-foreground shrink-0" aria-hidden="true" />
              <div className="min-w-0">
                <div className="text-[11px] text-muted-foreground">
                  {viewState === "available"
                    ? t("commons.update.latestLabel")
                    : viewState === "upToDate"
                      ? t("commons.update.installedLabel")
                      : t("commons.update.latestLabel")}
                </div>
                <div
                  className={cn(
                    "mt-0.5 font-mono text-base font-semibold tnum",
                    hasUpdate && "text-amber-700 dark:text-amber-300",
                  )}
                >
                  {latestVersion ? `v${latestVersion}` : "—"}
                </div>
              </div>
            </div>

            {/* 发布元信息 */}
            <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground">
              {check?.publishedAt && (
                <span>{t("commons.update.publishedAt", { time: formatPublishedAt(check.publishedAt) })}</span>
              )}
              {check?.url && (
                <a
                  href={check.url}
                  target="_blank"
                  rel="noreferrer noopener"
                  className="inline-flex items-center gap-1 text-primary hover:underline"
                >
                  {t("commons.update.viewOnGithub")}
                  <ExternalLink className="w-3 h-3" />
                </a>
              )}
              <button
                type="button"
                onClick={() => void update.refetch()}
                disabled={update.isFetching}
                className="ml-auto inline-flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground disabled:opacity-50 cursor-pointer"
              >
                <RefreshCw className={cn("w-3 h-3", update.isFetching && "animate-spin")} />
                {update.isFetching ? t("commons.update.checking") : t("commons.update.checkNow")}
              </button>
            </div>

            {refreshFailed && (
              <div className="flex items-center gap-2 rounded-xl border border-destructive/40 bg-destructive/5 px-3 py-2 text-xs text-destructive">
                <AlertTriangle className="w-3.5 h-3.5 shrink-0" />
                <span>
                  {t("commons.update.checkFailed")}
                  {viewState === "staleError" && latestVersion
                    ? ` (${t("commons.update.lastSnapshot", { version: latestVersion })})`
                    : ""}
                </span>
              </div>
            )}

            {/* 运行中抢购任务提示（来自已鉴权的 useQueue） */}
            {hasUpdate && activeTasks > 0 && (
              <div className="flex items-start gap-2 rounded-xl border border-amber-400/40 bg-amber-500/10 px-3 py-2.5 text-xs text-amber-800 dark:text-amber-200">
                <AlertTriangle className="w-4 h-4 shrink-0 mt-0.5" />
                <span>{t("commons.update.activeTasksWarn", { count: activeTasks })}</span>
              </div>
            )}

            {/* 容器部署更新指引：明确区分固定版本/digest 与浮动 latest */}
            {hasUpdate && check?.inContainer && (
              <div className="rounded-xl border border-border bg-muted/30 p-3 space-y-2.5">
                <div className="text-xs font-medium">{t("commons.update.containerSectionTitle")}</div>
                <div className="space-y-1.5">
                  <div className="flex items-center justify-between gap-2">
                    <span className="text-[11px] text-muted-foreground">{t("commons.update.containerPinnedLabel")}</span>
                    <Button type="button" size="sm" variant="outline" className="h-6 px-2 text-[11px]" onClick={() => void handleCopy("pinned", pinnedUpdateCmd)}>
                      {copied === "pinned" ? <Check className="w-3 h-3 mr-1" /> : <Copy className="w-3 h-3 mr-1" />}
                      {t("commons.update.copyCmd")}
                    </Button>
                  </div>
                  <pre className="rounded-lg bg-background/80 border border-border/70 px-3 py-2 text-[11px] font-mono overflow-x-auto select-all whitespace-pre-wrap">
                    {pinnedUpdateCmd}
                  </pre>
                </div>
                <div className="space-y-1.5">
                  <div className="flex items-center justify-between gap-2">
                    <span className="text-[11px] text-muted-foreground">{t("commons.update.containerLatestLabel")}</span>
                    <Button type="button" size="sm" variant="outline" className="h-6 px-2 text-[11px]" onClick={() => void handleCopy("latest", CONTAINER_LATEST_CMD)}>
                      {copied === "latest" ? <Check className="w-3 h-3 mr-1" /> : <Copy className="w-3 h-3 mr-1" />}
                      {t("commons.update.copyCmd")}
                    </Button>
                  </div>
                  <pre className="rounded-lg bg-background/80 border border-border/70 px-3 py-2 text-[11px] font-mono overflow-x-auto select-all">
                    {CONTAINER_LATEST_CMD}
                  </pre>
                </div>
              </div>
            )}

            {/* 更新进度 / 报错状态 */}
            {waitingRestart && (
              <div className="flex items-center gap-2 rounded-xl border border-primary/30 bg-primary/5 px-3 py-2.5 text-xs text-primary">
                <Loader2 className="w-3.5 h-3.5 animate-spin shrink-0" />
                <span>{t("commons.update.restarting")}</span>
              </div>
            )}
            {started && !waitingRestart && (
              <div className="rounded-xl border border-border bg-muted/30 px-3 py-2.5 space-y-1.5 text-xs">
                <div className="flex items-center justify-between gap-2">
                  <span className="inline-flex items-center gap-1.5 text-muted-foreground">
                    <Loader2 className="w-3.5 h-3.5 animate-spin" />
                    {freshProgress?.phase === "installing"
                      ? t("commons.update.installing")
                      : freshProgress?.message || t("commons.update.downloading")}
                  </span>
                  <span className="font-mono tnum">{freshProgress?.percent ?? 0}%</span>
                </div>
                <div className="h-1.5 rounded-full bg-secondary overflow-hidden">
                  <div
                    className="h-full bg-primary transition-all"
                    style={{ width: `${freshProgress?.percent ?? 0}%` }}
                  />
                </div>
              </div>
            )}
            {failed && (
              <div className="flex items-start gap-2 rounded-xl border border-destructive/40 bg-destructive/5 px-3 py-2.5 text-xs text-destructive">
                <AlertTriangle className="w-4 h-4 shrink-0 mt-0.5" />
                <span className="break-words">{failed}</span>
              </div>
            )}

            {/* 更新说明（GitHub Release body） */}
            <section className="rounded-xl border border-border overflow-hidden">
              <div className="px-3.5 py-2 border-b border-border bg-muted/30 text-xs font-medium text-muted-foreground">
                {t("commons.update.notesHeading")}
              </div>
              <div className="px-3.5 py-3 max-h-60 overflow-y-auto text-xs leading-relaxed">
                {check?.body?.trim() ? (
                  <pre className="whitespace-pre-wrap break-words font-sans text-foreground/90">
                    {check.body.trim()}
                  </pre>
                ) : (
                  <p className="text-muted-foreground">
                    {latestVersion ? t("commons.update.notesEmpty") : t("commons.update.noReleaseYet")}
                  </p>
                )}
              </div>
            </section>

            {/* 裸机二次确认条（参考 OpenBear 更新确认） */}
            {confirming && hasUpdate && !check?.inContainer && (
              <div className="flex flex-wrap items-center justify-between gap-2 rounded-xl border border-amber-400/50 bg-amber-500/10 px-3.5 py-2.5 text-xs">
                <span className="text-foreground">
                  {t("commons.update.confirmUpdate", { version: latestVersion })}
                </span>
                <div className="flex items-center gap-2 ml-auto">
                  <Button type="button" size="sm" variant="outline" className="h-7 px-2.5 text-xs" onClick={() => setConfirming(false)}>
                    {t("common.cancel")}
                  </Button>
                  <Button
                    type="button"
                    size="sm"
                    className="h-7 px-2.5 text-xs"
                    disabled={mut.isPending}
                    onClick={handleStartUpdate}
                  >
                    {mut.isPending ? <Loader2 className="w-3 h-3 mr-1 animate-spin" /> : <Download className="w-3 h-3 mr-1" />}
                    {t("commons.update.updateNow")}
                  </Button>
                </div>
              </div>
            )}
          </div>

          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => setOpen(false)}>
              {t("common.close")}
            </Button>
            {hasUpdate && !check?.inContainer && !confirming && (
              <Button
                type="button"
                disabled={mut.isPending || started || waitingRestart}
                onClick={() => setConfirming(true)}
              >
                {mut.isPending || started || waitingRestart ? (
                  <Loader2 className="w-3.5 h-3.5 mr-1.5 animate-spin" />
                ) : (
                  <Download className="w-3.5 h-3.5 mr-1.5" />
                )}
                {t("commons.update.updateTo", { version: latestVersion })}
              </Button>
            )}
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}
