import { useEffect, useRef, useState } from "react";
import { Sparkles, Loader2, Download, CheckCircle2, AlertTriangle } from "lucide-react";
import { Button } from "@/components/ui/button";
import { api } from "@/lib/api";
import {
  allocateUpdateProgressRunId,
  useSelfUpdate,
  useUpdateProgress,
  type UpdateCheck,
  type UpdateProgress,
} from "@/hooks/use-system-metrics";
import {
  DEFAULT_RESTART_DEADLINE_MS,
  DEFAULT_RESTART_POLL_INTERVAL_MS,
  openVersionUpdateDialog,
  resolveUpdateProgressTransition,
  runBoundedRestartWait,
} from "@/components/common/VersionUpdateTrigger";
import { toast } from "sonner";
import { useTranslation } from "react-i18next";

/**
 * 在线更新按钮。
 *
 * 全流程：点一下 → 后端下载 → 校验 SHA256 → 替换正在运行的二进制 → 自动重启。
 * 用户不需要下载文件、不需要手动替换、不需要自己重启。
 *
 * 前端这一半的关键在**重启那几秒**：服务在换进程映像，所有请求都会失败。
 * 这时不能报错吓人 —— 连不上恰恰是重启正在发生的信号。所以进入 restarting 之后
 * 改为轮询 /api/version（不带鉴权也能通），直到它回报新版本号，再整页刷新
 * （二进制换了，内嵌的前端资源也跟着换了，不刷新会继续跑旧页面）。
 */
const MAX_PROGRESS_POLL_ERRORS = 3;

export function UpdateButton({
  check,
  progressPollMs = 1000,
  restartDeadlineMs = DEFAULT_RESTART_DEADLINE_MS,
  restartPollIntervalMs = DEFAULT_RESTART_POLL_INTERVAL_MS,
}: {
  check?: UpdateCheck;
  progressPollMs?: number;
  restartDeadlineMs?: number;
  restartPollIntervalMs?: number;
}) {
  const { t } = useTranslation();
  const mut = useSelfUpdate();
  const [started, setStarted] = useState(false);
  const [waitingRestart, setWaitingRestart] = useState(false);
  const [updateRunId, setUpdateRunId] = useState(0);
  const [pinnedTarget, setPinnedTarget] = useState("");
  const [failed, setFailed] = useState<string | null>(null);
  const sawDisconnectRef = useRef(false);
  const consecutiveErrsRef = useRef(0);
  const restartDeadlineRef = useRef(0);

  const progress = useUpdateProgress(started && !waitingRestart, updateRunId, progressPollMs);
  // 只有本轮成功收到一次真实服务端响应后，才采信并渲染 progress.data；首轮尚未成功或失败期间绝不渲染残留缓存
  const hasFreshProgress =
    progress.status === "success" && progress.isFetchedAfterMount && progress.dataUpdatedAt > 0;
  const freshProgress = hasFreshProgress ? progress.data : undefined;

  // 若 /version/update/status 连续失败达到阈值（例如服务已经快速切入重启断连或状态接口不可达），
  // 自动转入有次数上限的 waitingRestart 版本探针，绝不无限卡在“下载中 0%”。
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
      if (consecutiveErrsRef.current >= MAX_PROGRESS_POLL_ERRORS) {
        consecutiveErrsRef.current = 0;
        sawDisconnectRef.current = false;
        setWaitingRestart(true);
      }
    }
  }, [started, waitingRestart, hasFreshProgress, progress.status, progress.errorUpdateCount]);

  // 仅当本次更新仍活跃（started === true）且本轮全局唯一 updateRunId 收到成功响应时才迁移状态，
  // 彻底杜绝跨组件、跨挂载残留缓存以及首次请求失败误触发旧状态。
  useEffect(() => {
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

  // 等待重启：共享本轮更新的绝对墙钟截止时间（默认 90s），超时立即中止在途网络探测，杜绝超过 90s 仍无限等待
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
            t("commons.update.rollback", { target, current: res.version || check?.current || "" }),
        );
        return;
      }
      setFailed(t("commons.update.restartTimeout"));
    };
    void loop();
    return () => {
      ac.abort();
    };
  }, [waitingRestart, pinnedTarget, check?.latest, check?.current, restartDeadlineMs, restartPollIntervalMs, t]);

  const start = async () => {
    setFailed(null);
    setWaitingRestart(false);
    sawDisconnectRef.current = false;
    consecutiveErrsRef.current = 0;
    restartDeadlineRef.current = Date.now() + restartDeadlineMs;
    setPinnedTarget(check?.latest || "");
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

  // 没有新版本（或用户已忽略该版本提醒）就不显示高亮更新提示
  if ((!check?.hasUpdate || check?.ignored) && !started && !waitingRestart && !failed) return null;

  // 容器里不给更新按钮 —— 点击高亮胶囊直接打开「版本与更新」弹窗看完整说明和升级命令。
  if (check?.inContainer) {
    return (
      <button
        type="button"
        onClick={openVersionUpdateDialog}
        className="inline-flex items-center gap-1.5 text-[11px] text-primary hover:underline cursor-pointer whitespace-nowrap"
        title={t("commons.update.latestTitle", { version: check.latest })}
      >
        <Sparkles className="w-3 h-3" />
        {t("commons.update.containerBadge", { version: check.latest })}
      </button>
    );
  }

  if (waitingRestart) {
    return (
      <span className="inline-flex items-center gap-1.5 text-[11px] text-primary">
        <Loader2 className="w-3 h-3 animate-spin" />
        {t("commons.update.restarting")}
      </span>
    );
  }

  if (started) {
    const p = freshProgress;
    const pct = p?.percent ?? 0;
    return (
      <span className="inline-flex items-center gap-2 text-[11px] text-muted-foreground" title={p?.message}>
        <Loader2 className="w-3 h-3 animate-spin" />
        {p?.phase === "installing" ? (
          <span>{t("commons.update.installing")}</span>
        ) : (
          <span className="inline-flex items-center gap-1.5">
            {t("commons.update.downloading")}
            <span className="inline-block w-16 h-1 rounded bg-secondary overflow-hidden align-middle">
              <span className="block h-full bg-primary transition-all" style={{ width: `${pct}%` }} />
            </span>
            {pct}%
          </span>
        )}
      </span>
    );
  }

  if (failed) {
    return (
      <span className="inline-flex items-center gap-1.5 text-[11px] text-destructive" title={failed}>
        <AlertTriangle className="w-3 h-3" />
        <span className="max-w-[220px] truncate">{failed}</span>
        <button className="underline hover:no-underline" onClick={start}>
          {t("commons.update.retry")}
        </button>
      </span>
    );
  }

  return (
    <span className="inline-flex items-center gap-1.5">
      <button
        type="button"
        onClick={openVersionUpdateDialog}
        className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-medium bg-primary/10 text-primary hover:bg-primary/20 transition-colors cursor-pointer"
        title={t("commons.update.latestTitle", { version: check!.latest })}
      >
        <Sparkles className="w-3 h-3" />
        v{check!.latest}
      </button>
      <Button size="sm" variant="outline" className="h-6 px-2 text-[11px]" onClick={start} disabled={mut.isPending}>
        {mut.isPending ? <Loader2 className="w-3 h-3 animate-spin" /> : <Download className="w-3 h-3 mr-1" />}
        {t("commons.update.updateNow")}
      </Button>
    </span>
  );
}

/** 更新完成后的提示（重启回来那一下用得上，目前留给将来扩展） */
export function UpdatedBadge({ version }: { version: string }) {
  const { t } = useTranslation();
  return (
    <span className="inline-flex items-center gap-1 text-[11px] text-success">
      <CheckCircle2 className="w-3 h-3" />
      {t("commons.update.upToDate", { version })}
    </span>
  );
}
