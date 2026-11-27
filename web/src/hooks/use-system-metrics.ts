import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import i18n from "@/i18n";
import { apiMessage } from "@/lib/api-error";

/** 后端二进制版本*/
export function useAppVersion() {
  return useQuery({
    queryKey: ["app", "version"],
    // ?? "" 不能省:queryFn 返回 undefined 会被 react-query 当成错误抛,
    // 而这个 hook 用在仪表盘上 —— 抛出去就是整页白屏。
    // 后端正常返回 { version }, 这里防的是响应结构意外(代理插页、网关 200 带错误体)。
    queryFn: async () => (await api.get<{ version: string }>("/version")).data?.version ?? "",
    staleTime: Infinity, // 进程跑起来版本号不会变,缓存到 unmount
    gcTime: Infinity,
    retry: 0,
  });
}

/** GitHub releases 上游更新检查结果 */
export interface UpdateCheck {
  current: string;
  latest: string;
  tag: string;
  name: string;
  hasUpdate: boolean;
  /** 该新版本是否已被用户在 Web 或 Telegram 标记为忽略提醒 */
  ignored?: boolean;
  /** 当前后端记录的已忽略版本号（不含 v） */
  ignoredVersion?: string;
  url: string;
  publishedAt: string;
  body: string;
  prerelease: boolean;
  checkedAt: string;
  /** 最近一次刷新检查失败的原因（后端沿用上次成功快照时非空），界面据此提示“上次刷新失败”而不误报“已是最新” */
  lastError?: string;
  /** 跑在容器里。自更新在容器里是停用的 —— 新二进制只会写进容器的可写层，
   *  容器一重建就回到镜像里的旧版本。界面据此显示「怎么拉新镜像」而不是更新按钮。 */
  inContainer?: boolean;
  /** 容器里的更新指引（多行文本）。inContainer 为 false 时是空串。 */
  updateHint?: string;
}

/** 检查上游 (w0ven/ovh-console) 是否有新版本。
 *  - 后端已按 5 分钟 ETag 增量轮询并带 15 分钟缓存 + singleflight，前端切页/聚焦可安全低开销刷新
 *  - 全局 AppShell 顶栏与仪表盘共用同一 queryKey，全站任意页面均可第一时间显示新版本红点与版本弹窗
 */
export function useUpdateCheck() {
  return useQuery<UpdateCheck>({
    queryKey: ["app", "update-check"],
    queryFn: async () => (await api.get<UpdateCheck>("/version/check-update")).data,
    staleTime: 5 * 60 * 1000, // 5min，对齐后端 ETag 刷新周期
    refetchInterval: 5 * 60 * 1000,
    gcTime: 24 * 60 * 60 * 1000,
    retry: 1,
    refetchOnWindowFocus: true,
  });
}

/** 自更新进度。后端把状态放内存,重启阶段这个接口会连不上 —— 那正是重启成功的信号 */
export interface UpdateProgress {
  phase: "idle" | "downloading" | "verifying" | "installing" | "restarting" | "done" | "failed";
  message: string;
  percent: number;
  version: string;
  error?: string;
}

/** 触发在线更新:下载 → 校验 SHA256 → 替换二进制 → 自动重启。
 *  接口立刻返回,真正的活在后端 goroutine 里,进度靠 useUpdateProgress 轮询。 */
export function useSelfUpdate() {
  return useMutation({
    mutationFn: async () =>
      (await api.post<{ success: boolean; message?: string; error?: string; latest?: string }>("/version/update")).data,
  });
}

/** 忽略或恢复（传空串）指定新版本的更新提醒，后端持久化在 SQLite kv 中并与 Telegram 同步。 */
export function useIgnoreUpdateVersion() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (version: string) =>
      (await api.post<{ status: string; ignoredVersion: string }>("/version/ignore", { version })).data,
    onSuccess: (res) => {
      qc.setQueryData<UpdateCheck | undefined>(["app", "update-check"], (prev) => {
        if (!prev) return prev;
        const ignoredVer = res?.ignoredVersion || "";
        return {
          ...prev,
          ignoredVersion: ignoredVer,
          ignored: Boolean(prev.hasUpdate && ignoredVer && ignoredVer === prev.latest),
        };
      });
      void qc.invalidateQueries({ queryKey: ["app", "update-check"] });
    },
  });
}

let nextUpdateProgressRunId = 0;

/** 分配跨组件、跨挂载全局单调递增的更新轮次 ID，彻底杜绝 UpdateButton / VersionUpdateTrigger
 *  以及重挂载组件在共享 QueryClient 下复用到上一轮遗留的 progress 缓存键。 */
export function allocateUpdateProgressRunId(): number {
  nextUpdateProgressRunId += 1;
  return nextUpdateProgressRunId;
}

/** 轮询更新进度。enabled 时按 refetchIntervalMs 周期轮询（默认 1000ms） ——
 *  更新是用户主动点的、有明确终点，这个频率只在更新期间存在，不会常驻。
 *  runId 必须由 allocateUpdateProgressRunId() 分配。 */
export function useUpdateProgress(enabled: boolean, runId: number, refetchIntervalMs = 1000) {
  return useQuery<UpdateProgress>({
    queryKey: ["app", "update-progress", runId],
    queryFn: async () => (await api.get<UpdateProgress>("/version/update/status")).data,
    enabled: enabled && runId > 0,
    refetchInterval: enabled && runId > 0 ? refetchIntervalMs : false,
    refetchIntervalInBackground: true,
    retry: 0,
    gcTime: 0,
  });
}

export interface SystemMetrics {
  cpu: { percent: number; cores: number };
  memory: { totalBytes: number; usedBytes: number; percent: number };
  disk: { totalBytes: number; usedBytes: number; percent: number; path: string };
  host: { hostname: string; platform: string; uptimeSec: number };
}

/** 宿主机实时监控。仪表盘专用，每 2 秒拉一次。
 *  - 这是唯一需要后台轮询的查询(实时监控本质要求)
 *  - 组件 unmount(切走仪表盘) 后 React Query 自动停止 refetch
 */
export function useSystemMetrics() {
  return useQuery({
    queryKey: ["system", "metrics"],
    queryFn: async () => {
      const d = (await api.get<SystemMetrics>("/system/metrics")).data;
      // 形状不对就抛错:仪表盘的守卫是 sys.data ? 渲染环 : 未知环,
      // 一个 truthy 但缺 cpu 子对象的结构(比如代理层回了别的 JSON)
      // 会让 `sys.data.cpu.cores` 直接白屏。
      // 抛错而不是返回 undefined —— React Query 不允许 data 为 undefined,
      // 且 isError 路径会让未知环带上"读取失败"的正确语义
      if (!d || typeof d !== "object" || !d.cpu || !d.memory || !d.disk) {
        throw new Error(i18n.t("hooksMsg.metrics.badResponse"));
      }
      return d;
    },
    refetchInterval: 2000,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
    staleTime: 1500,
  });
}
