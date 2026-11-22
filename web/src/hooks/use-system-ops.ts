import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import i18n from "@/i18n";
import { api } from "@/lib/api";
import { apiMessage } from "@/lib/api-error";

export interface LoopHeartbeat {
  name: string;
  running: boolean;
  healthy: boolean;
  stale: boolean;
  intervalSec: number;
  startedAt?: string;
  lastBeatAt?: string;
  lastOkAt?: string;
  lastErrorAt?: string;
  lastError?: string;
  beatCount: number;
}

export interface DBMaintenanceStats {
  driver: string;
  dbBytes: number;
  walBytes: number;
  shmBytes: number;
  totalBytes: number;
  pageSize: number;
  pageCount: number;
  freelistPages: number;
  freelistBytes: number;
  journalMode: string;
  autoVacuum: number;
  diskAvailBytes?: number;
  vacuumSpaceOk: boolean;
  lastCheckpointAt?: string;
  lastVacuumAt?: string;
  lastMaintainAction?: string;
  maintenanceBusy: boolean;
}

export interface SystemRuntimeInfo {
  version: string;
  inContainer: boolean;
  goVersion: string;
  goos: string;
  goarch: string;
  goroutines: number;
  heapAlloc: number;
  heapInuse: number;
  sysBytes: number;
  startedAt: string;
  uptimeSec: number;
}

export interface BackupSummary {
  keepAuto: number;
  autoPeriodH: number;
  totalCount: number;
  autoCount: number;
  manualCount: number;
  totalBytes: number;
  latestAt: string;
}

export interface SystemStatusResponse {
  runtime: SystemRuntimeInfo;
  database: DBMaintenanceStats;
  backups: BackupSummary;
  heartbeats: LoopHeartbeat[];
}

export interface BackupItem {
  id: string;
  filename: string;
  kind: "auto" | "manual";
  sizeBytes: number;
  createdAt: string;
  credentials: "encrypted" | "redacted";
}

export interface BackupListResponse {
  success: boolean;
  keepAuto: number;
  backups: BackupItem[];
}

export interface DBMaintainResult {
  action: "checkpoint" | "vacuum";
  before: DBMaintenanceStats;
  after: DBMaintenanceStats;
  reclaimed: number;
  partial?: boolean;
  durationMs: number;
}

export function useSystemStatus(enabled = true) {
  return useQuery({
    queryKey: ["system-status"],
    queryFn: async () => (await api.get<SystemStatusResponse>("/system/status")).data,
    enabled,
    refetchInterval: 15_000,
    staleTime: 5_000,
  });
}

export function useSystemBackups(enabled = true) {
  return useQuery({
    queryKey: ["system-backups"],
    queryFn: async () => (await api.get<BackupListResponse>("/system/backups")).data,
    enabled,
    staleTime: 10_000,
  });
}

export function useMaintainDatabase() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (action: "checkpoint" | "vacuum") => {
      const res = await api.post<{ success: boolean; result: DBMaintainResult }>(
        "/system/db/maintain",
        { action },
        { timeout: 120_000 },
      );
      return res.data.result;
    },
    onSuccess: (res) => {
      qc.invalidateQueries({ queryKey: ["system-status"] });
      const savedKb = Math.max(0, Math.round(res.reclaimed / 1024));
      if (res.partial) {
        toast.warning(
          res.action === "checkpoint"
            ? i18n.t("settings.system.checkpointPartial", { savedKb, ms: res.durationMs })
            : i18n.t("settings.system.vacuumPartial", { savedKb, ms: res.durationMs }),
        );
        return;
      }
      toast.success(
        res.action === "checkpoint"
          ? i18n.t("settings.system.checkpointDone", { savedKb, ms: res.durationMs })
          : i18n.t("settings.system.vacuumDone", { savedKb, ms: res.durationMs }),
      );
    },
    onError: (err) => {
      const code = (err as { code?: string })?.code;
      if (code === "ECONNABORTED") {
        toast.error(i18n.t("settings.system.maintainTimeoutCanceled"));
        return;
      }
      toast.error(apiMessage(err));
    },
  });
}

export function useCreateBackup() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (redact: boolean) => {
      const res = await api.post<{ success: boolean; backup: BackupItem }>(
        "/system/backups",
        { redact },
        { timeout: 120_000 },
      );
      return res.data.backup;
    },
    onSuccess: (b) => {
      qc.invalidateQueries({ queryKey: ["system-status"] });
      qc.invalidateQueries({ queryKey: ["system-backups"] });
      toast.success(i18n.t("settings.system.backupCreated", { name: b.filename }));
    },
    onError: (err) => {
      toast.error(apiMessage(err));
    },
  });
}

export function useDownloadBackup() {
  return useMutation({
    mutationFn: async ({ id, filename, redact }: { id: string; filename: string; redact: boolean }) => {
      const tokRes = await api.post<{
        success: boolean;
        token: string;
        redacted: boolean;
        expiresAt: string;
      }>(`/system/backups/${encodeURIComponent(id)}/download-token`, { redact });
      const token = tokRes.data.token;
      const dlRes = await api.post<Blob>(
        `/system/backups/${encodeURIComponent(id)}/download`,
        { token },
        { responseType: "blob", timeout: 180_000 },
      );
      const disp = String(dlRes.headers?.["content-disposition"] || "");
      const m = disp.match(/filename="([^"]+)"/);
      const finalName = m?.[1] || (redact && !filename.endsWith("-redacted.db") ? filename.replace(/\.db$/, "-redacted.db") : filename);
      const url = URL.createObjectURL(dlRes.data);
      try {
        const a = document.createElement("a");
        a.href = url;
        a.download = finalName;
        document.body.appendChild(a);
        a.click();
        a.remove();
      } finally {
        setTimeout(() => URL.revokeObjectURL(url), 5_000);
      }
      return finalName;
    },
    onSuccess: (name) => {
      toast.success(i18n.t("settings.system.backupDownloaded", { name }));
    },
    onError: async (err) => {
      const respData = (err as { response?: { data?: unknown } })?.response?.data;
      if (typeof Blob !== "undefined" && respData instanceof Blob) {
        try {
          const text = await respData.text();
          const parsed = JSON.parse(text) as { error?: string; message?: string };
          if (parsed?.error || parsed?.message) {
            toast.error(parsed.error || parsed.message);
            return;
          }
        } catch {
          // ignore non-JSON blob
        }
      }
      toast.error(apiMessage(err));
    },
  });
}
