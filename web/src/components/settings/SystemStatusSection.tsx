import { useTranslation } from "react-i18next";
import {
  Activity,
  Database,
  Download,
  HardDrive,
  RefreshCw,
  Server,
  ShieldAlert,
  ShieldCheck,
  Sparkles,
  Wrench,
} from "lucide-react";
import { Section } from "@/routes/settings-sections";
import { Button } from "@/components/ui/button";
import { LoadFailedBanner } from "@/components/common/LoadFailed";
import { StatusDot } from "@/components/common/StatusDot";
import { useConfirm } from "@/components/common/ConfirmDialog";
import { fmtDateTime } from "@/i18n/format";
import { cn } from "@/lib/utils";
import {
  useCreateBackup,
  useDownloadBackup,
  useMaintainDatabase,
  useSystemBackups,
  useSystemStatus,
  type BackupItem,
  type LoopHeartbeat,
} from "@/hooks/use-system-ops";

function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
  const units = ["B", "KiB", "MiB", "GiB"];
  let v = bytes;
  let idx = 0;
  while (v >= 1024 && idx < units.length - 1) {
    v /= 1024;
    idx++;
  }
  return `${v >= 100 || idx === 0 ? v.toFixed(0) : v.toFixed(2)} ${units[idx]}`;
}

function formatUptime(sec: number, t: (k: string, opts?: Record<string, unknown>) => string): string {
  if (!Number.isFinite(sec) || sec < 0) return "—";
  const d = Math.floor(sec / 86400);
  const h = Math.floor((sec % 86400) / 3600);
  const m = Math.floor((sec % 3600) / 60);
  if (d > 0) return t("settings.system.uptimeDays", { d, h, m });
  if (h > 0) return t("settings.system.uptimeHours", { h, m });
  return t("settings.system.uptimeMins", { m: Math.max(1, m) });
}

function heartbeatLabel(name: string, t: (k: string) => string): string {
  const key = `settings.system.loops.${name}`;
  const translated = t(key);
  return translated === key ? name : translated;
}

function HeartbeatRow({ loop }: { loop: LoopHeartbeat }) {
  const { t } = useTranslation();
  const statusTone = !loop.running
    ? "muted"
    : loop.healthy
      ? "success"
      : loop.stale || loop.lastError
        ? "warning"
        : "info";

  const statusText = !loop.running
    ? t("settings.system.loopStopped")
    : loop.healthy
      ? t("settings.system.loopHealthy")
      : loop.stale
        ? t("settings.system.loopStale")
        : loop.lastError
          ? t("settings.system.loopError")
          : t("settings.system.loopStarting");

  return (
    <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 rounded-xl border border-border px-3.5 py-2.5 text-[12px]">
      <div className="flex items-center gap-2.5 min-w-0">
        <StatusDot
          tone={statusTone}
          pulse={loop.running && loop.healthy}
        />
        <div className="min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="font-medium text-[13px]">{heartbeatLabel(loop.name, t)}</span>
            <span className="text-[11px] font-mono text-muted-foreground">{loop.name}</span>
          </div>
          {loop.lastError && (
            <p className="text-[11px] text-destructive mt-0.5 break-all">{loop.lastError}</p>
          )}
        </div>
      </div>
      <div className="flex items-center gap-3 text-[11px] text-muted-foreground sm:text-right flex-shrink-0">
        <div>
          <div className="font-medium text-foreground">{statusText}</div>
          <div>
            {loop.lastBeatAt
              ? t("settings.system.lastBeat", { time: fmtDateTime(loop.lastBeatAt) })
              : t("settings.system.neverBeat")}
          </div>
        </div>
      </div>
    </div>
  );
}

export function SystemStatusSection() {
  const { t } = useTranslation();
  const status = useSystemStatus(true);
  const backups = useSystemBackups(true);
  const maintain = useMaintainDatabase();
  const createBackup = useCreateBackup();
  const downloadBackup = useDownloadBackup();
  const [confirm, confirmDialog] = useConfirm();

  const handleVacuum = async () => {
    const ok = await confirm({
      title: t("settings.system.vacuumConfirmTitle"),
      description: t("settings.system.vacuumConfirmDesc"),
      confirmText: t("settings.system.vacuumBtn"),
    });
    if (ok) maintain.mutate("vacuum");
  };

  const handleRawDownload = async (item: BackupItem) => {
    const ok = await confirm({
      title: t("settings.system.rawDownloadConfirmTitle"),
      description: t("settings.system.rawDownloadConfirmDesc"),
      confirmText: t("settings.system.downloadFull"),
      tone: "danger",
    });
    if (ok) downloadBackup.mutate({ id: item.id, filename: item.filename, redact: false });
  };

  const rt = status.data?.runtime;
  const db = status.data?.database;
  const bkSummary = status.data?.backups;
  const loops = status.data?.heartbeats || [];

  const busy = Boolean(db?.maintenanceBusy) || maintain.isPending || createBackup.isPending;

  return (
    <Section title={t("settings.system.title")}>
      <div className="flex items-center justify-between gap-2">
        <p className="text-[12px] text-muted-foreground">{t("settings.system.subtitle")}</p>
        <Button
          type="button"
          size="sm"
          variant="outline"
          onClick={() => {
            status.refetch();
            backups.refetch();
          }}
          disabled={status.isFetching || backups.isFetching}
        >
          <RefreshCw className={cn("w-3.5 h-3.5", (status.isFetching || backups.isFetching) && "animate-spin")} />
          {t("common.refresh")}
        </Button>
      </div>

      {status.isError && (
        <LoadFailedBanner
          title={t("settings.system.loadFailed")}
          error={status.error}
          onRetry={() => status.refetch()}
        />
      )}

      {/* 1. 运行环境与进程资源 */}
      <div className="rounded-2xl border border-border p-4 space-y-3">
        <div className="flex items-center gap-2 text-[13px] font-medium">
          <Server className="w-4 h-4 text-muted-foreground" />
          <span>{t("settings.system.runtimeTitle")}</span>
        </div>
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-[12px]">
          <div className="rounded-xl bg-secondary/40 px-3 py-2">
            <div className="text-[11px] text-muted-foreground">{t("settings.system.deployMode")}</div>
            <div className="font-medium mt-0.5">
              {rt ? (rt.inContainer ? t("settings.system.modeDocker") : t("settings.system.modeBareMetal")) : "—"}
            </div>
          </div>
          <div className="rounded-xl bg-secondary/40 px-3 py-2">
            <div className="text-[11px] text-muted-foreground">{t("settings.system.version")}</div>
            <div className="font-mono font-medium mt-0.5">{rt ? `v${rt.version}` : "—"}</div>
          </div>
          <div className="rounded-xl bg-secondary/40 px-3 py-2">
            <div className="text-[11px] text-muted-foreground">{t("settings.system.uptime")}</div>
            <div className="font-medium mt-0.5">{rt ? formatUptime(rt.uptimeSec, t) : "—"}</div>
          </div>
          <div className="rounded-xl bg-secondary/40 px-3 py-2">
            <div className="text-[11px] text-muted-foreground">{t("settings.system.memoryGoroutines")}</div>
            <div className="font-mono font-medium mt-0.5">
              {rt ? `${formatBytes(rt.heapAlloc)} · ${rt.goroutines}g` : "—"}
            </div>
          </div>
        </div>
      </div>

      {/* 2. 后台循环心跳 */}
      <div className="rounded-2xl border border-border p-4 space-y-3">
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-2 text-[13px] font-medium">
            <Activity className="w-4 h-4 text-muted-foreground" />
            <span>{t("settings.system.heartbeatsTitle")}</span>
          </div>
          <span className="text-[11px] text-muted-foreground">
            {t("settings.system.heartbeatsCount", {
              running: loops.filter((l) => l.running).length,
              total: loops.length,
            })}
          </span>
        </div>
        <div className="space-y-2">
          {loops.length === 0 ? (
            <p className="text-[12px] text-muted-foreground">{t("common.empty")}</p>
          ) : (
            loops.map((loop) => <HeartbeatRow key={loop.name} loop={loop} />)
          )}
        </div>
      </div>

      {/* 3. SQLite 数据库健康与维护 */}
      <div className="rounded-2xl border border-border p-4 space-y-3">
        <div className="flex items-center justify-between gap-2 flex-wrap">
          <div className="flex items-center gap-2 text-[13px] font-medium">
            <Database className="w-4 h-4 text-muted-foreground" />
            <span>{t("settings.system.dbTitle")}</span>
          </div>
          <div className="flex items-center gap-2">
            <Button
              type="button"
              size="sm"
              variant="outline"
              disabled={busy}
              onClick={() => maintain.mutate("checkpoint")}
            >
              <Wrench className="w-3.5 h-3.5" />
              {maintain.isPending && maintain.variables === "checkpoint"
                ? t("settings.system.runningAction")
                : t("settings.system.checkpointBtn")}
            </Button>
            <Button
              type="button"
              size="sm"
              variant="outline"
              disabled={busy}
              onClick={handleVacuum}
            >
              <Sparkles className="w-3.5 h-3.5" />
              {maintain.isPending && maintain.variables === "vacuum"
                ? t("settings.system.runningAction")
                : t("settings.system.vacuumBtn")}
            </Button>
          </div>
        </div>

        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-[12px]">
          <div className="rounded-xl bg-secondary/40 px-3 py-2">
            <div className="text-[11px] text-muted-foreground">{t("settings.system.dbTotalSize")}</div>
            <div className="font-mono font-medium mt-0.5">{db ? formatBytes(db.totalBytes) : "—"}</div>
          </div>
          <div className="rounded-xl bg-secondary/40 px-3 py-2">
            <div className="text-[11px] text-muted-foreground">{t("settings.system.dbMainWal")}</div>
            <div className="font-mono font-medium mt-0.5">
              {db ? `${formatBytes(db.dbBytes)} / ${formatBytes(db.walBytes)}` : "—"}
            </div>
          </div>
          <div className="rounded-xl bg-secondary/40 px-3 py-2">
            <div className="text-[11px] text-muted-foreground">{t("settings.system.dbFreelist")}</div>
            <div className="font-mono font-medium mt-0.5">
              {db ? `${formatBytes(db.freelistBytes)} (${db.freelistPages})` : "—"}
            </div>
          </div>
          <div className="rounded-xl bg-secondary/40 px-3 py-2">
            <div className="text-[11px] text-muted-foreground">{t("settings.system.diskFree")}</div>
            <div className="font-mono font-medium mt-0.5">
              {db && (db.diskAvailBytes ?? 0) > 0 ? formatBytes(db.diskAvailBytes ?? 0) : "—"}
            </div>
          </div>
        </div>
        <p className="text-[11px] text-muted-foreground leading-relaxed">{t("settings.system.dbHint")}</p>
      </div>

      {/* 4. 自动化备份与安全导出 */}
      <div className="rounded-2xl border border-border p-4 space-y-3">
        <div className="flex items-center justify-between gap-2 flex-wrap">
          <div className="flex items-center gap-2 text-[13px] font-medium">
            <HardDrive className="w-4 h-4 text-muted-foreground" />
            <span>{t("settings.system.backupsTitle")}</span>
          </div>
          <div className="flex items-center gap-2 flex-wrap">
            <Button
              type="button"
              size="sm"
              disabled={busy}
              onClick={() => createBackup.mutate(false)}
            >
              {createBackup.isPending ? t("settings.system.creatingBackup") : t("settings.system.createBackupBtn")}
            </Button>
          </div>
        </div>

        <p className="text-[11px] text-muted-foreground leading-relaxed">
          {t("settings.system.backupsPolicy", {
            hours: bkSummary?.autoPeriodH ?? 24,
            keep: bkSummary?.keepAuto ?? 14,
            total: bkSummary?.totalCount ?? 0,
            size: formatBytes(bkSummary?.totalBytes ?? 0),
          })}
        </p>

        {backups.isError ? (
          <LoadFailedBanner
            title={t("settings.system.backupsLoadFailed")}
            error={backups.error}
            onRetry={() => backups.refetch()}
          />
        ) : (backups.data?.backups || []).length === 0 ? (
          <p className="text-[12px] text-muted-foreground">{t("settings.system.noBackups")}</p>
        ) : (
          <div className="space-y-2 max-h-[320px] overflow-y-auto pr-1">
            {(backups.data?.backups || []).map((item) => (
              <div
                key={item.id}
                className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 rounded-xl border border-border px-3.5 py-2.5 text-[12px]"
              >
                <div className="min-w-0">
                  <div className="flex items-center gap-2 flex-wrap">
                    <span className="font-mono font-medium truncate">{item.filename}</span>
                    <span className="rounded-md bg-secondary px-1.5 py-0.5 text-[10px] font-medium">
                      {item.kind === "auto" ? t("settings.system.kindAuto") : t("settings.system.kindManual")}
                    </span>
                    <span className="inline-flex items-center gap-1 rounded-md bg-secondary px-1.5 py-0.5 text-[10px] text-muted-foreground">
                      {item.credentials === "redacted" ? (
                        <>
                          <ShieldCheck className="w-3 h-3 text-emerald-500" />
                          {t("settings.system.credsRedacted")}
                        </>
                      ) : (
                        <>
                          <ShieldAlert className="w-3 h-3 text-amber-500" />
                          {t("settings.system.credsEncrypted")}
                        </>
                      )}
                    </span>
                  </div>
                  <div className="text-[11px] text-muted-foreground mt-0.5">
                    {formatBytes(item.sizeBytes)} · {fmtDateTime(item.createdAt)}
                  </div>
                </div>
                <div className="flex items-center gap-1.5 flex-shrink-0">
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    disabled={downloadBackup.isPending}
                    onClick={() => downloadBackup.mutate({ id: item.id, filename: item.filename, redact: true })}
                  >
                    <Download className="w-3.5 h-3.5" />
                    {t("settings.system.downloadRedacted")}
                  </Button>
                  {item.credentials !== "redacted" && (
                    <Button
                      type="button"
                      size="sm"
                      variant="ghost"
                      disabled={downloadBackup.isPending}
                      onClick={() => handleRawDownload(item)}
                    >
                      {t("settings.system.downloadFull")}
                    </Button>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {confirmDialog}
    </Section>
  );
}
