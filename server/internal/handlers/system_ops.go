package handlers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/ovh-buy/server/internal/app"
	"github.com/ovh-buy/server/internal/db"
	"github.com/ovh-buy/server/internal/updater"
)

const (
	dbMaintenanceLoopInterval = 6 * time.Hour
	autoBackupCheckInterval   = 1 * time.Hour
	autoBackupPeriod          = 24 * time.Hour
	backupDownloadTokenTTL    = 5 * time.Minute
	backupDownloadHourlyLimit = 10
	backupMaxPendingTickets   = 3
)

// BackupDirForState 统一返回数据卷下的备份目录（兼容 read_only 容器根文件系统）。
func BackupDirForState(state *app.State) string {
	if state != nil && state.Paths.DataDir != "" {
		return filepath.Join(state.Paths.DataDir, "backups")
	}
	if state != nil && state.DB != nil && state.DB.Path != "" {
		return filepath.Join(filepath.Dir(state.DB.Path), "backups")
	}
	return "backups"
}

// SQLiteTempDirForState 返回数据卷下的临时目录（供下载脱敏临时文件及 SQLite 大操作使用）。
func SQLiteTempDirForState(state *app.State) string {
	if state != nil && state.Paths.DataDir != "" {
		return filepath.Join(state.Paths.DataDir, "tmp")
	}
	if state != nil && state.DB != nil && state.DB.Path != "" {
		return filepath.Join(filepath.Dir(state.DB.Path), "tmp")
	}
	return os.TempDir()
}

// readHeapMetricsFast 使用 runtime/metrics 无 STW 读取堆内存与对象指标，避免 runtime.ReadMemStats 周期性全局停顿。
func readHeapMetricsFast() (heapAlloc, heapInuse, sysBytes uint64) {
	samples := []metrics.Sample{
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/memory/classes/heap/unused:bytes"},
		{Name: "/memory/classes/total:bytes"},
	}
	metrics.Read(samples)
	if samples[0].Value.Kind() == metrics.KindUint64 {
		heapAlloc = samples[0].Value.Uint64()
	}
	var heapUnused uint64
	if samples[1].Value.Kind() == metrics.KindUint64 {
		heapUnused = samples[1].Value.Uint64()
	}
	heapInuse = heapAlloc + heapUnused
	if samples[2].Value.Kind() == metrics.KindUint64 {
		sysBytes = samples[2].Value.Uint64()
	}
	return heapAlloc, heapInuse, sysBytes
}

// StartDBMaintenanceLoopWithContext 后台每 6 小时执行一次轻量 WAL 截断检查（PRAGMA wal_checkpoint(TRUNCATE)），
// 绝不在后台自动触发全量阻塞式 VACUUM。
func StartDBMaintenanceLoopWithContext(ctx context.Context, state *app.State) {
	intervalSec := int(dbMaintenanceLoopInterval.Seconds())
	var epoch uint64
	if state != nil && state.Heartbeats != nil {
		epoch = state.Heartbeats.Register("db_maintenance", intervalSec, true)
		defer state.Heartbeats.SetStoppedIf("db_maintenance", epoch)
	}
	initTimer := time.NewTimer(5 * time.Minute)
	defer initTimer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-initTimer.C:
	}

	runOnce := func() {
		if ctx.Err() != nil || state == nil || state.DB == nil {
			return
		}
		res, err := state.DB.CheckpointWAL(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			if errors.Is(err, db.ErrMaintenanceBusy) {
				if state.Heartbeats != nil {
					state.Heartbeats.BeatError("db_maintenance", intervalSec, "维护锁繁忙，本轮 WAL 截断已延后")
				}
				return
			}
			if state.Logger != nil {
				state.Logger.Warn("定期 WAL 截断失败: "+err.Error(), "system")
			}
			if state.Heartbeats != nil {
				state.Heartbeats.BeatError("db_maintenance", intervalSec, err.Error())
			}
			return
		}
		if res.Partial {
			if state.Logger != nil {
				state.Logger.Warn("定期 WAL 截断部分被读事务阻塞（busy=1），将在下轮继续", "system")
			}
			if state.Heartbeats != nil {
				state.Heartbeats.BeatError("db_maintenance", intervalSec, "WAL 截断部分被阻塞，将在下轮重试")
			}
			return
		}
		if state.Logger != nil && res.Reclaimed > 0 {
			state.Logger.Info(fmt.Sprintf("定期 WAL 截断完成：释放 %.1f KiB (WAL %.1f KiB -> %.1f KiB)",
				float64(res.Reclaimed)/1024.0,
				float64(res.Before.WALBytes)/1024.0,
				float64(res.After.WALBytes)/1024.0), "system")
		}
		if state.Heartbeats != nil {
			state.Heartbeats.BeatOK("db_maintenance", intervalSec)
		}
	}

	runOnce()
	ticker := time.NewTicker(dbMaintenanceLoopInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runOnce()
		}
	}
}

// StartAutoBackupLoopWithContext 后台每日自动创建一份 SQLite 在线一致性快照（VACUUM INTO + 瘦身 + quick_check + 原子重命名），
// 启动时统一在维护锁内修正目录权限 0700 并清理 backups 与 tmp 目录下遗留的 .tmp-* 文件。
func StartAutoBackupLoopWithContext(ctx context.Context, state *app.State) {
	intervalSec := int(autoBackupCheckInterval.Seconds())
	var epoch uint64
	if state != nil && state.Heartbeats != nil {
		epoch = state.Heartbeats.Register("auto_backup", intervalSec, true)
		defer state.Heartbeats.SetStoppedIf("auto_backup", epoch)
	}
	backupDir := BackupDirForState(state)
	tmpDir := SQLiteTempDirForState(state)
	_ = db.EnsurePrivateDir(backupDir)
	_ = db.EnsurePrivateDir(tmpDir)
	if state != nil && state.DB != nil {
		if removed := state.DB.CleanupStaleTempFilesInLock(ctx, state.StartedAt, backupDir, tmpDir); removed > 0 && state.Logger != nil {
			state.Logger.Info(fmt.Sprintf("启动已清理 %d 个未完成的备份/下载临时文件", removed), "system")
		}
	}

	initTimer := time.NewTimer(2 * time.Minute)
	defer initTimer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-initTimer.C:
	}

	runCheck := func() {
		if ctx.Err() != nil || state == nil || state.DB == nil {
			return
		}
		list, err := db.ListBackups(backupDir)
		if err != nil {
			if state.Heartbeats != nil {
				state.Heartbeats.BeatError("auto_backup", intervalSec, err.Error())
			}
			return
		}
		var latestAuto time.Time
		for _, b := range list {
			if b.Kind != "auto" {
				continue
			}
			if ts, err := time.Parse(time.RFC3339, b.CreatedAt); err == nil && ts.After(latestAuto) {
				latestAuto = ts
			}
		}
		if !latestAuto.IsZero() && time.Since(latestAuto) < autoBackupPeriod {
			if state.Heartbeats != nil {
				state.Heartbeats.BeatOK("auto_backup", intervalSec)
			}
			return
		}
		info, err := state.DB.CreateConsistentBackup(ctx, backupDir, "auto", db.DefaultAutoBackupKeep, false)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			if errors.Is(err, db.ErrMaintenanceBusy) {
				if state.Heartbeats != nil {
					state.Heartbeats.BeatError("auto_backup", intervalSec, "维护锁繁忙，本轮自动备份已延后")
				}
				return
			}
			if state.Logger != nil {
				state.Logger.Warn("自动备份数据库失败: "+err.Error(), "system")
			}
			if state.Heartbeats != nil {
				state.Heartbeats.BeatError("auto_backup", intervalSec, err.Error())
			}
			return
		}
		if state.Logger != nil {
			state.Logger.Info(fmt.Sprintf("已自动创建数据库一致性备份: %s (%.2f MiB)",
				info.Filename, float64(info.SizeBytes)/(1024*1024)), "system")
		}
		if state.Heartbeats != nil {
			state.Heartbeats.BeatOK("auto_backup", intervalSec)
		}
	}

	runCheck()
	ticker := time.NewTicker(autoBackupCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runCheck()
		}
	}
}

// GetSystemStatus GET /api/system/status
// 返回当前运行模式（Docker/裸机）、进程内存/协程/运行时长、SQLite 体积与空闲页统计、备份摘要与全部后台循环心跳。
// 不返回宿主机或容器绝对路径。
func GetSystemStatus(state *app.State) gin.HandlerFunc {
	return func(c *gin.Context) {
		heapAlloc, heapInuse, sysBytes := readHeapMetricsFast()

		var dbStats db.MaintenanceStats
		if state != nil && state.DB != nil {
			dbStats, _ = state.DB.MaintenanceStatsSnapshot()
		}

		backupDir := BackupDirForState(state)
		backups, _ := db.ListBackups(backupDir)
		autoCount := 0
		manualCount := 0
		latestBackupAt := ""
		var totalBackupBytes int64
		for _, b := range backups {
			totalBackupBytes += b.SizeBytes
			if b.Kind == "auto" {
				autoCount++
			} else {
				manualCount++
			}
			if latestBackupAt == "" {
				latestBackupAt = b.CreatedAt
			}
		}

		uptimeSec := int64(0)
		startedAt := ""
		if state != nil && !state.StartedAt.IsZero() {
			uptimeSec = int64(time.Since(state.StartedAt).Seconds())
			startedAt = state.StartedAt.UTC().Format(time.RFC3339)
		}

		var loops []app.LoopHeartbeat
		if state != nil && state.Heartbeats != nil {
			loops = state.Heartbeats.Snapshot()
		}
		if loops == nil {
			loops = []app.LoopHeartbeat{}
		}

		c.JSON(http.StatusOK, gin.H{
			"runtime": gin.H{
				"version":     Version,
				"inContainer": updater.InContainer(),
				"goVersion":   runtime.Version(),
				"goos":        runtime.GOOS,
				"goarch":      runtime.GOARCH,
				"goroutines":  runtime.NumGoroutine(),
				"heapAlloc":   heapAlloc,
				"heapInuse":   heapInuse,
				"sysBytes":    sysBytes,
				"startedAt":   startedAt,
				"uptimeSec":   uptimeSec,
			},
			"database": dbStats,
			"backups": gin.H{
				"keepAuto":    db.DefaultAutoBackupKeep,
				"autoPeriodH": int(autoBackupPeriod.Hours()),
				"totalCount":  len(backups),
				"autoCount":   autoCount,
				"manualCount": manualCount,
				"totalBytes":  totalBackupBytes,
				"latestAt":    latestBackupAt,
			},
			"heartbeats": loops,
		})
	}
}

// MaintainDatabase POST /api/system/db/maintain
// 手动执行 WAL 截断（checkpoint）或全量空间整理（vacuum），共用全局维护锁防止并发冲突。
func MaintainDatabase(state *app.State) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req struct {
			Action string `json:"action"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "invalid request body"})
			return
		}
		action := strings.ToLower(strings.TrimSpace(req.Action))
		if action != "checkpoint" && action != "vacuum" {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "action 必须是 checkpoint 或 vacuum"})
			return
		}
		if state == nil || state.DB == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"success": false, "error": "数据库未就绪"})
			return
		}

		var (
			res db.MaintainResult
			err error
		)
		if action == "checkpoint" {
			res, err = state.DB.CheckpointWAL(c.Request.Context())
		} else {
			res, err = state.DB.Vacuum(c.Request.Context())
		}
		if err != nil {
			code := http.StatusInternalServerError
			if errors.Is(err, db.ErrMaintenanceBusy) {
				code = http.StatusConflict
			} else if errors.Is(err, db.ErrInsufficientSpace) {
				code = http.StatusInsufficientStorage
			} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				code = http.StatusRequestTimeout
			}
			c.JSON(code, gin.H{"success": false, "error": err.Error()})
			return
		}

		if state.Logger != nil {
			state.Logger.Info(fmt.Sprintf("手动数据库维护(%s)完成：总大小 %.2f MiB -> %.2f MiB，partial=%v，耗时 %dms",
				action,
				float64(res.Before.TotalBytes)/(1024*1024),
				float64(res.After.TotalBytes)/(1024*1024),
				res.Partial,
				res.DurationMs), "system")
		}
		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"result":  res,
		})
	}
}

// ─── 一次性备份下载凭证管理（5 分钟 TTL + 单次消费 + 签发即计数限流 + 未消费上限） ──

type backupDownloadTicket struct {
	backupID  string
	redacted  bool
	expiresAt time.Time
}

type backupTicketStore struct {
	mu              sync.Mutex
	tickets         map[string]backupDownloadTicket
	issueTimestamps []time.Time
	nowFn           func() time.Time
}

var backupTickets = &backupTicketStore{
	tickets: map[string]backupDownloadTicket{},
	nowFn:   time.Now,
}

func (s *backupTicketStore) now() time.Time {
	if s.nowFn != nil {
		return s.nowFn()
	}
	return time.Now()
}

func (s *backupTicketStore) issue(backupID string, redacted bool) (string, time.Time, error) {
	var raw [24]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", time.Time{}, err
	}
	tok := hex.EncodeToString(raw[:])
	now := s.now()
	exp := now.Add(backupDownloadTokenTTL)

	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range s.tickets {
		if now.After(v.expiresAt) {
			delete(s.tickets, k)
		}
	}
	// 防止并发堆积过多未消费 ticket
	if len(s.tickets) >= backupMaxPendingTickets {
		return "", time.Time{}, errors.New("当前待处理的备份下载请求过多，请稍后再试")
	}
	// 签发即计数：杜绝先批量签发再并发消费绕过每小时限流
	cutoff := now.Add(-time.Hour)
	kept := s.issueTimestamps[:0]
	for _, ts := range s.issueTimestamps {
		if ts.After(cutoff) {
			kept = append(kept, ts)
		}
	}
	s.issueTimestamps = kept
	if len(s.issueTimestamps) >= backupDownloadHourlyLimit {
		return "", time.Time{}, errors.New("本小时备份下载次数已达上限，请稍后再试")
	}
	s.issueTimestamps = append(s.issueTimestamps, now)
	s.tickets[tok] = backupDownloadTicket{
		backupID:  backupID,
		redacted:  redacted,
		expiresAt: exp,
	}
	return tok, exp, nil
}

func (s *backupTicketStore) consume(token, backupID string) (backupDownloadTicket, bool) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tickets[token]
	if !ok {
		return backupDownloadTicket{}, false
	}
	delete(s.tickets, token) // 单次消费：无论校验是否匹配，使用后立刻作废
	if now.After(t.expiresAt) || t.backupID != backupID {
		return backupDownloadTicket{}, false
	}
	return t, true
}

// ListSystemBackups GET /api/system/backups
func ListSystemBackups(state *app.State) gin.HandlerFunc {
	return func(c *gin.Context) {
		dir := BackupDirForState(state)
		items, err := db.ListBackups(dir)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"success":  true,
			"keepAuto": db.DefaultAutoBackupKeep,
			"backups":  items,
		})
	}
}

// CreateSystemBackup POST /api/system/backups
// 手动创建一致性 SQLite 快照（支持 redact=true 生成不含凭据的脱敏副本；默认保留业务库用于灾备恢复）。
func CreateSystemBackup(state *app.State) gin.HandlerFunc {
	return func(c *gin.Context) {
		if state == nil || state.DB == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"success": false, "error": "数据库未就绪"})
			return
		}
		var req struct {
			Redact bool `json:"redact"`
		}
		_ = c.ShouldBindJSON(&req)

		dir := BackupDirForState(state)
		info, err := state.DB.CreateConsistentBackup(c.Request.Context(), dir, "manual", db.DefaultAutoBackupKeep, req.Redact)
		if err != nil {
			code := http.StatusInternalServerError
			if errors.Is(err, db.ErrMaintenanceBusy) {
				code = http.StatusConflict
			} else if errors.Is(err, db.ErrInsufficientSpace) {
				code = http.StatusInsufficientStorage
			}
			c.JSON(code, gin.H{"success": false, "error": err.Error()})
			return
		}
		if state.Logger != nil {
			state.Logger.Info(fmt.Sprintf("已手动创建数据库备份: %s (%.2f MiB, credentials=%s)",
				info.Filename, float64(info.SizeBytes)/(1024*1024), info.Credentials), "system")
		}
		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"backup":  info,
		})
	}
}

// IssueBackupDownloadToken POST /api/system/backups/:id/download-token
// 颁布 5 分钟有效、单次消费的备份下载令牌（不强绑回源 IP 以兼容 Cloudflare / CDN 多边缘回源）。
func IssueBackupDownloadToken(state *app.State) gin.HandlerFunc {
	return func(c *gin.Context) {
		backupID := strings.TrimSpace(c.Param("id"))
		dir := BackupDirForState(state)
		if _, _, err := db.ResolveBackupPathByID(dir, backupID); err != nil {
			c.JSON(http.StatusNotFound, gin.H{"success": false, "error": "备份不存在"})
			return
		}

		var req struct {
			Redact *bool `json:"redact"`
		}
		_ = c.ShouldBindJSON(&req)
		redact := true
		if req.Redact != nil {
			redact = *req.Redact
		}

		tok, exp, err := backupTickets.issue(backupID, redact)
		if err != nil {
			if state != nil && state.Logger != nil {
				state.Logger.Warn(fmt.Sprintf("[audit] 签发备份下载令牌被拒绝：id=%s redact=%v ip=%s reason=%s",
					backupID, redact, c.ClientIP(), err.Error()), "system")
			}
			c.JSON(http.StatusTooManyRequests, gin.H{"success": false, "error": err.Error()})
			return
		}
		if state != nil && state.Logger != nil {
			state.Logger.Info(fmt.Sprintf("[audit] 签发备份下载令牌：id=%s redacted=%v ip=%s",
				backupID, redact, c.ClientIP()), "system")
		}
		c.JSON(http.StatusOK, gin.H{
			"success":   true,
			"token":     tok,
			"redacted":  redact,
			"expiresAt": exp.UTC().Format(time.RFC3339),
		})
	}
}

// DownloadSystemBackup POST /api/system/backups/:id/download
// 必须经主密钥 X-API-Key 鉴权并携带一次性下载令牌（POST + JSON 拒绝浏览器导航/表单触发），流式输出并记录完整审计日志。
func DownloadSystemBackup(state *app.State) gin.HandlerFunc {
	return func(c *gin.Context) {
		backupID := strings.TrimSpace(c.Param("id"))
		var req struct {
			Token string `json:"token"`
		}
		if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.Token) == "" {
			if state != nil && state.Logger != nil {
				state.Logger.Warn(fmt.Sprintf("[audit] 备份下载被拒（缺少令牌）：id=%s ip=%s", backupID, c.ClientIP()), "system")
			}
			c.JSON(http.StatusNotFound, gin.H{"success": false, "error": "下载凭证无效或已过期"})
			return
		}

		ticket, ok := backupTickets.consume(strings.TrimSpace(req.Token), backupID)
		if !ok {
			if state != nil && state.Logger != nil {
				state.Logger.Warn(fmt.Sprintf("[audit] 备份下载被拒（令牌无效/已过期/ID不匹配）：id=%s ip=%s", backupID, c.ClientIP()), "system")
			}
			c.JSON(http.StatusNotFound, gin.H{"success": false, "error": "下载凭证无效或已过期"})
			return
		}

		dir := BackupDirForState(state)
		realPath, filename, err := db.ResolveBackupPathByID(dir, backupID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"success": false, "error": "备份不存在"})
			return
		}

		servePath := realPath
		downloadName := filename
		if ticket.redacted {
			tmpPath, created, rerr := db.CreateRedactedDownloadTemp(c.Request.Context(), realPath, SQLiteTempDirForState(state))
			if rerr != nil {
				code := http.StatusInternalServerError
				if errors.Is(rerr, db.ErrMaintenanceBusy) {
					code = http.StatusConflict
				} else if errors.Is(rerr, db.ErrInsufficientSpace) {
					code = http.StatusInsufficientStorage
				}
				if state != nil && state.Logger != nil {
					state.Logger.Warn(fmt.Sprintf("[audit] 生成脱敏备份下载副本失败：id=%s ip=%s err=%s", backupID, c.ClientIP(), rerr.Error()), "system")
				}
				c.JSON(code, gin.H{"success": false, "error": "生成脱敏备份副本失败: " + rerr.Error()})
				return
			}
			if created {
				defer os.Remove(tmpPath)
				servePath = tmpPath
				if !strings.HasSuffix(downloadName, "-redacted.db") {
					downloadName = strings.TrimSuffix(downloadName, ".db") + "-redacted.db"
				}
			}
		}

		f, err := os.Open(servePath)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"success": false, "error": "备份不存在"})
			return
		}
		defer f.Close()
		fi, err := f.Stat()
		if err != nil || !fi.Mode().IsRegular() {
			c.JSON(http.StatusNotFound, gin.H{"success": false, "error": "备份不存在"})
			return
		}

		if state != nil && state.Logger != nil {
			state.Logger.Info(fmt.Sprintf("[audit] 备份下载开始：id=%s redacted=%v size=%d ip=%s",
				backupID, ticket.redacted, fi.Size(), c.ClientIP()), "system")
		}

		c.Header("Content-Type", "application/octet-stream")
		c.Header("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, downloadName))
		c.Header("Content-Length", strconv.FormatInt(fi.Size(), 10))
		c.Header("Cache-Control", "no-store")
		c.Header("X-Content-Type-Options", "nosniff")
		c.Status(http.StatusOK)
		_, _ = io.Copy(c.Writer, f)
	}
}
