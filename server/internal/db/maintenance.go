package db

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	// ErrMaintenanceBusy 当另一个数据库维护（WAL checkpoint / VACUUM / 备份）正在执行时返回。
	ErrMaintenanceBusy = errors.New("数据库维护任务正在执行，请稍后再试")
	// ErrInsufficientSpace 当数据卷剩余磁盘空间不足以安全执行 VACUUM 或备份时返回。
	ErrInsufficientSpace = errors.New("数据卷剩余磁盘空间不足")
)

const (
	// AutoBackupPrefix 自动定时备份文件前缀（与手动备份严格区分，清理策略仅作用于此前缀）。
	AutoBackupPrefix = "sniper-auto-"
	// ManualBackupPrefix 手动备份文件前缀（永远不会被自动保留策略误删）。
	ManualBackupPrefix = "sniper-manual-"
	// DefaultAutoBackupKeep 默认保留的自动备份份数。
	DefaultAutoBackupKeep = 14
	// MinDiskFreeFloorBytes 自动备份完成后仍必须保留的最低磁盘安全余量（512 MiB）。
	MinDiskFreeFloorBytes uint64 = 512 << 20
	// ManualDiskFreeFloorBytes 手动备份/脱敏副本完成后仍必须保留的最低磁盘安全余量（128 MiB）。
	ManualDiskFreeFloorBytes uint64 = 128 << 20
	// MaxAutoBackupTotalBytes 自动备份目录允许占用的总空间软上限（512 MiB）。
	MaxAutoBackupTotalBytes int64 = 512 << 20
)

// availableDiskBytesFn 允许单测注入模拟磁盘可用空间以验证水位拒绝逻辑。
var availableDiskBytesFn = availableDiskBytes

var strictBackupFilenameRe = regexp.MustCompile(`^sniper-(auto|manual)-\d{8}-\d{6}-[0-9a-f]{6}(-redacted)?\.db$`)

// MaintenanceStats 描述 SQLite 主库、WAL、空闲页与最近维护/空间概况。
type MaintenanceStats struct {
	Driver             string `json:"driver"`
	DBBytes            int64  `json:"dbBytes"`
	WALBytes           int64  `json:"walBytes"`
	SHMBytes           int64  `json:"shmBytes"`
	TotalBytes         int64  `json:"totalBytes"`
	PageSize           int64  `json:"pageSize"`
	PageCount          int64  `json:"pageCount"`
	FreelistPages      int64  `json:"freelistPages"`
	FreelistBytes      int64  `json:"freelistBytes"`
	JournalMode        string `json:"journalMode"`
	AutoVacuum         int    `json:"autoVacuum"`
	DiskAvailBytes     uint64 `json:"diskAvailBytes,omitempty"`
	VacuumSpaceOK      bool   `json:"vacuumSpaceOk"`
	LastCheckpointAt   string `json:"lastCheckpointAt,omitempty"`
	LastVacuumAt       string `json:"lastVacuumAt,omitempty"`
	LastMaintainAction string `json:"lastMaintainAction,omitempty"`
	MaintenanceBusy    bool   `json:"maintenanceBusy"`
}

// MaintainResult 记录单次数据库维护前后的实际体积与页数变化。
type MaintainResult struct {
	Action      string           `json:"action"`
	Before      MaintenanceStats `json:"before"`
	After       MaintenanceStats `json:"after"`
	Reclaimed   int64            `json:"reclaimed"`
	Busy        int              `json:"busy"`
	Partial     bool             `json:"partial"`
	LogFrames   int              `json:"logFrames"`
	Checkpoints int              `json:"checkpoints"`
	DurationMs  int64            `json:"durationMs"`
}

// BackupInfo 描述一份已验证并原子落盘的 SQLite 一致性备份快照。
type BackupInfo struct {
	ID          string `json:"id"`
	Filename    string `json:"filename"`
	Kind        string `json:"kind"` // "auto" | "manual"
	SizeBytes   int64  `json:"sizeBytes"`
	SHA256      string `json:"sha256,omitempty"`
	CreatedAt   string `json:"createdAt"`
	Credentials string `json:"credentials"` // "encrypted" | "redacted"
}

// dbMaintState 保存每个 *DB 实例的进程内维护互斥与最近执行时间。
type dbMaintState struct {
	mu                 sync.Mutex
	busy               bool
	lastCheckpointAt   time.Time
	lastVacuumAt       time.Time
	lastMaintainAction string
}

var (
	maintStatesMu sync.Mutex
	maintStates   = map[*DB]*dbMaintState{}
	// dlRedactSem 限制并发生成脱敏下载副本上限为 1，防止并发下载将 /data 空间瞬间放大 3xN。
	dlRedactSem = make(chan struct{}, 1)
)

func (db *DB) maintState() *dbMaintState {
	maintStatesMu.Lock()
	defer maintStatesMu.Unlock()
	s, ok := maintStates[db]
	if !ok {
		s = &dbMaintState{}
		maintStates[db] = s
	}
	return s
}

// CleanupMaintState 释放关闭后的 *DB 在维护状态表里的登记，避免测试中累积引用。
func (db *DB) CleanupMaintState() {
	maintStatesMu.Lock()
	delete(maintStates, db)
	maintStatesMu.Unlock()
}

// withMaintenanceLock 使用“进程内互斥 + 跨进程 flock”串行化所有重维护与备份动作。
func (db *DB) withMaintenanceLock(ctx context.Context, fn func() error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ms := db.maintState()
	ms.mu.Lock()
	if ms.busy {
		ms.mu.Unlock()
		return ErrMaintenanceBusy
	}
	ms.busy = true
	ms.mu.Unlock()

	defer func() {
		ms.mu.Lock()
		ms.busy = false
		ms.mu.Unlock()
	}()

	dataDir := filepath.Dir(db.Path)
	fh, err := acquireMaintenanceFileLock(dataDir)
	if err != nil {
		return err
	}
	defer fh.Release()

	return fn()
}

func fileSizeOrZero(p string) int64 {
	fi, err := os.Stat(p)
	if err != nil || fi.IsDir() {
		return 0
	}
	return fi.Size()
}

// offlineReadWriteDSN 返回用于离线备份副本操作的独立 DSN（显式 journal_mode=DELETE，绝不走 WAL）。
func offlineReadWriteDSN(path string) string {
	uri := sqliteFileURI(path)
	if driverName == "sqlite" {
		return uri + "?_pragma=journal_mode(DELETE)&_pragma=synchronous(FULL)&_pragma=busy_timeout(5000)"
	}
	return uri + "?_journal_mode=DELETE&_synchronous=FULL&_busy_timeout=5000"
}

// offlineReadOnlyDSN 返回只读校验 DSN（immutable=1 禁止 SQLite 改写文件头或创建 -wal/-shm）。
func offlineReadOnlyDSN(path string) string {
	return sqliteFileURI(path) + "?mode=ro&immutable=1"
}

// MaintenanceStatsSnapshot 读取当前 SQLite 物理体积、WAL 体积、空闲页数与磁盘余量。
// 不覆盖嵌入式 *sqlx.DB 的 Stats() 方法，且不向外暴露宿主机/容器绝对路径。
func (db *DB) MaintenanceStatsSnapshot() (MaintenanceStats, error) {
	st := MaintenanceStats{
		Driver: db.Driver,
	}
	st.DBBytes = fileSizeOrZero(db.Path)
	st.WALBytes = fileSizeOrZero(db.Path + "-wal")
	st.SHMBytes = fileSizeOrZero(db.Path + "-shm")
	st.TotalBytes = st.DBBytes + st.WALBytes + st.SHMBytes

	_ = db.Get(&st.PageSize, `PRAGMA page_size`)
	_ = db.Get(&st.PageCount, `PRAGMA page_count`)
	_ = db.Get(&st.FreelistPages, `PRAGMA freelist_count`)
	_ = db.Get(&st.JournalMode, `PRAGMA journal_mode`)
	_ = db.Get(&st.AutoVacuum, `PRAGMA auto_vacuum`)
	st.FreelistBytes = st.PageSize * st.FreelistPages

	dataDir := filepath.Dir(db.Path)
	if avail, ok := availableDiskBytesFn(dataDir); ok {
		st.DiskAvailBytes = avail
		need := uint64(st.DBBytes+st.WALBytes)*2 + ManualDiskFreeFloorBytes
		st.VacuumSpaceOK = avail >= need
	} else {
		st.VacuumSpaceOK = true
	}

	ms := db.maintState()
	ms.mu.Lock()
	if !ms.lastCheckpointAt.IsZero() {
		st.LastCheckpointAt = ms.lastCheckpointAt.UTC().Format(time.RFC3339)
	}
	if !ms.lastVacuumAt.IsZero() {
		st.LastVacuumAt = ms.lastVacuumAt.UTC().Format(time.RFC3339)
	}
	st.LastMaintainAction = ms.lastMaintainAction
	st.MaintenanceBusy = ms.busy
	ms.mu.Unlock()
	return st, nil
}

// CheckpointWAL 在全局维护锁内执行一次 PRAGMA wal_checkpoint(TRUNCATE)，安全截断累积的 WAL 文件。
func (db *DB) CheckpointWAL(ctx context.Context) (MaintainResult, error) {
	var res MaintainResult
	start := time.Now()
	err := db.withMaintenanceLock(ctx, func() error {
		before, err := db.MaintenanceStatsSnapshot()
		if err != nil {
			return err
		}
		res.Action = "checkpoint"
		res.Before = before

		var busy, logFrames, checkpointed int
		row := db.QueryRowContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`)
		if err := row.Scan(&busy, &logFrames, &checkpointed); err != nil {
			return fmt.Errorf("wal_checkpoint(TRUNCATE): %w", err)
		}
		res.Busy = busy
		res.Partial = busy != 0
		res.LogFrames = logFrames
		res.Checkpoints = checkpointed

		ms := db.maintState()
		ms.mu.Lock()
		ms.lastCheckpointAt = time.Now()
		ms.lastMaintainAction = "checkpoint"
		ms.mu.Unlock()

		after, err := db.MaintenanceStatsSnapshot()
		if err != nil {
			return err
		}
		res.After = after
		res.Reclaimed = before.TotalBytes - after.TotalBytes
		res.DurationMs = time.Since(start).Milliseconds()
		return nil
	})
	return res, err
}

// Vacuum 在全局维护锁内检查磁盘余量（>= 2x 数据库体积）后执行全量 VACUUM 并截断 WAL。
// 仅用于人工运维触发，不作为高频定期动作。
func (db *DB) Vacuum(ctx context.Context) (MaintainResult, error) {
	var res MaintainResult
	start := time.Now()
	err := db.withMaintenanceLock(ctx, func() error {
		before, err := db.MaintenanceStatsSnapshot()
		if err != nil {
			return err
		}
		res.Action = "vacuum"
		res.Before = before

		if !before.VacuumSpaceOK {
			return fmt.Errorf("%w (可用 %.1f MiB，安全阈值需 >= 2x 数据库大小)",
				ErrInsufficientSpace, float64(before.DiskAvailBytes)/(1024*1024))
		}

		if _, err := db.ExecContext(ctx, `VACUUM`); err != nil {
			return fmt.Errorf("sqlite VACUUM: %w", err)
		}
		var busy, logFrames, checkpointed int
		if err := db.QueryRowContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &logFrames, &checkpointed); err != nil {
			return fmt.Errorf("post-vacuum wal_checkpoint(TRUNCATE): %w", err)
		}
		res.Busy = busy
		res.Partial = busy != 0
		res.LogFrames = logFrames
		res.Checkpoints = checkpointed

		now := time.Now()
		ms := db.maintState()
		ms.mu.Lock()
		ms.lastVacuumAt = now
		ms.lastCheckpointAt = now
		ms.lastMaintainAction = "vacuum"
		ms.mu.Unlock()

		after, err := db.MaintenanceStatsSnapshot()
		if err != nil {
			return err
		}
		res.After = after
		res.Reclaimed = before.TotalBytes - after.TotalBytes
		res.DurationMs = time.Since(start).Milliseconds()
		return nil
	})
	return res, err
}

// EnsurePrivateDir 创建并显式将目录权限修正为 0700（覆盖既有宽松权限）。
func EnsurePrivateDir(dir string) error {
	if dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	return os.Chmod(dir, 0o700)
}

// CleanupStaleTempFilesInLock 在维护锁内清理指定目录中 mtime 早于 olderThan 的 .tmp-* 临时文件
// （olderThan 为零值时清理全部 .tmp-*；传入进程 StartedAt 可避免与刚发起的 HTTP 下载产生竞态）。
func (db *DB) CleanupStaleTempFilesInLock(ctx context.Context, olderThan time.Time, dirs ...string) int {
	removed := 0
	_ = db.withMaintenanceLock(ctx, func() error {
		for _, dir := range dirs {
			removed += CleanupStaleTempFilesBefore(dir, olderThan)
		}
		return nil
	})
	return removed
}

// CleanupStaleTempFiles 清理目录中全部遗留的 .tmp-* 临时文件。
func CleanupStaleTempFiles(dir string) int {
	return CleanupStaleTempFilesBefore(dir, time.Time{})
}

// CleanupStaleTempFilesBefore 清理目录中 mtime 早于 olderThan（或 olderThan 为零值时全部）的 .tmp-* 文件。
func CleanupStaleTempFilesBefore(dir string, olderThan time.Time) int {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	removed := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if !strings.HasPrefix(e.Name(), ".tmp-") {
			continue
		}
		full := filepath.Join(dir, e.Name())
		if !olderThan.IsZero() {
			if fi, err := e.Info(); err == nil && !fi.ModTime().Before(olderThan) {
				continue
			}
		}
		if err := os.Remove(full); err == nil {
			removed++
		}
	}
	return removed
}

// CreateConsistentBackup 在全局维护锁内通过 SQLite 原生 VACUUM INTO 创建单文件一致性快照：
// 1. 预检磁盘剩余空间（自动备份检查最低剩余水位与 2x~3x 峰值需求；超自动备份总量软上限先清理旧自动备份）；
// 2. VACUUM INTO 写入临时文件，并剔除可重建的大体积目录缓存表（catalogs/servers）再 VACUUM 瘦身；
// 3. 若 sanitize=true，使用 journal_mode=DELETE 单连接按白名单执行强脱敏并重新 VACUUM + 断言无残余 WAL 与 freelist=0；
// 4. 以 mode=ro&immutable=1 只读执行 PRAGMA quick_check 验证完整性，绝不改写文件头；
// 5. chmod 0600 + fsync + SHA-256 + 原子 rename + 目录 fsync；
// 6. 自动备份按 AutoBackupPrefix 清理超出 keepAuto 份或超总量预算的旧自动备份，绝不删手动备份。
func (db *DB) CreateConsistentBackup(ctx context.Context, backupDir, kind string, keepAuto int, sanitize bool) (BackupInfo, error) {
	var info BackupInfo
	if kind != "auto" && kind != "manual" {
		kind = "manual"
	}
	if keepAuto <= 0 {
		keepAuto = DefaultAutoBackupKeep
	}
	err := db.withMaintenanceLock(ctx, func() error {
		if err := EnsurePrivateDir(backupDir); err != nil {
			return fmt.Errorf("mkdir backup dir: %w", err)
		}
		st, err := db.MaintenanceStatsSnapshot()
		if err != nil {
			return err
		}
		if avail, ok := availableDiskBytesFn(backupDir); ok {
			// 峰值写入需求 + 完成后仍必须保底的磁盘安全余量（peak + floor），彻底防止大库把 /data 写满导致抢购队列写盘失败。
			mult := uint64(2)
			if sanitize {
				mult = 3
			}
			peak := uint64(st.DBBytes+st.WALBytes)*mult + (64 << 20)
			floor := ManualDiskFreeFloorBytes
			if kind == "auto" {
				floor = MinDiskFreeFloorBytes
			}
			need := peak + floor
			if avail < need {
				return fmt.Errorf("%w (可用 %.1f MiB，安全备份需 >= %.1f MiB 含 %.0f MiB 余量底线)",
					ErrInsufficientSpace, float64(avail)/(1024*1024), float64(need)/(1024*1024), float64(floor)/(1024*1024))
			}
		}

		var rnd [6]byte
		_, _ = rand.Read(rnd[:])
		suffix := hex.EncodeToString(rnd[:])
		now := time.Now().UTC()
		prefix := ManualBackupPrefix
		if kind == "auto" {
			prefix = AutoBackupPrefix
		}
		redactTag := ""
		if sanitize {
			redactTag = "-redacted"
		}
		finalName := fmt.Sprintf("%s%s-%s%s.db", prefix, now.Format("20060102-150405"), suffix[:6], redactTag)
		tmpPath := filepath.Join(backupDir, ".tmp-"+suffix+".db")
		finalPath := filepath.Join(backupDir, finalName)

		cleanupTmp := func() {
			_ = os.Remove(tmpPath)
			_ = os.Remove(tmpPath + "-wal")
			_ = os.Remove(tmpPath + "-shm")
			_ = os.Remove(tmpPath + "-journal")
		}
		defer cleanupTmp()

		if _, err := db.ExecContext(ctx, `VACUUM INTO ?`, tmpPath); err != nil {
			return fmt.Errorf("VACUUM INTO: %w", err)
		}

		// 在独立离线副本上剔除可自动从 OVH 重建的大体积目录缓存（catalogs/servers），避免 14 份备份无谓放大磁盘占用；
		// 同时强制 DELETE 日志模式并 VACUUM 压缩，确保主文件单文件自洽且无残留 -wal。
		if err := finalizeBackupCopy(ctx, tmpPath, sanitize); err != nil {
			return fmt.Errorf("finalize backup copy: %w", err)
		}

		if err := quickCheckBackupFile(ctx, tmpPath); err != nil {
			return fmt.Errorf("backup quick_check failed: %w", err)
		}

		if err := os.Chmod(tmpPath, 0o600); err != nil {
			return fmt.Errorf("chmod backup: %w", err)
		}

		shaHex, sizeBytes, err := fsyncAndHashFile(tmpPath)
		if err != nil {
			return fmt.Errorf("fsync/hash backup: %w", err)
		}

		if err := os.Rename(tmpPath, finalPath); err != nil {
			return fmt.Errorf("rename backup: %w", err)
		}
		syncDirBestEffort(backupDir)

		credsMode := "encrypted"
		if sanitize {
			credsMode = "redacted"
		}
		info = BackupInfo{
			ID:          strings.TrimSuffix(finalName, ".db"),
			Filename:    finalName,
			Kind:        kind,
			SizeBytes:   sizeBytes,
			SHA256:      shaHex,
			CreatedAt:   now.Format(time.RFC3339),
			Credentials: credsMode,
		}

		if kind == "auto" {
			_ = pruneAutoBackupsLocked(backupDir, keepAuto, MaxAutoBackupTotalBytes)
		}
		return nil
	})
	return info, err
}

// redactTablePolicy 描述每张已知表在脱敏副本中的列级策略（默认 fail-closed：未知表或未知列一律拒绝导出）：
// - clearTable=true：整表清空（如 app_devices、app_pairing_codes、telegram_order_buttons、telegram_updates、catalogs）；
// - clearCols：显式清为空字符串/零值的敏感列（如 ovh_accounts 凭据/代理、history.order_url 含免登 orderPassword）；
// - keepCols：显式审查过、允许原样保留的非敏感业务列；
// 任何不在 keepCols 也不在 clearCols 的新列（例如未来迁移新增的敏感列）都会触发 fail-closed 报错，倒逼新列显式评审！
type redactTablePolicy struct {
	clearTable bool
	keepCols   map[string]bool
	clearCols  map[string]bool
}

var redactSchemaPolicies = map[string]redactTablePolicy{
	"kv": {
		keepCols: map[string]bool{"key": true, "value": true},
	},
	"ovh_accounts": {
		keepCols: map[string]bool{
			"id": true, "name": true, "endpoint": true, "zone": true,
			"iam": true, "is_default": true, "created_at": true, "fingerprint": true,
		},
		clearCols: map[string]bool{
			"app_key": true, "app_secret": true, "consumer_key": true, "proxy_url": true,
		},
	},
	"history": {
		keepCols: map[string]bool{
			"id": true, "task_id": true, "plan_code": true, "datacenter": true, "options": true,
			"status": true, "order_id": true, "error_message": true, "purchase_time": true,
			"attempt_count": true, "expiration_time": true, "retraction_time": true, "price": true,
			"account_id": true, "order_status": true, "order_status_at": true, "timing": true,
			"total_ms": true, "detection_to_write_us": true, "prewarmed": true,
			"connection_reused": true, "wait_ms": true, "coupon_warning": true, "duration": true,
		},
		// order_url 常带 OVH 免登 orderPassword 查询参数，脱敏版必须清除
		clearCols: map[string]bool{
			"order_url": true,
		},
	},
	"queue": {
		keepCols: map[string]bool{
			"id": true, "plan_code": true, "datacenter": true, "options": true, "status": true,
			"created_at": true, "updated_at": true, "retry_interval": true, "retry_count": true,
			"failure_count": true, "max_retries": true, "last_check_time": true, "quick_order": true,
			"priority": true, "from_telegram": true, "config_sniper_task_id": true, "account_id": true,
			"auto_pay": true, "preorder": true, "preorder_wait_stock": true, "coupon": true,
			"order_group": true, "group_limit": true, "stop_reason": true, "duration": true,
		},
	},
	"queue_order_groups": {
		keepCols: map[string]bool{"id": true, "max_orders": true},
	},
	"queue_order_claims": {
		keepCols: map[string]bool{"task_id": true, "group_id": true, "state": true},
	},
	"servers": {
		keepCols: map[string]bool{"plan_code": true, "data": true, "updated_at": true},
	},
	"monitor_subscriptions": {
		keepCols: map[string]bool{
			"plan_code": true, "datacenters": true, "notify_available": true, "notify_unavailable": true,
			"last_status": true, "created_at": true, "history": true, "server_name": true,
			"auto_order": true, "quantity": true, "auto_order_account_id": true, "auto_pay": true,
			"paused": true, "options": true, "duration": true,
		},
	},
	"vps_subscriptions": {
		keepCols: map[string]bool{
			"id": true, "plan_code": true, "ovh_subsidiary": true, "datacenters": true,
			"monitor_linux": true, "monitor_windows": true, "notify_available": true,
			"notify_unavailable": true, "last_status": true, "history": true, "created_at": true,
			"auto_order": true, "quantity": true, "auto_pay": true, "os": true,
			"auto_order_account_id": true, "duration": true,
		},
	},
	"server_aliases": {
		keepCols: map[string]bool{"account_id": true, "service_name": true, "alias": true, "updated_at": true},
	},
	"catalogs":               {clearTable: true},
	"telegram_order_buttons": {clearTable: true},
	"telegram_updates":       {clearTable: true},
	"app_devices":            {clearTable: true},
	"app_pairing_codes":      {clearTable: true},
	"config_sniper_tasks":    {clearTable: true},
}

// allowedRedactedKVKeys 是脱敏导出时允许保留的真实业务 kv 键白名单；其余全部 kv 键（含 config、未来密钥键）一律清除。
var allowedRedactedKVKeys = map[string]bool{
	"monitor_interval":         true,
	"monitor_stopped":          true,
	"vps_check_interval":       true,
	"vps_monitor_stopped":      true,
	"update_last_notified_tag": true,
}

func tableExists(ctx context.Context, rawDB *sql.DB, table string) (bool, error) {
	var cnt int
	err := rawDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&cnt)
	return cnt > 0, err
}

func listTableColumns(ctx context.Context, rawDB *sql.DB, table string) ([]string, error) {
	rows, err := rawDB.QueryContext(ctx, fmt.Sprintf(`PRAGMA table_info("%s")`, table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}
		cols = append(cols, name)
	}
	return cols, rows.Err()
}

// finalizeBackupCopy 以单连接 + journal_mode=DELETE 打开离线备份副本：
// - 清理可自动重建的大体积 catalogs 目录缓存（保留小体积 servers 表避免恢复后首轮被当成全新上架刷屏）；
// - 若 sanitize=true，执行表级 + 列级双重白名单校验（任何未知表或未知列直接 fail-closed 拒绝），清空一切凭据/order_url/设备与非白名单 kv；
// - 执行 VACUUM 并断言 journal_mode=delete、freelist_count=0 且无残余 -wal/-journal 文件。
func finalizeBackupCopy(ctx context.Context, path string, sanitize bool) error {
	rawDB, err := sql.Open(driverName, offlineReadWriteDSN(path))
	if err != nil {
		return err
	}
	rawDB.SetMaxOpenConns(1)
	rawDB.SetMaxIdleConns(1)

	closeAndVerify := func() error {
		closeErr := rawDB.Close()
		if fi, statErr := os.Stat(path + "-wal"); statErr == nil && fi.Size() > 0 {
			return fmt.Errorf("backup copy still has non-empty -wal file (%d bytes)", fi.Size())
		}
		_ = os.Remove(path + "-wal")
		_ = os.Remove(path + "-shm")
		_ = os.Remove(path + "-journal")
		return closeErr
	}

	var jm string
	if err := rawDB.QueryRowContext(ctx, `PRAGMA journal_mode=DELETE`).Scan(&jm); err != nil {
		_ = rawDB.Close()
		return fmt.Errorf("set journal_mode=DELETE: %w", err)
	}
	if !strings.EqualFold(strings.TrimSpace(jm), "delete") {
		_ = rawDB.Close()
		return fmt.Errorf("expected journal_mode=delete, got %q", jm)
	}

	// 仅剥离可由 OVH 接口重建的 10MB+ 级 catalogs 原始目录大块，保留小体积 servers 列表避免恢复后误触发“新上架”通知
	if ok, err := tableExists(ctx, rawDB, "catalogs"); err != nil {
		_ = rawDB.Close()
		return err
	} else if ok {
		if _, err := rawDB.ExecContext(ctx, `DELETE FROM "catalogs"`); err != nil {
			_ = rawDB.Close()
			return fmt.Errorf("clear cache table catalogs: %w", err)
		}
	}

	if sanitize {
		rows, err := rawDB.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`)
		if err != nil {
			_ = rawDB.Close()
			return err
		}
		var tables []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				_ = rows.Close()
				_ = rawDB.Close()
				return err
			}
			tables = append(tables, name)
		}
		_ = rows.Close()

		for _, tbl := range tables {
			pol, ok := redactSchemaPolicies[tbl]
			if !ok {
				_ = rawDB.Close()
				return fmt.Errorf("refusing redacted backup: unknown table %q not covered by sanitization allowlist", tbl)
			}
			if pol.clearTable {
				if _, err := rawDB.ExecContext(ctx, fmt.Sprintf(`DELETE FROM "%s"`, tbl)); err != nil {
					_ = rawDB.Close()
					return fmt.Errorf("clear %s: %w", tbl, err)
				}
				continue
			}

			// 列级白名单严格审查：每列必须显式归类到 keepCols 或 clearCols，任何未审查新列一律 fail-closed 拒绝！
			cols, err := listTableColumns(ctx, rawDB, tbl)
			if err != nil {
				_ = rawDB.Close()
				return fmt.Errorf("inspect columns of %s: %w", tbl, err)
			}
			var clearSets []string
			for _, col := range cols {
				low := strings.ToLower(col)
				if pol.clearCols[low] {
					clearSets = append(clearSets, fmt.Sprintf(`"%s"=''`, col))
					continue
				}
				if !pol.keepCols[low] {
					_ = rawDB.Close()
					return fmt.Errorf("refusing redacted backup: unreviewed column %s.%s not covered by allowlist", tbl, col)
				}
			}
			if len(clearSets) > 0 {
				if _, err := rawDB.ExecContext(ctx, fmt.Sprintf(`UPDATE "%s" SET %s`, tbl, strings.Join(clearSets, ", "))); err != nil {
					_ = rawDB.Close()
					return fmt.Errorf("redact %s: %w", tbl, err)
				}
			}
		}

		// kv 表采用白名单保留：删除一切不在 allowedRedactedKVKeys 中的键
		if ok, _ := tableExists(ctx, rawDB, "kv"); ok {
			placeholders := make([]string, 0, len(allowedRedactedKVKeys))
			args := make([]interface{}, 0, len(allowedRedactedKVKeys))
			for k := range allowedRedactedKVKeys {
				placeholders = append(placeholders, "?")
				args = append(args, k)
			}
			q := fmt.Sprintf(`DELETE FROM kv WHERE key NOT IN (%s)`, strings.Join(placeholders, ","))
			if _, err := rawDB.ExecContext(ctx, q, args...); err != nil {
				_ = rawDB.Close()
				return fmt.Errorf("redact kv allowlist: %w", err)
			}
		}
	}

	// 强制重建页布局，彻底擦除 freelist 中可能残留的旧敏感字节
	if _, err := rawDB.ExecContext(ctx, `VACUUM`); err != nil {
		_ = rawDB.Close()
		return fmt.Errorf("vacuum backup copy: %w", err)
	}

	var freelist int64
	if err := rawDB.QueryRowContext(ctx, `PRAGMA freelist_count`).Scan(&freelist); err != nil {
		_ = rawDB.Close()
		return fmt.Errorf("check freelist_count: %w", err)
	}
	if freelist != 0 {
		_ = rawDB.Close()
		return fmt.Errorf("expected freelist_count=0 after VACUUM, got %d", freelist)
	}

	var busy, logFrames, checkpointed int
	if err := rawDB.QueryRowContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &logFrames, &checkpointed); err == nil {
		if busy != 0 {
			_ = rawDB.Close()
			return fmt.Errorf("backup copy wal_checkpoint busy=%d", busy)
		}
	}

	return closeAndVerify()
}

func quickCheckBackupFile(ctx context.Context, path string) error {
	rawDB, err := sql.Open(driverName, offlineReadOnlyDSN(path))
	if err != nil {
		return err
	}
	rawDB.SetMaxOpenConns(1)
	defer rawDB.Close()
	var res string
	if err := rawDB.QueryRowContext(ctx, `PRAGMA quick_check`).Scan(&res); err != nil {
		return err
	}
	if strings.ToLower(strings.TrimSpace(res)) != "ok" {
		return fmt.Errorf("unexpected quick_check result: %s", res)
	}
	return nil
}

func fsyncAndHashFile(path string) (string, int64, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		return "", 0, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return "", 0, err
	}
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

func syncDirBestEffort(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = d.Sync()
	_ = d.Close()
}

// pruneAutoBackupsLocked 仅清理 AutoBackupPrefix 开头的旧自动备份：
// 同时满足数量不超过 keepAuto 且自动备份总占用不超过 maxTotalBytes（至少保留最新 1 份），绝不触碰手动备份。
func pruneAutoBackupsLocked(backupDir string, keepAuto int, maxTotalBytes int64) error {
	if keepAuto <= 0 {
		keepAuto = DefaultAutoBackupKeep
	}
	entries, err := os.ReadDir(backupDir)
	if err != nil {
		return err
	}
	type item struct {
		name    string
		size    int64
		modTime time.Time
	}
	var autos []item
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, AutoBackupPrefix) || !IsValidBackupFilename(name) {
			continue
		}
		fi, err := e.Info()
		if err != nil || !fi.Mode().IsRegular() {
			continue
		}
		autos = append(autos, item{name: name, size: fi.Size(), modTime: fi.ModTime()})
	}
	sort.Slice(autos, func(i, j int) bool {
		if !autos[i].modTime.Equal(autos[j].modTime) {
			return autos[i].modTime.After(autos[j].modTime)
		}
		return autos[i].name > autos[j].name
	})

	var total int64
	exceeded := false
	for idx, it := range autos {
		if exceeded || idx >= keepAuto {
			_ = os.Remove(filepath.Join(backupDir, it.name))
			continue
		}
		if maxTotalBytes > 0 && idx > 0 && total+it.size > maxTotalBytes {
			// 一旦触及总量上限，按时间顺序截断当前及更旧的全部自动备份，保持严格连续的时间窗口
			exceeded = true
			_ = os.Remove(filepath.Join(backupDir, it.name))
			continue
		}
		total += it.size
	}
	return nil
}

// ListBackups 列出备份目录中所有受管理的自动与手动备份（按时间倒序）。
func ListBackups(backupDir string) ([]BackupInfo, error) {
	entries, err := os.ReadDir(backupDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []BackupInfo{}, nil
		}
		return nil, err
	}
	type sortable struct {
		info    BackupInfo
		modTime time.Time
	}
	var out []sortable
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !IsValidBackupFilename(name) {
			continue
		}
		full := filepath.Join(backupDir, name)
		fi, err := os.Lstat(full)
		if err != nil || !fi.Mode().IsRegular() {
			continue
		}
		kind := "manual"
		if strings.HasPrefix(name, AutoBackupPrefix) {
			kind = "auto"
		}
		credsMode := "encrypted"
		if strings.HasSuffix(name, "-redacted.db") {
			credsMode = "redacted"
		}
		out = append(out, sortable{
			info: BackupInfo{
				ID:          strings.TrimSuffix(name, ".db"),
				Filename:    name,
				Kind:        kind,
				SizeBytes:   fi.Size(),
				CreatedAt:   fi.ModTime().UTC().Format(time.RFC3339),
				Credentials: credsMode,
			},
			modTime: fi.ModTime(),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].modTime.Equal(out[j].modTime) {
			return out[i].modTime.After(out[j].modTime)
		}
		return out[i].info.Filename > out[j].info.Filename
	})
	res := make([]BackupInfo, len(out))
	for i := range out {
		res[i] = out[i].info
	}
	return res, nil
}

// IsValidBackupFilename 仅允许严格匹配命名规范的受管备份文件名，彻底杜绝路径穿越与响应头注入。
func IsValidBackupFilename(name string) bool {
	return strictBackupFilenameRe.MatchString(name)
}

// CreateRedactedDownloadTemp 在并发上限 1 的信号量（支持 ctx 等待，避免并发下载误耗作废票据）与 peak+floor 磁盘余量预检下，
// 对已落盘备份生成独立脱敏副本，由调用方负责 defer 移除返回路径。若源文件本身已是 redacted，直接返回原路径且 tempCreated=false。
func CreateRedactedDownloadTemp(ctx context.Context, srcPath, tmpDir string) (outPath string, tempCreated bool, err error) {
	if strings.HasSuffix(srcPath, "-redacted.db") {
		return srcPath, false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	select {
	case dlRedactSem <- struct{}{}:
		defer func() { <-dlRedactSem }()
	case <-waitCtx.Done():
		return "", false, ErrMaintenanceBusy
	}

	if err := EnsurePrivateDir(tmpDir); err != nil {
		return "", false, err
	}
	srcInfo, err := os.Stat(srcPath)
	if err != nil {
		return "", false, err
	}
	if avail, ok := availableDiskBytesFn(tmpDir); ok {
		peak := uint64(srcInfo.Size())*2 + (64 << 20)
		need := peak + ManualDiskFreeFloorBytes
		if avail < need {
			return "", false, fmt.Errorf("%w (可用 %.1f MiB，生成脱敏副本需 >= %.1f MiB)",
				ErrInsufficientSpace, float64(avail)/(1024*1024), float64(need)/(1024*1024))
		}
	}

	var rnd [6]byte
	_, _ = rand.Read(rnd[:])
	tmpPath := filepath.Join(tmpDir, ".tmp-dl-"+hex.EncodeToString(rnd[:])+".db")
	cleanup := func() {
		_ = os.Remove(tmpPath)
		_ = os.Remove(tmpPath + "-wal")
		_ = os.Remove(tmpPath + "-shm")
		_ = os.Remove(tmpPath + "-journal")
	}

	in, err := os.Open(srcPath)
	if err != nil {
		return "", false, err
	}
	out, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		_ = in.Close()
		return "", false, err
	}
	_, copyErr := io.Copy(out, in)
	closeInErr := in.Close()
	closeOutErr := out.Close()
	if copyErr != nil || closeInErr != nil || closeOutErr != nil {
		cleanup()
		if copyErr != nil {
			return "", false, copyErr
		}
		if closeOutErr != nil {
			return "", false, closeOutErr
		}
		return "", false, closeInErr
	}

	if err := finalizeBackupCopy(ctx, tmpPath, true); err != nil {
		cleanup()
		return "", false, err
	}
	if err := quickCheckBackupFile(ctx, tmpPath); err != nil {
		cleanup()
		return "", false, err
	}
	return tmpPath, true, nil
}

// ResolveBackupPathByID 通过不透明 backupID 安全定位真实备份文件路径，严格拒绝路径穿越与符号链接。
func ResolveBackupPathByID(backupDir, backupID string) (string, string, error) {
	id := strings.TrimSpace(backupID)
	if id == "" || strings.Contains(id, "..") || strings.ContainsAny(id, `/\`) {
		return "", "", os.ErrNotExist
	}
	filename := id
	if !strings.HasSuffix(filename, ".db") {
		filename += ".db"
	}
	if !IsValidBackupFilename(filename) {
		return "", "", os.ErrNotExist
	}
	absDir, err := filepath.Abs(backupDir)
	if err != nil {
		return "", "", err
	}
	evalDir, err := filepath.EvalSymlinks(absDir)
	if err != nil {
		return "", "", os.ErrNotExist
	}
	candidate := filepath.Join(evalDir, filename)
	fi, err := os.Lstat(candidate)
	if err != nil || !fi.Mode().IsRegular() {
		return "", "", os.ErrNotExist
	}
	evalPath, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return "", "", os.ErrNotExist
	}
	rel, err := filepath.Rel(evalDir, evalPath)
	if err != nil || rel == "." || strings.HasPrefix(rel, "..") || strings.ContainsAny(rel, `/\`) {
		return "", "", os.ErrNotExist
	}
	return evalPath, filename, nil
}
