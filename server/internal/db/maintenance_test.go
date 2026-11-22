package db

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMaintenanceCheckpointVacuumBackupsAndByteScanRedaction(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		d.CleanupMaintState()
		_ = d.Close()
	}()

	// 写入敏感凭据（与 config.Store 实际格式一致）、明文代理账密、设备令牌、TG 按钮、含免登密码的 order_url 与大目录缓存
	if err := d.SetKV("config", map[string]string{
		"appKey":           "ak-plain-123",
		"appSecret":        "enc:secret-app-999",
		"consumerKey":      "ck-plain-456",
		"tgToken":          "123456:SECRET_TG_BOT_TOKEN_XYZ",
		"notifyWebhookUrl": "https://hooks.example.test/webhook/super-secret-path",
	}); err != nil {
		t.Fatalf("SetKV config: %v", err)
	}
	if err := d.SetKV("unexpected_secret_kv", "TOP_SECRET_LEAK_CHECK"); err != nil {
		t.Fatalf("SetKV unexpected_secret_kv: %v", err)
	}
	// 使用真实业务白名单键 monitor_interval
	if err := d.SetKV("monitor_interval", 45); err != nil {
		t.Fatalf("SetKV monitor_interval: %v", err)
	}
	if _, err := d.Exec(`INSERT INTO ovh_accounts (id, name, endpoint, zone, app_key, app_secret, consumer_key, iam, proxy_url, is_default, created_at)
		VALUES ('acc-1', 'EU Main', 'ovh-eu', 'IE', 'AK_ACC_SECRET', 'AS_ACC_SECRET', 'CK_ACC_SECRET', 'ovh-console', 'http://proxyuser:proxypass99@1.2.3.4:8080', 1, ?)`, time.Now().UTC().Format(time.RFC3339)); err != nil {
		t.Fatalf("insert ovh_account: %v", err)
	}
	if _, err := d.Exec(`INSERT INTO history (id, task_id, plan_code, datacenter, options, status, order_id, order_url, purchase_time)
		VALUES ('h-1', 't-1', '24sk20', 'gra', '[]', 'success', '998877', 'https://www.ovh.com/cgi-bin/order/displayOrder.cgi?orderId=998877&orderPassword=ORDPASS_SECRET_777', ?)`, time.Now().UTC().Format(time.RFC3339)); err != nil {
		t.Fatalf("insert history: %v", err)
	}
	if _, err := d.Exec(`INSERT INTO app_devices (name, token_hash, created_at) VALUES ('Phone', 'DEV_HASH_SECRET_XYZ', ?)`, time.Now().UTC().Format(time.RFC3339)); err != nil {
		t.Fatalf("insert device: %v", err)
	}
	if _, err := d.Exec(`INSERT INTO telegram_order_buttons (id, plan_code, datacenter, options, config_info, account_id, created_at, used_at)
		VALUES ('btn-1', '24sk20', 'gra', '[]', '{}', 'acc-1', 1, 0)`); err != nil {
		t.Fatalf("insert tg button: %v", err)
	}
	// 塞入一份大体积 catalog 缓存验证备份时会被自动剥离
	if _, err := d.Exec(`INSERT INTO catalogs (subsidiary, data, updated_at) VALUES ('IE', ?, ?)`, strings.Repeat("X", 256*1024), time.Now().UnixMilli()); err != nil {
		t.Fatalf("insert catalog: %v", err)
	}

	// 1. WAL Checkpoint
	cpRes, err := d.CheckpointWAL(context.Background())
	if err != nil {
		t.Fatalf("CheckpointWAL: %v", err)
	}
	if cpRes.Action != "checkpoint" || cpRes.After.DBBytes <= 0 {
		t.Fatalf("unexpected checkpoint result: %+v", cpRes)
	}

	// 2. VACUUM
	vacRes, err := d.Vacuum(context.Background())
	if err != nil {
		t.Fatalf("Vacuum: %v", err)
	}
	if vacRes.Action != "vacuum" || vacRes.After.FreelistPages != 0 {
		t.Fatalf("unexpected vacuum result: %+v", vacRes)
	}

	// 3. 启动清理 backups 与 tmp 两个目录下的陈旧 .tmp-* 文件（不误删更新的 .tmp-*）
	backupDir := filepath.Join(dir, "backups")
	tmpDir := filepath.Join(dir, "tmp")
	_ = EnsurePrivateDir(backupDir)
	_ = EnsurePrivateDir(tmpDir)
	oldFile1 := filepath.Join(backupDir, ".tmp-stale.db")
	oldFile2 := filepath.Join(tmpDir, ".tmp-dl-stale.db")
	newFile := filepath.Join(tmpDir, ".tmp-dl-active.db")
	_ = os.WriteFile(oldFile1, []byte("partial"), 0o600)
	_ = os.WriteFile(oldFile2, []byte("partial-dl"), 0o600)
	_ = os.WriteFile(newFile, []byte("active-dl"), 0o600)
	past := time.Now().Add(-10 * time.Minute)
	_ = os.Chtimes(oldFile1, past, past)
	_ = os.Chtimes(oldFile2, past, past)
	if removed := d.CleanupStaleTempFilesInLock(context.Background(), time.Now().Add(-1*time.Minute), backupDir, tmpDir); removed != 2 {
		t.Fatalf("expected 2 stale tmp files removed (leaving active newer file), got %d", removed)
	}
	if _, err := os.Stat(newFile); err != nil {
		t.Fatalf("active temp file newer than StartedAt should not be deleted")
	}
	_ = os.Remove(newFile)

	// 4. 磁盘余量 peak + floor 注入校验：avail = peak + floor - 1 必须返回 ErrInsufficientSpace
	stSnap, _ := d.MaintenanceStatsSnapshot()
	autoPeak := uint64(stSnap.DBBytes+stSnap.WALBytes)*2 + (64 << 20)
	origDiskFn := availableDiskBytesFn
	availableDiskBytesFn = func(string) (uint64, bool) {
		return autoPeak + MinDiskFreeFloorBytes - 1, true
	}
	if _, err := d.CreateConsistentBackup(context.Background(), backupDir, "auto", 2, false); !errors.Is(err, ErrInsufficientSpace) {
		availableDiskBytesFn = origDiskFn
		t.Fatalf("expected ErrInsufficientSpace when avail = peak + floor - 1, got %v", err)
	}
	availableDiskBytesFn = origDiskFn

	// 5. 创建手动备份 + 3 份自动备份（keepAuto=2），验证自动清理只删 auto 不删 manual 且 catalog 缓存已剥离
	manualInfo, err := d.CreateConsistentBackup(context.Background(), backupDir, "manual", 2, false)
	if err != nil {
		t.Fatalf("CreateConsistentBackup manual: %v", err)
	}
	if manualInfo.Kind != "manual" || manualInfo.Credentials != "encrypted" {
		t.Fatalf("unexpected manual backup info: %+v", manualInfo)
	}
	if manualInfo.SizeBytes >= 200*1024 {
		t.Fatalf("expected catalogs cache stripped from backup, got large size=%d", manualInfo.SizeBytes)
	}

	for i := 0; i < 3; i++ {
		if _, err := d.CreateConsistentBackup(context.Background(), backupDir, "auto", 2, false); err != nil {
			t.Fatalf("CreateConsistentBackup auto #%d: %v", i, err)
		}
		time.Sleep(15 * time.Millisecond)
	}
	list, err := ListBackups(backupDir)
	if err != nil {
		t.Fatalf("ListBackups: %v", err)
	}
	autoCount, manualCount := 0, 0
	for _, b := range list {
		if b.Kind == "auto" {
			autoCount++
		} else if b.Kind == "manual" {
			manualCount++
		}
	}
	if autoCount != 2 || manualCount != 1 {
		t.Fatalf("expected 2 auto + 1 manual backup, got auto=%d manual=%d", autoCount, manualCount)
	}

	// 6. 生成脱敏下载副本，并**仅复制主文件到隔离目录**进行只读 SQL 查询 + 原始二进制字节全量扫描！
	realPath, _, err := ResolveBackupPathByID(backupDir, manualInfo.ID)
	if err != nil {
		t.Fatalf("ResolveBackupPathByID: %v", err)
	}
	// 注入检查 CreateRedactedDownloadTemp 的 peak + ManualDiskFreeFloorBytes 水位拒绝
	srcFI, _ := os.Stat(realPath)
	dlPeak := uint64(srcFI.Size())*2 + (64 << 20)
	availableDiskBytesFn = func(string) (uint64, bool) {
		return dlPeak + ManualDiskFreeFloorBytes - 1, true
	}
	if _, _, err := CreateRedactedDownloadTemp(context.Background(), realPath, tmpDir); !errors.Is(err, ErrInsufficientSpace) {
		availableDiskBytesFn = origDiskFn
		t.Fatalf("expected ErrInsufficientSpace for redacted download temp, got %v", err)
	}
	availableDiskBytesFn = origDiskFn

	redactedPath, created, err := CreateRedactedDownloadTemp(context.Background(), realPath, tmpDir)
	if err != nil || !created {
		t.Fatalf("CreateRedactedDownloadTemp: path=%s created=%v err=%v", redactedPath, created, err)
	}
	defer os.Remove(redactedPath)

	if _, err := os.Stat(redactedPath + "-wal"); err == nil {
		t.Fatalf("redacted backup must not leave -wal file behind")
	}

	isoDir := t.TempDir()
	isoPath := filepath.Join(isoDir, "isolated.db")
	srcF, err := os.Open(redactedPath)
	if err != nil {
		t.Fatalf("open redacted: %v", err)
	}
	dstF, err := os.Create(isoPath)
	if err != nil {
		_ = srcF.Close()
		t.Fatalf("create isolated: %v", err)
	}
	_, err = io.Copy(dstF, srcF)
	_ = srcF.Close()
	_ = dstF.Close()
	if err != nil {
		t.Fatalf("copy isolated: %v", err)
	}

	rawBytes, err := os.ReadFile(isoPath)
	if err != nil {
		t.Fatalf("read isolated bytes: %v", err)
	}
	forbidden := []string{
		"SECRET_TG_BOT_TOKEN_XYZ",
		"super-secret-path",
		"secret-app-999",
		"TOP_SECRET_LEAK_CHECK",
		"AK_ACC_SECRET",
		"AS_ACC_SECRET",
		"CK_ACC_SECRET",
		"proxyuser:proxypass99",
		"DEV_HASH_SECRET_XYZ",
		"ORDPASS_SECRET_777",
		"btn-1",
	}
	for _, needle := range forbidden {
		if bytes.Contains(rawBytes, []byte(needle)) {
			t.Fatalf("redacted backup raw bytes leaked forbidden secret %q", needle)
		}
	}

	rdb, err := sql.Open(driverName, offlineReadOnlyDSN(isoPath))
	if err != nil {
		t.Fatalf("open isolated redacted backup: %v", err)
	}
	defer rdb.Close()

	var monitorIv, orderURL, jm string
	var freelist, cfgCount, btnCount, devCount, catCount int
	_ = rdb.QueryRow(`PRAGMA journal_mode`).Scan(&jm)
	_ = rdb.QueryRow(`PRAGMA freelist_count`).Scan(&freelist)
	_ = rdb.QueryRow(`SELECT value FROM kv WHERE key='monitor_interval'`).Scan(&monitorIv)
	_ = rdb.QueryRow(`SELECT order_url FROM history WHERE id='h-1'`).Scan(&orderURL)
	_ = rdb.QueryRow(`SELECT COUNT(*) FROM kv WHERE key='config'`).Scan(&cfgCount)
	_ = rdb.QueryRow(`SELECT COUNT(*) FROM telegram_order_buttons`).Scan(&btnCount)
	_ = rdb.QueryRow(`SELECT COUNT(*) FROM app_devices`).Scan(&devCount)
	_ = rdb.QueryRow(`SELECT COUNT(*) FROM catalogs`).Scan(&catCount)

	if strings.ToLower(jm) == "wal" {
		t.Fatalf("backup file header should not be WAL mode, got %q", jm)
	}
	if freelist != 0 || cfgCount != 0 || btnCount != 0 || devCount != 0 || catCount != 0 || orderURL != "" {
		t.Fatalf("unexpected state in isolated redacted backup: freelist=%d cfg=%d btn=%d dev=%d cat=%d orderURL=%q",
			freelist, cfgCount, btnCount, devCount, catCount, orderURL)
	}
	if !strings.Contains(monitorIv, "45") {
		t.Fatalf("allowlisted monitor_interval kv lost: %q", monitorIv)
	}

	// 7. 列级 fail-closed：已知表新增未审查列后，脱敏备份必须明确拒绝
	if _, err := d.Exec(`ALTER TABLE ovh_accounts ADD COLUMN future_secret TEXT NOT NULL DEFAULT 'LEAK'`); err != nil {
		t.Fatalf("alter table add unreviewed column: %v", err)
	}
	if _, err := d.CreateConsistentBackup(context.Background(), backupDir, "manual", 2, true); err == nil || !strings.Contains(err.Error(), "unreviewed column") {
		t.Fatalf("expected fail-closed error for unreviewed column, got %v", err)
	}

	// 8. 表级 fail-closed：新增未知表时，脱敏备份必须明确拒绝
	if _, err := d.Exec(`CREATE TABLE unexpected_secret_table (id TEXT PRIMARY KEY, secret TEXT)`); err != nil {
		t.Fatalf("create unknown table: %v", err)
	}
	if _, err := d.CreateConsistentBackup(context.Background(), backupDir, "manual", 2, true); err == nil {
		t.Fatalf("expected fail-closed error when redacting DB with unknown table")
	}

	// 9. 严格文件名白名单与路径穿越拒绝
	for _, bad := range []string{"", "../sniper", "a/b", `a\b`, ".tmp-abc", "sniper-manual-badname", "sniper-manual-20261122-120000-zz\r\n"} {
		if _, _, err := ResolveBackupPathByID(backupDir, bad); err == nil {
			t.Fatalf("expected error for invalid backupID %q", bad)
		}
	}
}
