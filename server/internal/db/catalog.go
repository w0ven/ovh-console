package db

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"
)

func catalogSHA256(data string) string {
	sum := sha256.Sum256([]byte(data))
	return hex.EncodeToString(sum[:])
}

// GetCatalog 按 subsidiary 取 catalog JSON 原文 + 更新时间（Unix ms）。
// 没记录时 ok=false。
func (db *DB) GetCatalog(subsidiary string) (raw string, updatedAtMs int64, ok bool, err error) {
	row := struct {
		Data      string `db:"data"`
		UpdatedAt int64  `db:"updated_at"`
	}{}
	err = db.Get(&row, `SELECT data, updated_at FROM catalogs WHERE subsidiary = ?`, subsidiary)
	if err == sql.ErrNoRows {
		return "", 0, false, nil
	}
	if err != nil {
		return "", 0, false, fmt.Errorf("catalog get %s: %w", subsidiary, err)
	}
	return row.Data, row.UpdatedAt, true, nil
}

// UpsertCatalog 按 subsidiary upsert catalog JSON 原文，updated_at = now ms。
// 写放大治理：先比对 SHA-256 内容哈希，内容未变时仅更新极小的 updated_at 列，
// 避免每 2 小时整行重写 ~12MB JSON 导致 SQLite 持续产生海量空闲页和 WAL 膨胀。
func (db *DB) UpsertCatalog(subsidiary, data string) error {
	nowMs := time.Now().UnixMilli()
	newHash := catalogSHA256(data)

	var existingHash string
	err := db.Get(&existingHash, `SELECT content_hash FROM catalogs WHERE subsidiary = ?`, subsidiary)
	if err == nil && existingHash == newHash {
		_, uerr := db.Exec(`UPDATE catalogs SET updated_at = ? WHERE subsidiary = ?`, nowMs, subsidiary)
		if uerr != nil {
			return fmt.Errorf("catalog touch %s: %w", subsidiary, uerr)
		}
		return nil
	}

	_, err = db.Exec(
		`INSERT INTO catalogs(subsidiary, data, updated_at, content_hash) VALUES(?, ?, ?, ?)
		 ON CONFLICT(subsidiary) DO UPDATE SET data=excluded.data, updated_at=excluded.updated_at, content_hash=excluded.content_hash`,
		subsidiary, data, nowMs, newHash,
	)
	if err != nil {
		return fmt.Errorf("catalog upsert %s: %w", subsidiary, err)
	}
	return nil
}

// ClearCatalogs 清空所有 catalog，缓存管理"清除全部"会调
func (db *DB) ClearCatalogs() error {
	_, err := db.Exec(`DELETE FROM catalogs`)
	return err
}
