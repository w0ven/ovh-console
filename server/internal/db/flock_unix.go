//go:build linux || darwin

package db

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

type fileLockHandle struct {
	file *os.File
}

func acquireMaintenanceFileLock(dataDir string) (*fileLockHandle, error) {
	if dataDir == "" {
		return nil, nil
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, fmt.Errorf("create lock dir: %w", err)
	}
	lockPath := filepath.Join(dataDir, ".maintenance.lock")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open maintenance lock %s: %w", lockPath, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("%w: %v", ErrMaintenanceBusy, err)
	}
	return &fileLockHandle{file: f}, nil
}

func (h *fileLockHandle) Release() {
	if h == nil || h.file == nil {
		return
	}
	_ = syscall.Flock(int(h.file.Fd()), syscall.LOCK_UN)
	_ = h.file.Close()
	h.file = nil
}

func availableDiskBytes(dir string) (uint64, bool) {
	if dir == "" {
		return 0, false
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, false
	}
	if st.Bsize <= 0 {
		return 0, false
	}
	return uint64(st.Bavail) * uint64(st.Bsize), true
}
