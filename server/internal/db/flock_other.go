//go:build !(linux || darwin)

package db

type fileLockHandle struct{}

func acquireMaintenanceFileLock(string) (*fileLockHandle, error) {
	return &fileLockHandle{}, nil
}

func (h *fileLockHandle) Release() {}

func availableDiskBytes(string) (uint64, bool) {
	return 0, false
}
