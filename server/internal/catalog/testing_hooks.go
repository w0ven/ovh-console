package catalog

// ResetCachesForTesting 排空在途刷新并清空包级公开目录与大区探测缓存(仅供跨包测试隔离使用)。
func ResetCachesForTesting() {
	regionCacheMu.Lock()
	inflight := make([]<-chan struct{}, 0, len(regionCacheCall))
	for _, c := range regionCacheCall {
		inflight = append(inflight, c.done)
	}
	regionCacheMu.Unlock()
	for _, done := range inflight {
		<-done
	}
	regionCacheMu.Lock()
	regionCache = map[string]*subsidiaryCatalog{}
	regionCacheFail = map[string]catalogFailure{}
	regionCacheCall = map[string]*catalogCall{}
	regionCacheMu.Unlock()

	availProbeMu.Lock()
	probeInflight := make([]<-chan struct{}, 0, len(availProbeCalls))
	for _, c := range availProbeCalls {
		probeInflight = append(probeInflight, c.done)
	}
	availProbeMu.Unlock()
	for _, done := range probeInflight {
		<-done
	}
	availProbeMu.Lock()
	availProbeCache = map[string]availProbeEntry{}
	availProbeFail = map[string]availProbeFailure{}
	availProbeCalls = map[string]*availProbeCall{}
	availProbeMu.Unlock()
}
