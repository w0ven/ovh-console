package app

import (
	"regexp"
	"strings"
	"sync"
	"time"
)

// LoopHeartbeat 描述单个后台循环的运行态、心跳时间与最近错误。
type LoopHeartbeat struct {
	Name        string `json:"name"`
	Running     bool   `json:"running"`
	IntervalSec int    `json:"intervalSec"`
	StartedAt   string `json:"startedAt,omitempty"`
	LastBeatAt  string `json:"lastBeatAt,omitempty"`
	LastOKAt    string `json:"lastOkAt,omitempty"`
	LastError   string `json:"lastError,omitempty"`
	LastErrorAt string `json:"lastErrorAt,omitempty"`
	BeatCount   int64  `json:"beatCount"`
	Healthy     bool   `json:"healthy"`
	Stale       bool   `json:"stale"`
}

type loopBeatRecord struct {
	name        string
	epoch       uint64
	running     bool
	intervalSec int
	startedAt   time.Time
	lastBeatAt  time.Time
	lastOKAt    time.Time
	lastError   string
	lastErrorAt time.Time
	beatCount   int64
}

// HeartbeatRegistry 线程安全的后台循环心跳注册表，支持代际 epoch 防止旧循环退出覆盖新循环状态。
type HeartbeatRegistry struct {
	mu        sync.RWMutex
	records   map[string]*loopBeatRecord
	order     []string
	nextEpoch uint64
	nowFn     func() time.Time
}

// NewHeartbeatRegistry 创建空的后台循环心跳注册表。
func NewHeartbeatRegistry() *HeartbeatRegistry {
	return &HeartbeatRegistry{
		records: map[string]*loopBeatRecord{},
		nowFn:   time.Now,
	}
}

func (r *HeartbeatRegistry) now() time.Time {
	if r != nil && r.nowFn != nil {
		return r.nowFn()
	}
	return time.Now()
}

var (
	hbBotTokenRe  = regexp.MustCompile(`(?i)(/|^|\s)bot\d+:[A-Za-z0-9_-]+`)
	hbBareTokenRe = regexp.MustCompile(`\b\d{6,}:[A-Za-z0-9_-]{20,}\b`)
	hbURLCredsRe  = regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*://)[^/\s@]+@`)
	hbURLQueryRe  = regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*://[^\s?#]+)\?[^\s"']*`)
	hbMaxErrRunes = 300
)

// SanitizeHeartbeatError 对写入心跳的错误信息进行统一脱敏（清除 Telegram Bot Token、裸 Token、各类协议 URL 账密/query）并截断长度。
func SanitizeHeartbeatError(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return ""
	}
	s = hbBotTokenRe.ReplaceAllString(s, "${1}bot***")
	s = hbBareTokenRe.ReplaceAllString(s, "***:***token***")
	s = hbURLCredsRe.ReplaceAllString(s, "${1}***@")
	s = hbURLQueryRe.ReplaceAllString(s, "${1}?***")
	runes := []rune(s)
	if len(runes) > hbMaxErrRunes {
		s = string(runes[:hbMaxErrRunes]) + "…"
	}
	return s
}

func (r *HeartbeatRegistry) ensureLocked(name string) *loopBeatRecord {
	rec, ok := r.records[name]
	if !ok {
		rec = &loopBeatRecord{name: name}
		r.records[name] = rec
		r.order = append(r.order, name)
	}
	return rec
}

// RegisterIfAbsent 仅在指定循环尚未注册时预建一条记录，绝不覆盖已在运行的循环状态或间隔。
func (r *HeartbeatRegistry) RegisterIfAbsent(name string, intervalSec int, running bool) uint64 {
	if r == nil || name == "" {
		return 0
	}
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	if rec, exists := r.records[name]; exists {
		return rec.epoch
	}
	rec := r.ensureLocked(name)
	r.nextEpoch++
	rec.epoch = r.nextEpoch
	rec.intervalSec = intervalSec
	rec.running = running
	if running {
		rec.startedAt = now
	}
	return rec.epoch
}

// Register 注册或启动一个后台循环，返回唯一代际 epoch 供 SetStoppedIf 使用。
// 当状态从 stopped 切为 running 时，重置 startedAt、清空历史 lastBeatAt 和 lastError，避免刚重启就误报 stale。
func (r *HeartbeatRegistry) Register(name string, intervalSec int, running bool) uint64 {
	if r == nil || name == "" {
		return 0
	}
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	rec := r.ensureLocked(name)
	r.nextEpoch++
	rec.epoch = r.nextEpoch
	if intervalSec > 0 {
		rec.intervalSec = intervalSec
	}
	wasRunning := rec.running
	rec.running = running
	if running && (!wasRunning || rec.startedAt.IsZero()) {
		rec.startedAt = now
		rec.lastBeatAt = time.Time{}
		rec.lastError = ""
		rec.lastErrorAt = time.Time{}
	}
	return rec.epoch
}

// BeatOK 记录指定循环完成一次成功心跳，清空最近错误并确保处于运行态。
func (r *HeartbeatRegistry) BeatOK(name string, intervalSec int) {
	if r == nil || name == "" {
		return
	}
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	rec := r.ensureLocked(name)
	rec.running = true
	if rec.startedAt.IsZero() {
		rec.startedAt = now
	}
	if intervalSec > 0 {
		rec.intervalSec = intervalSec
	}
	rec.lastBeatAt = now
	rec.lastOKAt = now
	rec.lastError = ""
	rec.beatCount++
}

// BeatError 记录指定循环出现一次错误（自动脱敏并截断），更新心跳时间戳。
func (r *HeartbeatRegistry) BeatError(name string, intervalSec int, errMsg string) {
	if r == nil || name == "" {
		return
	}
	now := r.now()
	cleanErr := SanitizeHeartbeatError(errMsg)
	r.mu.Lock()
	defer r.mu.Unlock()
	rec := r.ensureLocked(name)
	rec.running = true
	if rec.startedAt.IsZero() {
		rec.startedAt = now
	}
	if intervalSec > 0 {
		rec.intervalSec = intervalSec
	}
	rec.lastBeatAt = now
	rec.lastError = cleanErr
	rec.lastErrorAt = now
	rec.beatCount++
}

// SetStopped 标记循环已停止（无代际校验，仅推荐单实例循环使用）。
func (r *HeartbeatRegistry) SetStopped(name string) {
	if r == nil || name == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if rec, ok := r.records[name]; ok {
		rec.running = false
	}
}

// SetStoppedIf 仅当当前记录代际仍等于 epoch 时才标记为 stopped，避免旧代循环退出把新代覆盖成已停止。
func (r *HeartbeatRegistry) SetStoppedIf(name string, epoch uint64) {
	if r == nil || name == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if rec, ok := r.records[name]; ok {
		if epoch == 0 || rec.epoch == epoch {
			rec.running = false
		}
	}
}

// Snapshot 返回当前全部已注册后台循环的健康快照。
// 健康口径：若处于运行态且声明了 intervalSec > 0，且最近一次心跳（或启动时间）距离现在超过 max(3*interval, 60s) 视为失联（stale=true, healthy=false）。
func (r *HeartbeatRegistry) Snapshot() []LoopHeartbeat {
	if r == nil {
		return nil
	}
	now := r.now()
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]LoopHeartbeat, 0, len(r.order))
	for _, name := range r.order {
		rec := r.records[name]
		if rec == nil {
			continue
		}
		hb := LoopHeartbeat{
			Name:        rec.name,
			Running:     rec.running,
			IntervalSec: rec.intervalSec,
			LastError:   rec.lastError,
			BeatCount:   rec.beatCount,
			Healthy:     rec.running,
		}
		if !rec.startedAt.IsZero() {
			hb.StartedAt = rec.startedAt.UTC().Format(time.RFC3339)
		}
		if !rec.lastBeatAt.IsZero() {
			hb.LastBeatAt = rec.lastBeatAt.UTC().Format(time.RFC3339)
		}
		if !rec.lastOKAt.IsZero() {
			hb.LastOKAt = rec.lastOKAt.UTC().Format(time.RFC3339)
		}
		if !rec.lastErrorAt.IsZero() {
			hb.LastErrorAt = rec.lastErrorAt.UTC().Format(time.RFC3339)
		}
		if rec.running && rec.intervalSec > 0 {
			ttl := time.Duration(rec.intervalSec) * 3 * time.Second
			if ttl < 60*time.Second {
				ttl = 60 * time.Second
			}
			refTime := rec.lastBeatAt
			if refTime.IsZero() {
				refTime = rec.startedAt
			}
			if !refTime.IsZero() && now.Sub(refTime) > ttl {
				hb.Stale = true
				hb.Healthy = false
			}
		}
		if rec.lastError != "" && (rec.lastOKAt.IsZero() || rec.lastErrorAt.After(rec.lastOKAt)) {
			hb.Healthy = false
		}
		out = append(out, hb)
	}
	return out
}
