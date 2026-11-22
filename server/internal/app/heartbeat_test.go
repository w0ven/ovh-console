package app

import (
	"strings"
	"testing"
	"time"
)

func TestHeartbeatRegistryLifecycleStalenessEpochAndSanitize(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	reg := NewHeartbeatRegistry()
	reg.nowFn = func() time.Time { return now }

	reg.RegisterIfAbsent("server_monitor", 30, false)
	ep1 := reg.Register("update_check", 300, true)

	// RegisterIfAbsent 绝不能覆盖已经在运行的循环
	reg.RegisterIfAbsent("update_check", 10, false)

	snap := reg.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("expected 2 heartbeats, got %d", len(snap))
	}
	if snap[0].Name != "server_monitor" || snap[0].Running || snap[0].Healthy {
		t.Fatalf("unexpected stopped loop state: %+v", snap[0])
	}
	if snap[1].Name != "update_check" || !snap[1].Running || snap[1].Stale || snap[1].IntervalSec != 300 {
		t.Fatalf("newly started loop should keep running & interval 300: %+v", snap[1])
	}

	// 正常心跳上报
	now = now.Add(10 * time.Second)
	reg.BeatOK("update_check", 300)
	snap = reg.Snapshot()
	if !snap[1].Healthy || snap[1].Stale || snap[1].BeatCount != 1 {
		t.Fatalf("expected healthy beat: %+v", snap[1])
	}

	// 超过 3*300 = 900s 未上报 → stale=true, healthy=false
	now = now.Add(950 * time.Second)
	snap = reg.Snapshot()
	if !snap[1].Stale || snap[1].Healthy {
		t.Fatalf("expected stale loop after 950s: %+v", snap[1])
	}

	// 上报错误时必须自动脱敏 Bot Token / URL 账密 / query 参数
	reg.BeatError("update_check", 300, `Get "https://user:pass@api.telegram.org/bot123456:ABC-def_999/getUpdates?offset=12&token=xyz": dial tcp timeout`)
	snap = reg.Snapshot()
	if snap[1].Healthy {
		t.Fatalf("expected unhealthy after error: %+v", snap[1])
	}
	if strings.Contains(snap[1].LastError, "123456:ABC-def_999") || strings.Contains(snap[1].LastError, "user:pass") || strings.Contains(snap[1].LastError, "token=xyz") {
		t.Fatalf("sensitive material leaked in heartbeat lastError: %q", snap[1].LastError)
	}

	// 代际切换重叠：新代 ep2 启动后，旧代 ep1 退出调用 SetStoppedIf 不得把 ep2 覆盖成 stopped
	ep2 := reg.Register("update_check", 300, true)
	reg.SetStoppedIf("update_check", ep1)
	snap = reg.Snapshot()
	if !snap[1].Running {
		t.Fatalf("stale epoch SetStoppedIf must not overwrite newer epoch running state: %+v", snap[1])
	}

	// 停止后再重启：必须清空旧 lastBeatAt 与 lastError，不能刚重启就误报 stale
	reg.SetStoppedIf("update_check", ep2)
	now = now.Add(2 * time.Hour)
	snap = reg.Snapshot()
	if snap[1].Running || snap[1].Stale {
		t.Fatalf("stopped loop should not be running or stale: %+v", snap[1])
	}
	ep3 := reg.Register("update_check", 300, true)
	snap = reg.Snapshot()
	if !snap[1].Running || snap[1].Stale || snap[1].LastError != "" || snap[1].LastBeatAt != "" {
		t.Fatalf("restarted loop should reset stale/lastBeat/lastError: %+v", snap[1])
	}
	reg.SetStoppedIf("update_check", ep3)
}
