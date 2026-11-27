package app

import (
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/ovh-buy/server/internal/config"
	"github.com/ovh-buy/server/internal/db"
	"github.com/ovh-buy/server/internal/logger"
	"github.com/ovh-buy/server/internal/storage"
	"github.com/ovh-buy/server/internal/types"
)

func bundleState(t *testing.T, items []types.QueueItem) *State {
	t.Helper()
	dir := t.TempDir()
	database, err := db.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	lg := logger.New(filepath.Join(dir, "t.log"), slog.New(slog.NewTextHandler(io.Discard, nil)))
	s := NewState(storage.Paths{DataDir: dir}, config.New(database), lg, database)
	s.Queue = items
	if err := s.SaveQueue(); err != nil {
		t.Fatal(err)
	}
	return s
}

func task(id, dc, status string) types.QueueItem {
	return types.QueueItem{ID: id, AccountID: "acc", PlanCode: "plan", Datacenter: dc, Status: status, Options: []string{"old"}, Duration: "P1M", RetryInterval: 60, CreatedAt: "2026-11-27T00:00:0" + id[len(id)-1:] + "Z"}
}

func countBy(s *State) map[string]map[string]int {
	out := map[string]map[string]int{}
	for _, it := range s.Queue {
		if out[it.Datacenter] == nil {
			out[it.Datacenter] = map[string]int{}
		}
		out[it.Datacenter][it.Status]++
	}
	return out
}

func TestBundleEditAppliesConfigToAllAndAdjustsPerDCQuantities(t *testing.T) {
	s := bundleState(t, []types.QueueItem{
		task("a1", "fra", "running"), task("a2", "fra", "paused"), task("a3", "fra", "running"),
		task("b1", "gra", "running"), task("c1", "rbx", "completed"),
	})
	d := "P12M"
	res, err := s.UpdateQueueBundle(BundleUpdate{
		IDs: []string{"a1", "a2", "a3", "b1", "c1"}, Options: []string{"new"}, Duration: &d, RetryInterval: 30, AutoPay: true,
		Quantities: map[string]int{"fra": 1, "gra": 3, "sbg": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Added != 4 || res.Removed != 2 {
		t.Fatalf("unexpected result %+v", res)
	}
	c := countBy(s)
	if c["fra"]["running"]+c["fra"]["paused"] != 1 || c["gra"]["running"] != 3 || c["sbg"]["running"] != 2 {
		t.Fatalf("wrong per-DC counts: %v", c)
	}
	// FRA 多出来的先删暂停的
	for _, it := range s.Queue {
		if it.ID == "a2" {
			t.Fatal("paused task should be removed first")
		}
		if it.Status == "completed" {
			if it.Options[0] != "old" || it.AutoPay {
				t.Fatal("completed task must never be modified")
			}
			continue
		}
		if it.Options[0] != "new" || it.Duration != "P12M" || it.RetryInterval != 30 || !it.AutoPay {
			t.Fatalf("config not applied in place to %s: %+v", it.ID, it)
		}
	}
	if !s.IsTaskDeleted("a2") {
		t.Fatal("removed task must be cancelled")
	}
	// 落库一致
	stored, err := s.DB.ListQueue()
	if err != nil || len(stored) != len(s.Queue) {
		t.Fatalf("queue not persisted: %v %d/%d", err, len(stored), len(s.Queue))
	}
}

func TestBundleEditSharedTotalMode(t *testing.T) {
	mk := func(id, dc, status string) types.QueueItem {
		it := task(id, dc, status)
		it.OrderGroup, it.GroupLimit = "g", 2
		return it
	}
	s := bundleState(t, []types.QueueItem{mk("a1", "fra", "running"), mk("a2", "fra", "running"), mk("b1", "gra", "completed"), mk("b2", "gra", "running")})
	// 合计 3 台(已下单 1)→ 每个机房还要 2 个任务;去掉 GRA,加上 RBX
	res, err := s.UpdateQueueBundle(BundleUpdate{IDs: []string{"a1", "a2", "b1", "b2"}, Options: []string{"old"}, AutoPay: false, Total: 3, Datacenters: []string{"fra", "rbx"}})
	if err != nil {
		t.Fatal(err)
	}
	c := countBy(s)
	if c["fra"]["running"] != 2 || c["rbx"]["running"] != 2 || c["gra"]["running"] != 0 || c["gra"]["completed"] != 1 {
		t.Fatalf("wrong counts %v (%+v)", c, res)
	}
	for _, it := range s.Queue {
		if it.Status != "completed" && (it.GroupLimit != 3 || it.OrderGroup != "g") {
			t.Fatalf("shared limit not applied: %+v", it)
		}
	}
}

func TestBundleEditRejectsMixedAndEmpty(t *testing.T) {
	other := task("x1", "fra", "running")
	other.PlanCode = "other"
	s := bundleState(t, []types.QueueItem{task("a1", "fra", "running"), other})
	if _, err := s.UpdateQueueBundle(BundleUpdate{IDs: []string{"a1", "x1"}}); err != ErrBundleMixed {
		t.Fatalf("mixed bundle accepted: %v", err)
	}
	if _, err := s.UpdateQueueBundle(BundleUpdate{IDs: []string{"a1"}, Quantities: map[string]int{"fra": 0}}); err != ErrBundleNoTarget {
		t.Fatalf("emptying the order accepted: %v", err)
	}
	if len(s.Queue) != 2 || s.Queue[0].Options[0] != "old" {
		t.Fatal("failed edit changed the queue")
	}
}

// 已有结账登记后改「合计几台」:数据库里的上限要同步,否则之后每次结账都会报上限已变。
func TestBundleEditSyncsRegisteredGroupLimit(t *testing.T) {
	mk := func(id, dc, status string) types.QueueItem {
		it := task(id, dc, status)
		it.OrderGroup, it.GroupLimit = "g", 2
		return it
	}
	s := bundleState(t, []types.QueueItem{mk("a1", "fra", "running"), mk("a2", "fra", "running")})
	if ok, err := s.DB.ClaimQueueOrder(s.Queue[0]); err != nil || !ok {
		t.Fatalf("first claim failed: %v", err)
	}
	if _, err := s.UpdateQueueBundle(BundleUpdate{IDs: []string{"a1", "a2"}, Options: []string{"old"}, Total: 4, Datacenters: []string{"fra"}}); err != nil {
		t.Fatal(err)
	}
	var next types.QueueItem
	for _, it := range s.Queue {
		if it.ID != "a1" {
			next = it
			break
		}
	}
	if next.GroupLimit != 4 {
		t.Fatalf("task limit not updated: %+v", next)
	}
	if ok, err := s.DB.ClaimQueueOrder(next); err != nil || !ok {
		t.Fatalf("claim after raising the total failed: %v", err)
	}
}

// B1 回归:缩量时绝不优先删除 checkoutUncertain 或 submitting 任务;若必须删到它们则拒绝修改。
func TestBundleEditProtectsCheckoutUncertainAndSubmittingTasks(t *testing.T) {
	unc := task("u1", "fra", "paused")
	unc.StopReason = StopReasonCheckoutUncertain
	run := task("r1", "fra", "running")
	sub := task("s1", "fra", "running")
	sub.OrderPhase = "submitting"

	s := bundleState(t, []types.QueueItem{unc, run, sub})
	// 3 台缩到 2 台:u1(checkoutUncertain) 与 s1(submitting) 均受保护,只能删 r1
	res, err := s.UpdateQueueBundle(BundleUpdate{
		IDs:        []string{"u1", "r1", "s1"},
		Options:    []string{"new"},
		Quantities: map[string]int{"fra": 2},
	})
	if err != nil || res.Removed != 1 {
		t.Fatalf("expected 1 running task removed while keeping protected ones, got %+v err=%v", res, err)
	}
	for _, it := range s.Queue {
		if it.ID == "r1" {
			t.Fatal("expected unprotected r1 to be removed instead of u1/s1")
		}
		if it.ID == "u1" && it.StopReason != StopReasonCheckoutUncertain {
			t.Fatalf("uncertain stopReason must remain intact: %+v", it)
		}
		if it.ID == "s1" && it.OrderPhase != "submitting" {
			t.Fatalf("submitting orderPhase must remain intact: %+v", it)
		}
	}

	// 再把 2 台受保护任务缩到 1 台:可删任务不足,必须返回 ErrBundleUncertainBusy 且不改动队列
	if _, err := s.UpdateQueueBundle(BundleUpdate{
		IDs:        []string{"u1", "s1"},
		Options:    []string{"newer"},
		Quantities: map[string]int{"fra": 1},
	}); !errors.Is(err, ErrBundleUncertainBusy) {
		t.Fatalf("expected ErrBundleUncertainBusy when shrinking protected tasks, got %v", err)
	}
	if len(s.Queue) != 2 || s.Queue[0].Options[0] != "new" {
		t.Fatalf("failed shrink mutated protected queue: %+v", s.Queue)
	}
}

// B2 回归:严格校验单机房 MaxOrderQuantity(20)、单次新增扇出 MaxOrderFanout(60) 与总队列上限 MaxQueueItems(500)。
func TestBundleEditEnforcesPerDCFanoutAndQueueTotalLimits(t *testing.T) {
	s := bundleState(t, []types.QueueItem{task("a1", "fra", "running")})

	// 1. 单机房超过 20 台
	if _, err := s.UpdateQueueBundle(BundleUpdate{
		IDs:        []string{"a1"},
		Quantities: map[string]int{"fra": types.MaxOrderQuantity + 1},
	}); !errors.Is(err, ErrBundleTooLarge) {
		t.Fatalf("expected ErrBundleTooLarge for >20 per DC, got %v", err)
	}

	// 2. 4 个机房各 20 台(共新增 79 台 > MaxOrderFanout 60)
	if _, err := s.UpdateQueueBundle(BundleUpdate{
		IDs:        []string{"a1"},
		Quantities: map[string]int{"fra": 20, "gra": 20, "rbx": 20, "sbg": 20},
	}); !errors.Is(err, ErrBundleTooLarge) {
		t.Fatalf("expected ErrBundleTooLarge for >60 fanout, got %v", err)
	}

	// 3. 队列已有 495 条其它任务,再增 10 台超过 MaxQueueItems(500)
	many := make([]types.QueueItem, 0, 495)
	for i := 0; i < 494; i++ {
		other := task("o"+strconv.Itoa(i), "gra", "paused")
		other.PlanCode = "other"
		many = append(many, other)
	}
	many = append(many, task("a1", "fra", "running"))
	sFull := bundleState(t, many)
	if _, err := sFull.UpdateQueueBundle(BundleUpdate{
		IDs:        []string{"a1"},
		Quantities: map[string]int{"fra": 10},
	}); !errors.Is(err, ErrBundleQueueFull) {
		t.Fatalf("expected ErrBundleQueueFull when exceeding MaxQueueItems, got %v", err)
	}
}

// B3 & B4 回归:保留原任务特权字段(QuickOrder/MaxRetries)不因显示别名删旧建新;拒绝不同 OrderGroup 混改。
func TestBundleEditPreservesAliasTaskIdentityAndRejectsDifferentOrderGroups(t *testing.T) {
	// 1. 现有任务机房存着别名 "par-a" 且带 QuickOrder=true,前端传归一化后的 {"eu-west-par-a": 1}:
	//    必须原地更新同一条任务(ID 与 QuickOrder 均保留,Added=0, Removed=0)。
	aliasTask := task("p1", "par-a", "running")
	aliasTask.QuickOrder = true
	aliasTask.MaxRetries = 9
	s := bundleState(t, []types.QueueItem{aliasTask})

	res, err := s.UpdateQueueBundle(BundleUpdate{
		IDs:        []string{"p1"},
		Options:    []string{"ram-64g"},
		Quantities: map[string]int{"eu-west-par-a": 1, "par-a": 0},
	})
	if err != nil || res.Added != 0 || res.Removed != 0 || res.Updated != 1 {
		t.Fatalf("alias task should be updated in place without delete+recreate: res=%+v err=%v", res, err)
	}
	if len(s.Queue) != 1 || s.Queue[0].ID != "p1" || !s.Queue[0].QuickOrder || s.Queue[0].MaxRetries != 9 {
		t.Fatalf("alias task lost identity or metadata: %+v", s.Queue)
	}

	// 2. 空串机房必须被拒绝(不能回退成 gra)
	if _, err := s.UpdateQueueBundle(BundleUpdate{
		IDs:        []string{"p1"},
		Quantities: map[string]int{"": 1},
	}); !errors.Is(err, ErrBundleNoTarget) {
		t.Fatalf("expected ErrBundleNoTarget for empty datacenter key, got %v", err)
	}

	// 3. 两个不同 OrderGroup 的同型号任务不能混成一单修改
	g1 := task("g1", "fra", "running")
	g1.OrderGroup, g1.GroupLimit = "group-1", 2
	g2 := task("g2", "gra", "running")
	g2.OrderGroup, g2.GroupLimit = "group-2", 2
	sGroups := bundleState(t, []types.QueueItem{g1, g2})
	if _, err := sGroups.UpdateQueueBundle(BundleUpdate{
		IDs:         []string{"g1", "g2"},
		Total:       3,
		Datacenters: []string{"fra", "gra"},
	}); !errors.Is(err, ErrBundleMixed) {
		t.Fatalf("expected ErrBundleMixed for distinct OrderGroups, got %v", err)
	}
}

// N1 回归:前端在筛选视图下只传了合计组的部分 ID 时,自动把同一 OrderGroup 的全部组员纳入修改,
// 保持所有组员的 GroupLimit/Coupon 一致,不膨胀任务数,且任何组员随后 ClaimQueueOrder 都不会报 limit changed。
func TestBundleEditAutoIncludesEntireOrderGroupWhenSubsetSubmitted(t *testing.T) {
	mk := func(id, status string) types.QueueItem {
		it := task(id, "fra", status)
		it.OrderGroup, it.GroupLimit = "grp-shared", 3
		return it
	}
	s := bundleState(t, []types.QueueItem{
		mk("m1", "running"),
		mk("m2", "paused"),
		mk("m3", "paused"),
	})
	if _, _, err := s.DB.SetOrderGroupLimit("grp-shared", 3); err != nil {
		t.Fatal(err)
	}

	// 1. 只传 m1,只改优惠码且 Total 保持 3:不得重复新增 2 条任务,m2/m3 必须一并拿到新优惠码
	res, err := s.UpdateQueueBundle(BundleUpdate{
		IDs:         []string{"m1"},
		Coupon:      "NEWCODE",
		Total:       3,
		Datacenters: []string{"fra"},
	})
	if err != nil || res.Added != 0 || res.Removed != 0 || len(s.Queue) != 3 {
		t.Fatalf("subset edit with unchanged total should keep 3 tasks: res=%+v len=%d err=%v", res, len(s.Queue), err)
	}
	for _, it := range s.Queue {
		if it.Coupon != "NEWCODE" || it.GroupLimit != 3 {
			t.Fatalf("sibling in same OrderGroup not updated: %+v", it)
		}
	}

	// 2. 只传 m1,把合计上限从 3 缩到 2:自动删掉 1 条暂停组员,剩余 2 条(含未传的组员)GroupLimit 均同步为 2,
	//    并且未被显式传入的那条组员调用 ClaimQueueOrder 能正常认领而不报 ErrGroupLimitChanged。
	res2, err := s.UpdateQueueBundle(BundleUpdate{
		IDs:         []string{"m1"},
		Total:       2,
		Datacenters: []string{"fra"},
	})
	if err != nil || res2.Removed != 1 || len(s.Queue) != 2 {
		t.Fatalf("subset shrink to 2 should leave 2 tasks: res=%+v len=%d err=%v", res2, len(s.Queue), err)
	}
	var unsubmitted types.QueueItem
	for _, it := range s.Queue {
		if it.GroupLimit != 2 {
			t.Fatalf("expected GroupLimit=2 on every remaining member, got %+v", it)
		}
		if it.ID != "m1" {
			unsubmitted = it
		}
	}
	if ok, err := s.DB.ClaimQueueOrder(unsubmitted); err != nil || !ok {
		t.Fatalf("unsubmitted sibling claim failed after group limit change: ok=%v err=%v", ok, err)
	}
}

// N2 回归:同组内自动付款状态不一致时,若调用方只提交了其中开启自动付款的子集(AutoPay=true),
// 自动纳入的、原本已关闭自动付款的组员以及扩充的新增任务绝不能被静默打开自动付款;而关闭自动付款(AutoPay=false)可对全组生效。
func TestBundleEditNeverSilentlyEnablesAutoPayOnAutoIncludedSiblings(t *testing.T) {
	a1 := task("a1", "fra", "running")
	a1.OrderGroup, a1.GroupLimit, a1.AutoPay = "grp-pay", 2, true
	a2 := task("a2", "gra", "running")
	a2.OrderGroup, a2.GroupLimit, a2.AutoPay = "grp-pay", 2, false

	s := bundleState(t, []types.QueueItem{a1, a2})
	// 1. 只传 a1 且带 AutoPay=true,同时把合计扩到 3:a2 与扩充出的第 3 条任务都必须保持 AutoPay=false
	if _, err := s.UpdateQueueBundle(BundleUpdate{
		IDs:         []string{"a1"},
		AutoPay:     true,
		Coupon:      "SAVE10",
		Total:       3,
		Datacenters: []string{"fra", "gra"},
	}); err != nil {
		t.Fatal(err)
	}
	if len(s.Queue) != 6 {
		t.Fatalf("expected 6 tasks (3 per DC in total=3 mode across fra+gra), got %d", len(s.Queue))
	}
	for _, it := range s.Queue {
		if it.Coupon != "SAVE10" || it.GroupLimit != 3 {
			t.Fatalf("expected coupon/groupLimit synced on %+v", it)
		}
		switch it.ID {
		case "a1":
			if !it.AutoPay {
				t.Fatal("explicitly submitted a1 should stay AutoPay=true")
			}
		case "a2":
			if it.AutoPay {
				t.Fatal("auto-included a2 had AutoPay=false and must NEVER be silently upgraded to AutoPay=true")
			}
		default:
			if it.AutoPay {
				t.Fatal("newly expanded task when unsubmitted sibling had AutoPay=false must also default to AutoPay=false")
			}
		}
	}

	// 2. 反过来只传 a2 且带 AutoPay=false:从开改关是安全方向,a1 也会被同步关掉
	if _, err := s.UpdateQueueBundle(BundleUpdate{
		IDs:         []string{"a2"},
		AutoPay:     false,
		Total:       3,
		Datacenters: []string{"fra", "gra"},
	}); err != nil {
		t.Fatal(err)
	}
	for _, it := range s.Queue {
		if it.AutoPay {
			t.Fatalf("turning AutoPay off must apply to all group members, got %+v", it)
		}
	}
}
