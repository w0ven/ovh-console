package app

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/ovh-buy/server/internal/orderterm"
	"github.com/ovh-buy/server/internal/ovh"
	"github.com/ovh-buy/server/internal/types"
)

// 整单编辑。
//
// 队列页把同账户、同型号、同配置的任务合成「一单」显示(可跨机房)。以前编辑只改其中一台,
// 改完一单被拆成两行、其余几台还是旧配置;数量也只能一台台加删。这里把一单作为一个整体修改:
//   - 配置(选项、周期、间隔、自动付款、优惠码)应用到这一单所有未下单的任务,原地修改,不新建;
//   - 数量:按机房模式给每个机房「还要抢几台」,多了新增任务、少了删除未下单的任务;
//     合计模式(GroupLimit>0,多机房共享上限)给「合计几台」和要抢的机房;
//   - 已下单的任务一律不动;正在提交结账或结账结果待人工确认的任务不能在缩量时直接删除。
//
// 全部在一次落库里完成:要么整单改好,要么什么都没变。

var (
	ErrBundleMixed         = errors.New("这些任务不属于同一单(账户、型号或订单组不同),不能一起修改")
	ErrBundleNoTarget      = errors.New("至少要保留一个有效机房、一台服务器")
	ErrBundleTooLarge      = fmt.Errorf("单个机房或整单合计最多 %d 台,单次最多新增 %d 个任务", types.MaxOrderQuantity, types.MaxOrderFanout)
	ErrBundleQueueFull     = fmt.Errorf("队列任务总数不能超过 %d 条", types.MaxQueueItems)
	ErrBundleUncertainBusy = errors.New("该机房存在正在提交结账或结账结果待确认的任务,请先在抢购历史/队列中核对后再缩减该任务")
	ErrBundleNoOptions     = errors.New("配置无效")
)

type BundleUpdate struct {
	IDs           []string
	Options       []string
	Duration      *string
	RetryInterval int
	AutoPay       bool
	Coupon        string
	// Quantities 按机房模式:机房 → 还要抢的台数(未下单任务数)。nil 表示数量不变。
	Quantities map[string]int
	// Total 合计模式:这一单合计要抢的台数(含已下单);Datacenters 为参与的机房。Total<=0 表示不变。
	Total       int
	Datacenters []string
}

type BundleResult struct {
	Updated int `json:"updated"`
	Added   int `json:"added"`
	Removed int `json:"removed"`
}

// canonicalBundleDC 将机房写法统一成标准 API 小写机房码（如 "PAR-A" -> "eu-west-par-a"，"MUM" -> "ynm"）。
// 空串直接返回 ""（不走 ConvertDisplayDCToAPIDC 的空串回退 "gra"）。
func canonicalBundleDC(dc string) string {
	trimmed := strings.TrimSpace(dc)
	if trimmed == "" {
		return ""
	}
	return ovh.ConvertDisplayDCToAPIDC(trimmed)
}

// isBundleProtectedFromRemoval 判断任务是否处于结账结果待人工核对或正在写出结账阶段，缩量时严禁静默删掉。
func isBundleProtectedFromRemoval(it types.QueueItem) bool {
	return it.StopReason == StopReasonCheckoutUncertain || it.OrderPhase == "submitting"
}

// InspectQueueBundleMeta 读取本单代表任务的账户、型号与预售标记，供路由层在写库前做目录机房校验。
func (s *State) InspectQueueBundleMeta(ids []string) (accountID, planCode string, preorder, found bool) {
	want := map[string]bool{}
	for _, id := range ids {
		want[id] = true
	}
	s.QueueMu.Lock()
	defer s.QueueMu.Unlock()
	for _, it := range s.Queue {
		if want[it.ID] {
			return it.AccountID, it.PlanCode, it.Preorder, true
		}
	}
	return "", "", false, false
}

// UpdateQueueBundle 原子地修改一整单。
func (s *State) UpdateQueueBundle(u BundleUpdate) (BundleResult, error) {
	var res BundleResult
	if err := s.SaveBlocked("queue"); err != nil {
		return res, err
	}
	if u.Duration != nil {
		d, err := orderterm.Normalize(*u.Duration)
		if err != nil {
			return res, err
		}
		u.Duration = &d
	}
	want := map[string]bool{}
	explicitWant := map[string]bool{}
	for _, id := range u.IDs {
		want[id] = true
		explicitWant[id] = true
	}

	s.saveQueueMu.Lock()
	defer s.saveQueueMu.Unlock()
	s.QueueMu.Lock()
	defer s.QueueMu.Unlock()

	next := append([]types.QueueItem(nil), s.Queue...)
	var members []int   // next 下标:本单未下单的任务
	var completed []int // 本单已下单的任务(只用于合计模式计数)
	for i := range next {
		if !want[next[i].ID] {
			continue
		}
		if next[i].Status == "completed" {
			completed = append(completed, i)
			continue
		}
		members = append(members, i)
	}
	if len(members) == 0 && len(completed) == 0 {
		return res, ErrQueueNotFound
	}
	all := append(append([]int(nil), members...), completed...)
	tpl := next[all[0]]
	shared := tpl.GroupLimit > 0
	for _, i := range all {
		it := next[i]
		// 必须同时校验 OrderGroup 与共享上限模式一致，防止把两个不同合计组或合计/非合计任务混成一单导致配额错算多买
		if it.AccountID != tpl.AccountID || it.PlanCode != tpl.PlanCode || it.Preorder != tpl.Preorder ||
			it.OrderGroup != tpl.OrderGroup || (it.GroupLimit > 0) != shared {
			return res, ErrBundleMixed
		}
	}
	// N1 防护：若本单带有共享 OrderGroup（例如前端在按状态/机房筛选视图下只把可见任务 ID 传了上来），
	// 自动把队列中同一 OrderGroup 的其余全部任务一并纳入本单，保证组内 GroupLimit/配置完全一致，
	// 防止被筛掉的同组任务残留旧 GroupLimit 在下次 ClaimQueueOrder 时报错或重复建出多余任务。
	if tpl.OrderGroup != "" {
		included := make(map[int]bool, len(all))
		for _, idx := range all {
			included[idx] = true
		}
		for i := range next {
			if included[i] || next[i].OrderGroup != tpl.OrderGroup {
				continue
			}
			it := next[i]
			if it.AccountID != tpl.AccountID || it.PlanCode != tpl.PlanCode || it.Preorder != tpl.Preorder || (it.GroupLimit > 0) != shared {
				return res, ErrBundleMixed
			}
			want[it.ID] = true
			if it.Status == "completed" {
				completed = append(completed, i)
			} else {
				members = append(members, i)
			}
		}
	}

	anyRunning := false
	for _, i := range members {
		if next[i].Status == "running" || next[i].Status == "pending" {
			anyRunning = true
		}
	}
	newStatus := "paused"
	if anyRunning {
		newStatus = "running"
	}

	// 目标:每个机房还要多少个未下单任务
	target := map[string]int{}
	group, limit := tpl.OrderGroup, tpl.GroupLimit
	switch {
	case shared && u.Total > 0:
		if len(u.Datacenters) == 0 {
			return res, ErrBundleNoTarget
		}
		if u.Total > types.MaxOrderQuantity {
			return res, ErrBundleTooLarge
		}
		limit = u.Total
		remaining := limit - len(completed)
		if remaining < 0 {
			remaining = 0
		}
		seenDC := map[string]bool{}
		for _, rawDC := range u.Datacenters {
			dc := canonicalBundleDC(rawDC)
			if dc == "" {
				return res, ErrBundleNoTarget
			}
			if seenDC[dc] {
				continue
			}
			seenDC[dc] = true
			target[dc] = remaining
		}
		if len(target) == 0 {
			return res, ErrBundleNoTarget
		}
		if group == "" {
			group = uuid.NewString()
		}
	case !shared && u.Quantities != nil:
		total := 0
		for rawDC, n := range u.Quantities {
			dc := canonicalBundleDC(rawDC)
			if dc == "" {
				return res, ErrBundleNoTarget
			}
			if n < 0 {
				n = 0
			}
			if n > types.MaxOrderQuantity {
				return res, ErrBundleTooLarge
			}
			if n > 0 {
				target[dc] += n
				if target[dc] > types.MaxOrderQuantity {
					return res, ErrBundleTooLarge
				}
				total += n
			} else if _, exists := target[dc]; !exists {
				// 保留 0 值键但不覆盖同名别名已累加的正数（如 {"par-a":2, "eu-west-par-a":0} 仍为 2）
				target[dc] = 0
			}
		}
		positiveTotal := 0
		for dc, n := range target {
			if n <= 0 {
				delete(target, dc)
			} else {
				positiveTotal += n
			}
		}
		_ = total
		if positiveTotal == 0 && len(completed) == 0 {
			return res, ErrBundleNoTarget
		}
	default:
		target = nil // 数量不变
	}

	// 1) 配置原地应用到所有未下单任务（但处于 checkoutUncertain 的暂停任务绝不自动清除其 StopReason）
	opts := append([]string(nil), u.Options...)
	changed := map[string]bool{}
	for _, i := range members {
		it := &next[i]
		before := groupSignature(*it)
		it.Options = append([]string(nil), opts...)
		if u.Duration != nil {
			it.Duration = *u.Duration
		}
		if u.RetryInterval > 0 {
			it.RetryInterval = u.RetryInterval
		}
		// N2 防护：对于调用方未显式传入、靠同一 OrderGroup 自动纳入的同组任务，
		// 自动付款只允许从 true 改为 false，绝不允许从 false 静默升级为 true（禁止自动开启扣款）。
		if explicitWant[it.ID] || !u.AutoPay {
			it.AutoPay = u.AutoPay
		}
		it.Coupon = strings.TrimSpace(u.Coupon)
		if shared {
			it.OrderGroup, it.GroupLimit = group, limit
			// 合计数调高后,因「已凑够」暂停的任务重新参与（checkoutUncertain 绝不自动恢复）
			if it.StopReason == "groupComplete" && anyRunning {
				it.Status, it.StopReason = "running", ""
			}
		}
		if before != groupSignature(*it) {
			it.FailureCount, it.LastCheckTime = 0, 0
			if it.OrderPhase != "submitting" {
				it.OrderPhase = ""
			}
			changed[it.ID] = true
		}
		it.UpdatedAt = types.NowISO()
		res.Updated++
	}

	// 2) 按机房调整数量
	var removedItems []types.QueueItem
	removed := map[string]bool{}
	added := map[string]bool{}
	if target != nil {
		byDC := map[string][]int{}
		for _, i := range members {
			k := canonicalBundleDC(next[i].Datacenter)
			byDC[k] = append(byDC[k], i)
		}
		// 多出来的:仅从非保护任务（非 checkoutUncertain、非 submitting）中先删暂停的、再删最新建的；
		// 若可删除任务不足以满足缩量目标，明确报错拦截，绝不静默删掉需要人工核对订单的任务。
		for dc, idxs := range byDC {
			extra := len(idxs) - target[dc]
			if extra <= 0 {
				continue
			}
			deletable := make([]int, 0, len(idxs))
			for _, idx := range idxs {
				if !isBundleProtectedFromRemoval(next[idx]) {
					deletable = append(deletable, idx)
				}
			}
			if len(deletable) < extra {
				return BundleResult{}, ErrBundleUncertainBusy
			}
			sort.SliceStable(deletable, func(a, b int) bool {
				ra, rb := next[deletable[a]].Status == "running", next[deletable[b]].Status == "running"
				if ra != rb {
					return !ra
				}
				return next[deletable[a]].CreatedAt > next[deletable[b]].CreatedAt
			})
			for _, i := range deletable[:extra] {
				removed[next[i].ID] = true
				removedItems = append(removedItems, next[i])
			}
		}
		// 不够的:先计算本次总新增台数并校验单次扇出上限 MaxOrderFanout
		dcs := make([]string, 0, len(target))
		totalToAdd := 0
		for dc, wantN := range target {
			dcs = append(dcs, dc)
			if addN := wantN - len(byDC[dc]); addN > 0 {
				totalToAdd += addN
			}
		}
		if totalToAdd > types.MaxOrderFanout {
			return BundleResult{}, ErrBundleTooLarge
		}
		// 若组内存在被自动纳入且原先关闭自动付款的组员，说明调用方并未获全组显式授权开启自动付款，新增任务同样保守默认关闭。
		allowNewAutoPay := u.AutoPay
		for _, idx := range members {
			if !explicitWant[next[idx].ID] && !next[idx].AutoPay {
				allowNewAutoPay = false
				break
			}
		}
		sort.Strings(dcs)
		for _, dc := range dcs {
			for n := len(byDC[dc]); n < target[dc]; n++ {
				item := types.QueueItem{
					ID: uuid.NewString(), AccountID: tpl.AccountID, PlanCode: tpl.PlanCode, Datacenter: dc,
					Options: append([]string(nil), opts...), Duration: tpl.Duration, Status: newStatus,
					CreatedAt: types.NowISO(), UpdatedAt: types.NowISO(), RetryInterval: tpl.RetryInterval,
					AutoPay: allowNewAutoPay, Coupon: strings.TrimSpace(u.Coupon), Preorder: tpl.Preorder,
					PreorderWaitStock: tpl.PreorderWaitStock,
					QuickOrder:        tpl.QuickOrder, MaxRetries: tpl.MaxRetries,
				}
				if u.Duration != nil {
					item.Duration = *u.Duration
				}
				if u.RetryInterval > 0 {
					item.RetryInterval = u.RetryInterval
				}
				if shared {
					item.OrderGroup, item.GroupLimit = group, limit
				}
				next = append(next, item)
				added[item.ID] = true
				res.Added++
			}
		}
	}
	if len(removed) > 0 {
		kept := next[:0:0]
		for _, it := range next {
			if removed[it.ID] {
				continue
			}
			kept = append(kept, it)
		}
		next = kept
		res.Removed = len(removed)
		res.Updated -= len(removed)
	}
	if res.Updated < 0 {
		res.Updated = 0
	}
	if len(next) > types.MaxQueueItems {
		return BundleResult{}, fmt.Errorf("%w（当前修改后共 %d 条）", ErrBundleQueueFull, len(next))
	}

	// 合计数改了:先同步数据库里登记的上限,再写队列;写队列失败时把上限改回去
	var restoreLimit func()
	if shared && limit != tpl.GroupLimit {
		prev, had, err := s.DB.SetOrderGroupLimit(group, limit)
		if err != nil {
			return BundleResult{}, err
		}
		if had {
			restoreLimit = func() { _, _, _ = s.DB.SetOrderGroupLimit(group, prev) }
		}
	}
	s.criticalGenMu.Lock()
	snapGen := s.queueGen
	s.criticalGenMu.Unlock()
	writeFn := s.replaceQueueFn
	if writeFn == nil {
		writeFn = s.DB.ReplaceQueue
	}
	if err := writeFn(next); err != nil {
		if restoreLimit != nil {
			restoreLimit()
		}
		return BundleResult{}, err
	}
	s.Queue = next
	s.advanceQueuePersistedGen(snapGen)

	// 落库之后再中断/删除:失败时内存与磁盘都保持原样
	keptDCs := map[string]bool{}
	for _, it := range next {
		if want[it.ID] || added[it.ID] {
			keptDCs[canonicalBundleDC(it.Datacenter)] = true
		}
	}
	for _, it := range removedItems {
		s.MarkTaskDeleted(it.ID)
		if !keptDCs[canonicalBundleDC(it.Datacenter)] {
			// 这个机房整个不抢了:与删除任务一致,挡住监控在这个机房自动建单
			if err := s.SetAutomaticOrderBlockedLocked(it, true); err != nil {
				s.Logger.Warn("整单编辑:自动建单停止记录未能写入: "+err.Error(), "queue")
			}
		}
	}
	for _, it := range next {
		if it.Status != "running" || (!want[it.ID] && !added[it.ID]) {
			continue
		}
		if err := s.SetAutomaticOrderBlockedLocked(it, false); err != nil {
			s.Logger.Warn("整单编辑:自动建单停止记录未能更新: "+err.Error(), "queue")
		}
		if changed[it.ID] {
			s.UnregisterTaskCancel(it.ID) // 中断旧配置那一轮(结账发出后不受影响)
		}
	}
	s.WakeQueue()
	return res, nil
}

// groupSignature 决定下单行为的字段;变了才中断本轮、清零失败计数。
func groupSignature(it types.QueueItem) string {
	opts := append([]string(nil), it.Options...)
	sort.Strings(opts)
	return strings.Join([]string{strings.Join(opts, ","), it.Duration, it.Coupon, boolStr(it.AutoPay), it.OrderGroup, strconv.Itoa(it.GroupLimit), it.Status}, "|")
}

func boolStr(b bool) string {
	if b {
		return "1"
	}
	return "0"
}
