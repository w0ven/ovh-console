package catalog

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/ovh-buy/server/internal/app"
	"github.com/ovh-buy/server/internal/ovh"
)

// PlanVerdict 机型归属判定。
//
// 这份判定同时被三个地方用:入队闸门(挡住建不出来的任务)、抢购重试闸门(挡住
// 重试一万次也不会变的任务)、以及给用户看的诊断文案。以前它在 handlers/queue.go
// 和 purchase/purchase.go 各有一份**逐字复制**的实现,已经开始分裂(日志前缀不同)——
// 两处口径一旦真的分开,就会出现"能入队但每轮都被判死"的幽灵任务。
//
// 判据说明:eco 目录只回答"本工具能不能用 /order/cart/{id}/eco 下单它",
// **不回答"它属于哪个大区"**。实测公开接口(2026-08):EU 站点 availabilities 有
// 244 个 planCode、eco 目录只有 99 个,145 个差集里整条 Scale / HCI / SDS /
// High-Grade 产品线都在;US 站点是 423 对 143。所以归属必须靠可用性接口探。
type PlanVerdict int

const (
	PlanVerdictOK          PlanVerdict = iota // 属于本区,而且本工具下得了单
	PlanVerdictUnknown                        // 探测/目录失败 —— 不下结论,保持"无货"语义继续重试
	PlanVerdictCrossRegion                    // ① 真跨区:别的大区才有它的库存记录
	PlanVerdictNotEco                         // ② 本区有它,但不属于 Eco 系列,本工具下不了
	PlanVerdictNoSuchPlan                     // ③ 三区都查不到:planCode 拼错 / 已下架
	PlanVerdictPrerelease                     // Eco 系列在本区库存中出现,尚未进入销售目录
)

// 只识别本工具支持的 Eco 系列候选;是否存在仍须由库存接口确认。
var ecoStockPlanCode = regexp.MustCompile(`(?i)^[0-9]{2}(sk|sys|rise)[a-z0-9]*(-v[0-9]+)?(-[a-z]+)?$`)

const preorderCatalogTTL = 30 * time.Second

// PreorderCheckInterval bounds optional catalog metadata refreshes and API launch retries.
const PreorderCheckInterval = 15 * time.Second

// PreorderReady 用短缓存等上架,避免普通目录两小时缓存错过开售;并发刷新仍合并。
func PreorderReady(state *app.State, accountID, planCode string) (bool, error) {
	acc, ok := state.FindAccount(accountID)
	if !ok {
		return false, fmt.Errorf("下单账户不存在")
	}
	cat, err := loadSubsidiaryCatalogWithTTL(state, SubsidiaryOfAccount(acc), preorderCatalogTTL)
	if err != nil {
		return false, err
	}
	_, ready := cat.plans[planCode]
	return ready, nil
}

// ClassifyPlan 判定 planCode 对这个账户属于哪种情况,并给出面向用户的说明。
// 说明为空 = 无需提示(OK / Unknown),调用方必须保持原来的"无货,下轮再来"语义。
//
// 顺序有讲究:先查本子公司的 eco 目录(2 小时内存缓存,不耗账户配额、命中时不发网络请求),
// 有就直接放行 —— 抢购主链路上正常任务一次探测都不会多发。只有"目录里没有"才去探大区归属。
//
// logSource 只影响日志归类(queue / purchase),不影响判定。
func ClassifyPlan(state *app.State, accountID, planCode, logSource string) (PlanVerdict, string) {
	acc, _ := state.FindAccount(accountID)
	accRegion := ovh.EndpointRegion(acc.Endpoint)
	subsidiary := SubsidiaryOfAccount(acc)

	_, catErr := AddonFamiliesForPlan(state, accountID, planCode)
	if catErr == nil {
		// 本子公司的 Eco 目录里就有它,能下单,不用再探
		return PlanVerdictOK, ""
	}
	if !errors.Is(catErr, ErrPlanNotInCatalog) {
		// 目录拉不动(网络 / 429):不下结论。这里下结论 = 一次瞬断把正常任务判死。
		state.Logger.Warn(fmt.Sprintf("判定 %s 归属时目录拉取失败(子公司 %s)，本次不下结论: %s",
			planCode, subsidiary, catErr.Error()), logSource)
		return PlanVerdictUnknown, ""
	}

	// 先看是不是纯粹的大小写问题 —— 目录已经在内存里,这一步不发网络请求,
	// 而且能把下面那次探测也省掉。
	//
	// 不在入口处统一 ToLower 是因为目录里确实存在带大写的 planCode
	// (vps-2025-model1.LZ 那一批),一律转小写会把它们弄坏。详见 CanonicalPlanCode。
	if canonical, differs := CanonicalPlanCode(state, accountID, planCode); differs {
		return PlanVerdictNoSuchPlan, fmt.Sprintf(
			"机型 %s 的大小写不对。本区目录里它的正确写法是 %s —— 照这个改一下就能下单了。"+
				"(planCode 区分大小写,而手机键盘常会自动把首字母变大写)",
			planCode, canonical)
	}

	// 本区排第一:命中就立刻返回,只花一次公开请求
	region, probeErr := RegionOfPlan(state, planCode, []string{accRegion, "EU", "US", "CA"})
	if probeErr != nil {
		state.Logger.Warn(fmt.Sprintf("探测 %s 的大区归属失败，本次不下结论: %s",
			planCode, probeErr.Error()), logSource)
		return PlanVerdictUnknown, ""
	}

	switch {
	case region == "":
		// ③ 三区的可用性接口都没有这个 planCode 的任何记录(哪怕缺货也会有记录)
		return PlanVerdictNoSuchPlan, fmt.Sprintf(
			"机型 %s 在 OVH 的 EU / US / CA 三个站点都查不到任何库存记录（缺货的机型也会有记录）："+
				"planCode 可能拼错了，或者这个机型已经下架。",
			planCode)
	case region != accRegion:
		// ① 真跨区:别的大区有它的库存记录
		return PlanVerdictCrossRegion, fmt.Sprintf(
			"机型 %s 属于 OVH 的 %s 区，而账户 %s 在 %s 区（%s）。三个站点的目录 / 库存 / 购物车互不相通"+
				"（美区机型带 -us / -eu / -ca 后缀，欧区和加区不带），请改用本区的 planCode，或换一个 %s 区的账户下单。",
			planCode, region, acc.Name, accRegion, acc.Endpoint, region)
	}
	if ecoStockPlanCode.MatchString(planCode) {
		return PlanVerdictPrerelease, fmt.Sprintf("机型 %s 已出现在本区库存中，但尚未进入账户销售目录；请创建预订抢购任务等待上架。", planCode)
	}

	// ② 本区有货但不走 Eco 下单链路
	return PlanVerdictNotEco, fmt.Sprintf(
		"机型 %s 在本区（%s 区，子公司 %s）确实有库存记录，但它不在该子公司的 Eco 目录里 —— "+
			"Scale / HCI / SDS / High-Grade 这些产品线都不走 Eco（实测欧区 244 个有库存的机型里 145 个如此）。"+
			"本工具的下单链路只有 /order/cart/eco 一条，买不了这台，请到 OVH 官网下单。",
		planCode, accRegion, subsidiary)
}

// DatacenterVerdict 判定目标机房是否属于某 (账户子公司, planCode) 的销售目录。
//
// 为什么必须与"当前无库存"区分:OVH 在 summary / checkout 阶段对以下两种截然不同的场景
// 返回完全同一句 400 报错 "<fqn> is not available in <dc>":
//  1. 机房就在本子公司目录里(如 IE 的 27skle06-v1 含 fra/lon),但结账瞬间库存已被抢空;
//  2. 机房根本不在该子公司该机型的目录里(如在 IE 区填了美区 hil,或只卖 sgp 的机型填了 gra)。
type DatacenterVerdict int

const (
	// DatacenterVerdictUnknown 目录暂不可用、机型尚未进公开目录、或目录未声明机房列表 —— 不妄下定论。
	DatacenterVerdictUnknown DatacenterVerdict = iota
	// DatacenterVerdictInCatalog 目标机房明确属于该 (账户子公司, planCode) 目录的可选机房列表。
	DatacenterVerdictInCatalog
	// DatacenterVerdictOutOfCatalog 目录已给出该机型的非空可选机房列表,且刷新短缓存后仍不包含目标机房。
	DatacenterVerdictOutOfCatalog
)

// ClassifyDatacenter 判定 datacenter 是否在该账户所属子公司的 planCode 目录里,并返回目录中的可选机房列表。
func ClassifyDatacenter(state *app.State, accountID, planCode, datacenter string) (DatacenterVerdict, []string) {
	if state == nil {
		return DatacenterVerdictUnknown, nil
	}
	acc, _ := state.FindAccount(accountID)
	return ClassifyDatacenterInSubsidiary(state, SubsidiaryOfAccount(acc), planCode, datacenter)
}

// ClassifyDatacenterInSubsidiary 按指定子公司(与实际建车的 orderSubsidiary 口径一致)判定机房是否属于该型号目录:
//  1. 命中 2 小时缓存的合法机房直接返回 InCatalog(0 网络请求);
//  2. 型号不在目录或机房列表为空直接返回 Unknown(不触发 15 秒短 TTL 目录刷新,避免无意义拉取 12MB 目录);
//  3. 仅当"型号在目录、机房列表非空、但目标机房不在列表"时按短 TTL 刷新核实一次,且必须确认拿到的是刚刷新的新目录
//     (而非刷新失败时的旧目录兜底)才允许返回 OutOfCatalog,防止目录接口瞬断时误判。
func ClassifyDatacenterInSubsidiary(state *app.State, subsidiary, planCode, datacenter string) (DatacenterVerdict, []string) {
	subsidiary = strings.ToUpper(strings.TrimSpace(subsidiary))
	planCode = strings.TrimSpace(planCode)
	datacenter = strings.TrimSpace(datacenter)
	if state == nil || subsidiary == "" || planCode == "" || datacenter == "" {
		return DatacenterVerdictUnknown, nil
	}
	cat, err := loadSubsidiaryCatalog(state, subsidiary)
	if err != nil {
		return DatacenterVerdictUnknown, nil
	}
	pc, ok := cat.plans[planCode]
	if !ok || len(pc.datacenters) == 0 {
		return DatacenterVerdictUnknown, nil
	}
	if datacenterInList(pc.datacenters, datacenter) {
		return DatacenterVerdictInCatalog, append([]string(nil), pc.datacenters...)
	}
	// 仅在型号已存在且机房列表非空但未命中时,用短 TTL 刷新核实是否刚扩充了新机房。
	cat, err = loadSubsidiaryCatalogWithTTL(state, subsidiary, PreorderCheckInterval)
	if err != nil {
		return DatacenterVerdictUnknown, nil
	}
	pc, ok = cat.plans[planCode]
	if !ok || len(pc.datacenters) == 0 {
		return DatacenterVerdictUnknown, nil
	}
	dcs := append([]string(nil), pc.datacenters...)
	if datacenterInList(dcs, datacenter) {
		return DatacenterVerdictInCatalog, dcs
	}
	// loadSubsidiaryCatalogWithTTL 在远端 5xx/超时且存在旧缓存时会返回 stale 目录且 err == nil;
	// 此时不能拿过期旧目录断言 OutOfCatalog,必须确认目录确实在短 TTL 窗口内刷新成功。
	if time.Since(cat.fetchedAt) > PreorderCheckInterval {
		return DatacenterVerdictUnknown, nil
	}
	return DatacenterVerdictOutOfCatalog, dcs
}

func datacenterInList(dcs []string, target string) bool {
	raw := strings.TrimSpace(target)
	if raw == "" {
		return false
	}
	want := ovh.ConvertDisplayDCToAPIDC(raw)
	for _, dc := range dcs {
		trimmed := strings.TrimSpace(dc)
		if trimmed == "" {
			continue
		}
		if strings.EqualFold(trimmed, raw) || strings.EqualFold(ovh.ConvertDisplayDCToAPIDC(trimmed), want) {
			return true
		}
	}
	return false
}

// FormatOutOfCatalogDatacenterMessage 构造"机房确实不在本子公司目录"的统一诊断说明。
func FormatOutOfCatalogDatacenterMessage(subsidiary, planCode, datacenter string, catalogDCs []string) string {
	dcLabel := strings.ToUpper(strings.TrimSpace(datacenter))
	sub := strings.ToUpper(strings.TrimSpace(subsidiary))
	region := ovh.SubsidiaryRegion(sub)
	if len(catalogDCs) > 0 {
		return fmt.Sprintf("机房 %s 不在子公司 %s（%s 区）的 %s 目录里（该机型在本区可选机房：%s；OVH 三站目录独立，同一机型在不同区可选的机房不同，请改选本区支持的机房或切换对应区账户）",
			dcLabel, sub, region, planCode, strings.Join(catalogDCs, ", "))
	}
	return fmt.Sprintf("机房 %s 不在子公司 %s（%s 区）的 %s 目录里；OVH 三站目录独立，同一机型在不同区可选的机房不同",
		dcLabel, sub, region, planCode)
}
