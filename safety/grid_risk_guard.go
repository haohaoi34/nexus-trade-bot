package safety

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"

	"nexus-trade-bot/config"
	"nexus-trade-bot/exchange"
	"nexus-trade-bot/logger"
)

// GridRiskOrder 是下单前风控需要的最小订单视图。
type GridRiskOrder struct {
	Symbol     string
	Side       string
	Price      float64
	Quantity   float64
	ReduceOnly bool
}

// GridRiskSnapshot 是 bot 本地仓位视图。
type GridRiskSnapshot struct {
	FilledLayers  int
	TotalNotional float64
	UnrealizedPNL float64
}

// GridRiskDecision 描述一次风控判断结果。
type GridRiskDecision struct {
	Allowed   bool
	Triggered bool
	Reasons   []string
}

// GridRiskGuard 负责合约网格账户/仓位硬风控，不做行情预测、不自动平仓。
type GridRiskGuard struct {
	cfg      *config.Config
	exchange exchange.IExchange

	mu                  sync.Mutex
	triggered           bool
	lastReason          string
	peakMarginBalance   float64
	warnedNoLiquidation map[string]bool
}

func NewGridRiskGuard(cfg *config.Config, ex exchange.IExchange) *GridRiskGuard {
	return &GridRiskGuard{
		cfg:                 cfg,
		exchange:            ex,
		warnedNoLiquidation: make(map[string]bool),
	}
}

func (g *GridRiskGuard) IsTriggered() bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.triggered
}

func (g *GridRiskGuard) LastReason() string {
	if g == nil {
		return ""
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.lastReason
}

func (g *GridRiskGuard) CheckEntryOrders(ctx context.Context, orders []GridRiskOrder, markPrice float64, snapshot GridRiskSnapshot) GridRiskDecision {
	if g == nil || g.cfg == nil || !g.cfg.ContractRisk.Enabled {
		return GridRiskDecision{Allowed: true}
	}
	entryOrders := make([]GridRiskOrder, 0, len(orders))
	for _, order := range orders {
		if !order.ReduceOnly {
			entryOrders = append(entryOrders, order)
		}
	}
	if len(entryOrders) == 0 {
		return GridRiskDecision{Allowed: true}
	}

	decision := g.evaluate(ctx, entryOrders, markPrice, snapshot)
	if decision.Triggered {
		g.setTriggered(strings.Join(decision.Reasons, "; "))
		logger.Warn("🚨 [合约风控触发] %s；拒绝新开仓=true，后续将撤销开仓挂单并保留 reduce-only 平仓保护单", strings.Join(decision.Reasons, "; "))
		return decision
	}
	g.setRecovered()
	return decision
}

func (g *GridRiskGuard) Refresh(ctx context.Context, markPrice float64, snapshot GridRiskSnapshot) GridRiskDecision {
	if g == nil || g.cfg == nil || !g.cfg.ContractRisk.Enabled {
		return GridRiskDecision{Allowed: true}
	}
	decision := g.evaluate(ctx, nil, markPrice, snapshot)
	if decision.Triggered {
		g.setTriggered(strings.Join(decision.Reasons, "; "))
		return decision
	}
	g.setRecovered()
	return decision
}

func (g *GridRiskGuard) evaluate(ctx context.Context, entryOrders []GridRiskOrder, markPrice float64, snapshot GridRiskSnapshot) GridRiskDecision {
	reasons := make([]string, 0)
	pendingNotional := pendingEntryNotional(entryOrders)
	projectedNotional := snapshot.TotalNotional + pendingNotional

	if limit := g.cfg.ContractRisk.MaxTotalNotional; limit > 0 && projectedNotional > limit {
		reasons = append(reasons, fmt.Sprintf("max_total_notional 超限：当前名义 %.4f + 待开仓 %.4f = %.4f，阈值 %.4f", snapshot.TotalNotional, pendingNotional, projectedNotional, limit))
	}
	if limit := g.cfg.ContractRisk.MaxPositionLayers; limit > 0 && snapshot.FilledLayers+len(entryOrders) > limit {
		reasons = append(reasons, fmt.Sprintf("max_position_layers 超限：当前层数 %d + 待开仓 %d = %d，阈值 %d", snapshot.FilledLayers, len(entryOrders), snapshot.FilledLayers+len(entryOrders), limit))
	}
	if snapshot.TotalNotional > 0 && snapshot.UnrealizedPNL < 0 {
		lossPct := -snapshot.UnrealizedPNL / snapshot.TotalNotional * 100
		if limit := g.cfg.ContractRisk.MaxUnrealizedLossPct; limit > 0 && lossPct >= limit {
			reasons = append(reasons, fmt.Sprintf("max_unrealized_loss_pct 超限：当前 %.4f%%，阈值 %.4f%%，浮亏 %.4f，名义 %.4f", lossPct, limit, snapshot.UnrealizedPNL, snapshot.TotalNotional))
		}
	}

	account, err := g.exchange.GetAccount(ctx)
	if err != nil {
		reasons = append(reasons, fmt.Sprintf("账户信息获取失败，为保护账户拒绝新开仓：%v", err))
	} else if account != nil {
		if usagePct, ok := marginUsagePct(account); ok {
			if limit := g.cfg.ContractRisk.MaxMarginUsagePct; limit > 0 && usagePct >= limit {
				reasons = append(reasons, fmt.Sprintf("max_margin_usage_pct 超限：当前 %.4f%%，阈值 %.4f%%", usagePct, limit))
			}
		}
		if drawdownPct, ok := g.updateAndAccountDrawdownPct(account); ok {
			if limit := g.cfg.ContractRisk.MaxAccountDrawdownPct; limit > 0 && drawdownPct >= limit {
				reasons = append(reasons, fmt.Sprintf("max_account_drawdown_pct 超限：当前 %.4f%%，阈值 %.4f%%，运行期峰值权益 %.4f，当前权益 %.4f", drawdownPct, limit, g.peakMarginBalance, accountEquity(account)))
			}
		}
	}

	if g.cfg.ContractRisk.LiquidationGuard.Enabled {
		positions, err := g.exchange.GetPositions(ctx, g.cfg.Trading.Symbol)
		if err != nil {
			logger.Warn("⚠️ [合约风控] 获取强平价信息失败，liquidation_guard 采用 fail-open，不阻止启动/开仓: %v", err)
		} else {
			reasons = append(reasons, g.liquidationReasons(positions, markPrice)...)
		}
	}

	return GridRiskDecision{Allowed: len(reasons) == 0, Triggered: len(reasons) > 0, Reasons: reasons}
}

func pendingEntryNotional(orders []GridRiskOrder) float64 {
	total := 0.0
	for _, order := range orders {
		if order.ReduceOnly {
			continue
		}
		total += math.Abs(order.Price * order.Quantity)
	}
	return total
}

func marginUsagePct(account *exchange.Account) (float64, bool) {
	equity := accountEquity(account)
	if equity <= 0 {
		return 0, false
	}
	used := equity - account.AvailableBalance
	if used < 0 {
		used = 0
	}
	return used / equity * 100, true
}

func accountEquity(account *exchange.Account) float64 {
	if account == nil {
		return 0
	}
	if account.TotalMarginBalance > 0 {
		return account.TotalMarginBalance
	}
	return account.TotalWalletBalance
}

func (g *GridRiskGuard) updateAndAccountDrawdownPct(account *exchange.Account) (float64, bool) {
	equity := accountEquity(account)
	if equity <= 0 {
		return 0, false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if equity > g.peakMarginBalance {
		g.peakMarginBalance = equity
	}
	if g.peakMarginBalance <= 0 {
		g.peakMarginBalance = equity
		return 0, true
	}
	return (g.peakMarginBalance - equity) / g.peakMarginBalance * 100, true
}

func (g *GridRiskGuard) liquidationReasons(positions []*exchange.Position, markPrice float64) []string {
	reasons := make([]string, 0)
	if markPrice <= 0 {
		return reasons
	}
	for _, pos := range positions {
		if pos == nil || pos.Size == 0 {
			continue
		}
		if !sameTradingSymbol(pos.Symbol, g.cfg.Trading.Symbol) {
			continue
		}
		if !pos.HasLiquidationPrice || pos.LiquidationPrice <= 0 {
			key := pos.Symbol
			if key == "" {
				key = g.cfg.Trading.Symbol
			}
			g.mu.Lock()
			if !g.warnedNoLiquidation[key] {
				g.warnedNoLiquidation[key] = true
				logger.Warn("⚠️ [合约风控] 交易所未提供 %s 强平价，liquidation_guard 采用 fail-open，不阻止开仓", key)
			}
			g.mu.Unlock()
			continue
		}
		distancePct := liquidationDistancePct(pos, markPrice)
		limit := g.cfg.ContractRisk.LiquidationGuard.MinDistancePct
		if distancePct >= 0 && distancePct < limit {
			reasons = append(reasons, fmt.Sprintf("liquidation_guard 距离过近：当前 %.4f%%，阈值 %.4f%%，mark %.8f，强平价 %.8f", distancePct, limit, markPrice, pos.LiquidationPrice))
		}
	}
	return reasons
}

func liquidationDistancePct(pos *exchange.Position, markPrice float64) float64 {
	if pos == nil || markPrice <= 0 || pos.LiquidationPrice <= 0 {
		return -1
	}
	if pos.Size < 0 {
		return (pos.LiquidationPrice - markPrice) / markPrice * 100
	}
	return (markPrice - pos.LiquidationPrice) / markPrice * 100
}

func (g *GridRiskGuard) setTriggered(reason string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.triggered = true
	g.lastReason = reason
}

func (g *GridRiskGuard) setRecovered() {
	g.mu.Lock()
	wasTriggered := g.triggered
	oldReason := g.lastReason
	g.triggered = false
	g.lastReason = ""
	g.mu.Unlock()
	if wasTriggered {
		logger.Info("✅ [合约风控恢复] 指标恢复到阈值以内，恢复新开仓检查通过；上一原因: %s", oldReason)
	}
}
