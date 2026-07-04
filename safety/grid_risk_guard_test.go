package safety

import (
	"context"
	"testing"

	"nexus-trade-bot/config"
	"nexus-trade-bot/exchange"
)

type gridRiskFakeExchange struct {
	account   *exchange.Account
	positions []*exchange.Position
}

func (e *gridRiskFakeExchange) GetName() string { return "fake" }
func (e *gridRiskFakeExchange) PlaceOrder(context.Context, *exchange.OrderRequest) (*exchange.Order, error) {
	return nil, nil
}
func (e *gridRiskFakeExchange) BatchPlaceOrders(context.Context, []*exchange.OrderRequest) ([]*exchange.Order, bool) {
	return nil, false
}
func (e *gridRiskFakeExchange) CancelOrder(context.Context, string, int64) error { return nil }
func (e *gridRiskFakeExchange) BatchCancelOrders(context.Context, string, []int64) error {
	return nil
}
func (e *gridRiskFakeExchange) CancelAllOrders(context.Context, string) error { return nil }
func (e *gridRiskFakeExchange) GetOrder(context.Context, string, int64) (*exchange.Order, error) {
	return nil, nil
}
func (e *gridRiskFakeExchange) GetOpenOrders(context.Context, string) ([]*exchange.Order, error) {
	return nil, nil
}
func (e *gridRiskFakeExchange) GetAccount(context.Context) (*exchange.Account, error) {
	return e.account, nil
}
func (e *gridRiskFakeExchange) GetPositions(context.Context, string) ([]*exchange.Position, error) {
	return e.positions, nil
}
func (e *gridRiskFakeExchange) GetBalance(context.Context, string) (float64, error) { return 0, nil }
func (e *gridRiskFakeExchange) StartOrderStream(context.Context, func(interface{})) error {
	return nil
}
func (e *gridRiskFakeExchange) StopOrderStream() error { return nil }
func (e *gridRiskFakeExchange) GetLatestPrice(context.Context, string) (float64, error) {
	return 0, nil
}
func (e *gridRiskFakeExchange) StartPriceStream(context.Context, string, func(float64)) error {
	return nil
}
func (e *gridRiskFakeExchange) StartKlineStream(context.Context, []string, string, exchange.CandleUpdateCallback) error {
	return nil
}
func (e *gridRiskFakeExchange) StopKlineStream() error { return nil }
func (e *gridRiskFakeExchange) GetHistoricalKlines(context.Context, string, string, int) ([]*exchange.Candle, error) {
	return nil, nil
}
func (e *gridRiskFakeExchange) GetPriceDecimals() int    { return 2 }
func (e *gridRiskFakeExchange) GetQuantityDecimals() int { return 4 }
func (e *gridRiskFakeExchange) GetBaseAsset() string     { return "BTC" }
func (e *gridRiskFakeExchange) GetQuoteAsset() string    { return "USDT" }

func baseGridRiskConfig() *config.Config {
	cfg := &config.Config{}
	cfg.App.CurrentExchange = "bitget"
	cfg.App.MarketType = "futures"
	cfg.Exchanges = map[string]config.ExchangeConfig{"bitget": {APIKey: "k", SecretKey: "s"}}
	cfg.Trading.Symbol = "BTCUSDT"
	cfg.Trading.Direction = "long"
	cfg.Trading.PriceInterval = 10
	cfg.Trading.OrderQuantity = 100
	cfg.Trading.BuyWindowSize = 10
	cfg.Trading.SellWindowSize = 10
	cfg.ContractRisk.Enabled = true
	cfg.ContractRisk.MaxMarginUsagePct = 70
	cfg.ContractRisk.MaxUnrealizedLossPct = 20
	cfg.ContractRisk.MaxAccountDrawdownPct = 25
	cfg.ContractRisk.LiquidationGuard.Enabled = true
	cfg.ContractRisk.LiquidationGuard.MinDistancePct = 5
	if err := cfg.Validate(); err != nil {
		panic(err)
	}
	cfg.ContractRisk.Enabled = true
	return cfg
}

func safeFakeExchange() *gridRiskFakeExchange {
	return &gridRiskFakeExchange{
		account: &exchange.Account{TotalMarginBalance: 1000, AvailableBalance: 800},
		positions: []*exchange.Position{{
			Symbol:              "BTCUSDT",
			Size:                1,
			MarkPrice:           100,
			LiquidationPrice:    50,
			HasLiquidationPrice: true,
		}},
	}
}

func entryOrders() []GridRiskOrder {
	return []GridRiskOrder{{Symbol: "BTCUSDT", Side: "BUY", Price: 100, Quantity: 1}}
}

func TestGridRiskGuardRejectsMaxTotalNotionalExceeded(t *testing.T) {
	cfg := baseGridRiskConfig()
	cfg.ContractRisk.MaxTotalNotional = 150
	guard := NewGridRiskGuard(cfg, safeFakeExchange())
	decision := guard.CheckEntryOrders(context.Background(), entryOrders(), 100, GridRiskSnapshot{TotalNotional: 100})
	if decision.Allowed {
		t.Fatalf("expected max_total_notional to reject")
	}
}

func TestGridRiskGuardRejectsMaxPositionLayersExceeded(t *testing.T) {
	cfg := baseGridRiskConfig()
	cfg.ContractRisk.MaxPositionLayers = 1
	guard := NewGridRiskGuard(cfg, safeFakeExchange())
	decision := guard.CheckEntryOrders(context.Background(), entryOrders(), 100, GridRiskSnapshot{FilledLayers: 1})
	if decision.Allowed {
		t.Fatalf("expected max_position_layers to reject")
	}
}

func TestGridRiskGuardRejectsMaxMarginUsageExceeded(t *testing.T) {
	cfg := baseGridRiskConfig()
	cfg.ContractRisk.MaxMarginUsagePct = 50
	ex := safeFakeExchange()
	ex.account = &exchange.Account{TotalMarginBalance: 1000, AvailableBalance: 400}
	guard := NewGridRiskGuard(cfg, ex)
	decision := guard.CheckEntryOrders(context.Background(), entryOrders(), 100, GridRiskSnapshot{})
	if decision.Allowed {
		t.Fatalf("expected max_margin_usage_pct to reject")
	}
}

func TestGridRiskGuardRejectsMaxUnrealizedLossExceeded(t *testing.T) {
	cfg := baseGridRiskConfig()
	cfg.ContractRisk.MaxUnrealizedLossPct = 10
	guard := NewGridRiskGuard(cfg, safeFakeExchange())
	decision := guard.CheckEntryOrders(context.Background(), entryOrders(), 100, GridRiskSnapshot{TotalNotional: 1000, UnrealizedPNL: -150})
	if decision.Allowed {
		t.Fatalf("expected max_unrealized_loss_pct to reject")
	}
}

func TestGridRiskGuardRejectsMaxAccountDrawdownExceeded(t *testing.T) {
	cfg := baseGridRiskConfig()
	cfg.ContractRisk.MaxAccountDrawdownPct = 10
	ex := safeFakeExchange()
	guard := NewGridRiskGuard(cfg, ex)
	if !guard.CheckEntryOrders(context.Background(), entryOrders(), 100, GridRiskSnapshot{}).Allowed {
		t.Fatalf("initial check should pass")
	}
	ex.account = &exchange.Account{TotalMarginBalance: 850, AvailableBalance: 800}
	decision := guard.CheckEntryOrders(context.Background(), entryOrders(), 100, GridRiskSnapshot{})
	if decision.Allowed {
		t.Fatalf("expected max_account_drawdown_pct to reject")
	}
}

func TestGridRiskGuardRejectsLiquidationGuardTooClose(t *testing.T) {
	cfg := baseGridRiskConfig()
	cfg.ContractRisk.LiquidationGuard.MinDistancePct = 5
	ex := safeFakeExchange()
	ex.positions[0].LiquidationPrice = 96
	guard := NewGridRiskGuard(cfg, ex)
	decision := guard.CheckEntryOrders(context.Background(), entryOrders(), 100, GridRiskSnapshot{})
	if decision.Allowed {
		t.Fatalf("expected liquidation_guard to reject")
	}
}

func TestGridRiskGuardAllowsReduceOnlyOrders(t *testing.T) {
	cfg := baseGridRiskConfig()
	cfg.ContractRisk.MaxTotalNotional = 1
	guard := NewGridRiskGuard(cfg, safeFakeExchange())
	decision := guard.CheckEntryOrders(context.Background(), []GridRiskOrder{{Symbol: "BTCUSDT", Side: "SELL", Price: 100, Quantity: 1, ReduceOnly: true}}, 100, GridRiskSnapshot{TotalNotional: 1000})
	if !decision.Allowed {
		t.Fatalf("expected reduce-only order to pass, got reasons=%v", decision.Reasons)
	}
}

func TestGridRiskGuardAllowsWhenDisabled(t *testing.T) {
	cfg := baseGridRiskConfig()
	cfg.ContractRisk.Enabled = false
	cfg.ContractRisk.MaxTotalNotional = 1
	guard := NewGridRiskGuard(cfg, safeFakeExchange())
	decision := guard.CheckEntryOrders(context.Background(), entryOrders(), 100, GridRiskSnapshot{TotalNotional: 1000})
	if !decision.Allowed {
		t.Fatalf("expected disabled contract_risk to allow, got reasons=%v", decision.Reasons)
	}
}
