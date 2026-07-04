package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	DefaultFeeRate        = 0.0002 // 0.02%
	ClassicGridWindowSize = 50
)

// Config 做市商系统配置
type Config struct {
	// 应用配置
	App struct {
		CurrentExchange string `yaml:"current_exchange" json:"current_exchange"` // 当前使用的交易所
		MarketType      string `yaml:"market_type" json:"market_type"`           // 市场类型: futures(合约，默认) / spot(现货)
	} `yaml:"app" json:"app"`

	// 多交易所配置
	Exchanges map[string]ExchangeConfig `yaml:"exchanges" json:"exchanges"`

	Trading struct {
		Mode                  string  `yaml:"mode" json:"mode"`
		Direction             string  `yaml:"direction" json:"direction"`
		Symbol                string  `yaml:"symbol" json:"symbol"`
		PriceInterval         float64 `yaml:"price_interval" json:"price_interval"`
		OrderQuantity         float64 `yaml:"order_quantity" json:"order_quantity"`   // 每单购买金额（USDT/USDC）
		MinOrderValue         float64 `yaml:"min_order_value" json:"min_order_value"` // 最小订单价值（USDT），默认20U，小于此值不挂单
		BuyWindowSize         int     `yaml:"buy_window_size" json:"buy_window_size"`
		SellWindowSize        int     `yaml:"sell_window_size" json:"sell_window_size"` // 卖单窗口大小
		ReconcileInterval     int     `yaml:"reconcile_interval" json:"reconcile_interval"`
		OrderCleanupThreshold int     `yaml:"order_cleanup_threshold" json:"order_cleanup_threshold"`           // 订单清理上限（默认100）
		CleanupBatchSize      int     `yaml:"cleanup_batch_size" json:"cleanup_batch_size"`                     // 清理批次大小（默认10）
		MarginLockDurationSec int     `yaml:"margin_lock_duration_seconds" json:"margin_lock_duration_seconds"` // 保证金锁定时间（秒，默认10）
		PositionSafetyCheck   int     `yaml:"position_safety_check" json:"position_safety_check"`               // 持仓安全性检查（默认100，最少能向下持有多少仓）
		OrderTag              string  `yaml:"order_tag,omitempty" json:"order_tag,omitempty"`                   // Web 控制台机器人订单归属标签
		// 注意：price_decimals 和 quantity_decimals 已废弃，现在从交易所自动获取
	} `yaml:"trading" json:"trading"`

	System struct {
		LogLevel     string `yaml:"log_level" json:"log_level"`
		CancelOnExit bool   `yaml:"cancel_on_exit" json:"cancel_on_exit"`
	} `yaml:"system" json:"system"`

	// 主动安全风控配置
	RiskControl struct {
		Enabled           bool     `yaml:"enabled" json:"enabled"`                       // 是否启用风控，默认true
		MonitorSymbols    []string `yaml:"monitor_symbols" json:"monitor_symbols"`       // 监控币种，如 ["BTCUSDT", "ETHUSDT"]
		Interval          string   `yaml:"interval" json:"interval"`                     // K线周期，如 "1m", "3m", "5m"
		VolumeMultiplier  float64  `yaml:"volume_multiplier" json:"volume_multiplier"`   // 成交量倍数阈值，默认3.0
		AverageWindow     int      `yaml:"average_window" json:"average_window"`         // 移动平均窗口大小，默认20
		RecoveryThreshold int      `yaml:"recovery_threshold" json:"recovery_threshold"` // 恢复交易所需的正常币种数量，默认3
	} `yaml:"risk_control" json:"risk_control"`

	// 合约网格硬风控配置（账户/仓位维度，不包含市场预测）
	ContractRisk struct {
		Enabled               bool    `yaml:"enabled" json:"enabled"`
		MaxTotalNotional      float64 `yaml:"max_total_notional" json:"max_total_notional"`
		MaxPositionLayers     int     `yaml:"max_position_layers" json:"max_position_layers"`
		MaxMarginUsagePct     float64 `yaml:"max_margin_usage_pct" json:"max_margin_usage_pct"`
		MaxUnrealizedLossPct  float64 `yaml:"max_unrealized_loss_pct" json:"max_unrealized_loss_pct"`
		MaxAccountDrawdownPct float64 `yaml:"max_account_drawdown_pct" json:"max_account_drawdown_pct"`
		LiquidationGuard      struct {
			Enabled        bool    `yaml:"enabled" json:"enabled"`
			MinDistancePct float64 `yaml:"min_distance_pct" json:"min_distance_pct"`
		} `yaml:"liquidation_guard" json:"liquidation_guard"`
	} `yaml:"contract_risk" json:"contract_risk"`

	// 时间间隔配置（单位：秒，除非特别说明）
	Timing struct {
		// WebSocket相关
		WebSocketReconnectDelay    int `yaml:"websocket_reconnect_delay" json:"websocket_reconnect_delay"`         // WebSocket断线重连等待时间（秒，默认5）
		WebSocketWriteWait         int `yaml:"websocket_write_wait" json:"websocket_write_wait"`                   // WebSocket写入等待时间（秒，默认10）
		WebSocketPongWait          int `yaml:"websocket_pong_wait" json:"websocket_pong_wait"`                     // WebSocket PONG等待时间（秒，默认60）
		WebSocketPingInterval      int `yaml:"websocket_ping_interval" json:"websocket_ping_interval"`             // WebSocket PING间隔（秒，默认20）
		ListenKeyKeepAliveInterval int `yaml:"listen_key_keepalive_interval" json:"listen_key_keepalive_interval"` // listenKey保活间隔（分钟，默认30）

		// 价格监控相关
		PriceSendInterval int `yaml:"price_send_interval" json:"price_send_interval"` // 定期发送价格的间隔（毫秒，默认50）

		// 订单执行相关
		RateLimitRetryDelay  int `yaml:"rate_limit_retry_delay" json:"rate_limit_retry_delay"` // 速率限制重试等待时间（秒，默认1）
		OrderRetryDelay      int `yaml:"order_retry_delay" json:"order_retry_delay"`           // 其他错误重试等待时间（毫秒，默认500）
		PricePollInterval    int `yaml:"price_poll_interval" json:"price_poll_interval"`       // 等待获取价格的轮询间隔（毫秒，默认500）
		StatusPrintInterval  int `yaml:"status_print_interval" json:"status_print_interval"`   // 定期打印状态的间隔（分钟，默认1）
		OrderCleanupInterval int `yaml:"order_cleanup_interval" json:"order_cleanup_interval"` // 订单清理检查间隔（秒，默认60）
	} `yaml:"timing" json:"timing"`
}

// ExchangeConfig 交易所配置
type ExchangeConfig struct {
	APIKey     string  `yaml:"api_key" json:"api_key"`
	SecretKey  string  `yaml:"secret_key" json:"secret_key"`
	Passphrase string  `yaml:"passphrase" json:"passphrase"` // Bitget / OKX 需要
	FeeRate    float64 `yaml:"fee_rate" json:"fee_rate"`     // 手续费率（例如 0.0002 表示 0.02%）
}

// TargetOrderCapacity 返回当前方向下策略满配时应允许存在的订单数量。
func TargetOrderCapacity(c *Config) int {
	if c == nil {
		return 0
	}
	buyWindowSize := c.Trading.BuyWindowSize
	if buyWindowSize < 0 {
		buyWindowSize = 0
	}
	sellWindowSize := c.Trading.SellWindowSize
	if sellWindowSize <= 0 {
		sellWindowSize = buyWindowSize
	}
	baseCapacity := buyWindowSize + sellWindowSize
	if strings.EqualFold(strings.TrimSpace(c.Trading.Mode), "classic") {
		// 经典网格是固定 50 买 / 50 卖的一百单盘口，不按
		// neutral 的开/平仓双层预算翻倍。
		return ClassicGridWindowSize * 2
	}
	if strings.EqualFold(strings.TrimSpace(c.App.MarketType), "spot") {
		return baseCapacity
	}
	if strings.EqualFold(strings.TrimSpace(c.Trading.Direction), "neutral") {
		return baseCapacity * 2
	}
	return baseCapacity
}

// LoadConfig 加载配置文件
func LoadConfig(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %v", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %v", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("配置验证失败: %v", err)
	}

	return &cfg, nil
}

// SaveConfig 保存配置文件
func SaveConfig(configPath string, cfg *Config) error {
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("序列化配置失败: %v", err)
	}

	if err := os.WriteFile(configPath, data, 0600); err != nil {
		return fmt.Errorf("写入配置文件失败: %v", err)
	}

	return nil
}

func normalizeMarketType(marketType string) string {
	switch strings.ToLower(strings.TrimSpace(marketType)) {
	case "", "future", "futures", "contract", "contracts", "swap", "perp", "perpetual":
		return "futures"
	case "spot":
		return "spot"
	default:
		return ""
	}
}

// NormalizeExchangeName maps common user-facing exchange spellings to the
// internal configuration key used across adapters and persisted robot files.
func NormalizeExchangeName(exchangeName string) string {
	name := strings.ToLower(strings.TrimSpace(exchangeName))
	name = strings.ReplaceAll(name, " ", "")
	name = strings.ReplaceAll(name, "-", "")
	name = strings.ReplaceAll(name, "_", "")
	name = strings.ReplaceAll(name, ".", "")
	switch name {
	case "binance":
		return "binance"
	case "bitget":
		return "bitget"
	case "bybit":
		return "bybit"
	case "gate", "gateio":
		return "gate"
	case "okx", "okex":
		return "okx"
	case "hyperliquid", "hyperliquidex":
		return "hyperliquid"
	default:
		return ""
	}
}

func (c *Config) normalizeExchangeConfigs() error {
	if c.Exchanges == nil {
		return nil
	}
	normalized := make(map[string]ExchangeConfig, len(c.Exchanges))
	for name, exchangeCfg := range c.Exchanges {
		key := NormalizeExchangeName(name)
		if key == "" {
			normalized[strings.TrimSpace(name)] = exchangeCfg
			continue
		}
		if _, exists := normalized[key]; exists {
			return fmt.Errorf("交易所 %s 配置重复，请只保留一个配置项", key)
		}
		normalized[key] = exchangeCfg
	}
	c.Exchanges = normalized
	return nil
}

// Validate 验证配置
func (c *Config) Validate() error {
	// 验证交易所配置
	if c.App.CurrentExchange == "" {
		return fmt.Errorf("必须指定当前使用的交易所 (app.current_exchange)")
	}
	rawExchangeName := c.App.CurrentExchange
	c.App.CurrentExchange = NormalizeExchangeName(rawExchangeName)
	if c.App.CurrentExchange == "" {
		return fmt.Errorf("不支持的交易所: %s", rawExchangeName)
	}
	c.App.MarketType = normalizeMarketType(c.App.MarketType)
	if c.App.MarketType == "" {
		return fmt.Errorf("app.market_type 仅支持 futures 或 spot")
	}

	// 验证多交易所配置
	if len(c.Exchanges) == 0 {
		return fmt.Errorf("未配置任何交易所，请在 exchanges 中添加配置")
	}
	if err := c.normalizeExchangeConfigs(); err != nil {
		return err
	}

	exchangeCfg, exists := c.Exchanges[c.App.CurrentExchange]
	if !exists {
		return fmt.Errorf("交易所 %s 的配置不存在", c.App.CurrentExchange)
	}

	if exchangeCfg.APIKey == "" || exchangeCfg.SecretKey == "" {
		return fmt.Errorf("交易所 %s 的 API 配置不完整", c.App.CurrentExchange)
	}

	// 验证手续费率配置
	if exchangeCfg.FeeRate < 0 {
		return fmt.Errorf("交易所 %s 的手续费率不能为负数", c.App.CurrentExchange)
	}
	for name, exchangeCfg := range c.Exchanges {
		if exchangeCfg.FeeRate == 0 {
			exchangeCfg.FeeRate = DefaultFeeRate
			c.Exchanges[name] = exchangeCfg
		}
	}

	if c.Trading.Mode == "" {
		c.Trading.Mode = "normal"
	}
	c.Trading.Mode = strings.ToLower(strings.TrimSpace(c.Trading.Mode))
	switch c.Trading.Mode {
	case "normal", "aggressive", "classic":
	default:
		return fmt.Errorf("trading.mode 仅支持 normal、aggressive、classic")
	}
	if c.Trading.Mode == "classic" {
		c.App.MarketType = "futures"
		c.Trading.Direction = "neutral"
		c.Trading.BuyWindowSize = ClassicGridWindowSize
		c.Trading.SellWindowSize = ClassicGridWindowSize
		if strings.EqualFold(strings.TrimSpace(c.App.CurrentExchange), "hyperliquid") {
			return fmt.Errorf("classic 经典网格需要 neutral 双向持仓，Hyperliquid 合约暂不支持该模式")
		}
	}
	if c.Trading.Symbol == "" {
		return fmt.Errorf("交易对不能为空")
	}
	if c.Trading.Direction == "" {
		c.Trading.Direction = "long"
	}
	c.Trading.Direction = strings.ToLower(strings.TrimSpace(c.Trading.Direction))
	switch c.Trading.Direction {
	case "long", "short", "neutral":
	default:
		return fmt.Errorf("trading.direction 仅支持 long、short、neutral")
	}
	if c.App.MarketType == "spot" && c.Trading.Direction != "long" {
		return fmt.Errorf("现货模式仅支持 long（先买入、再卖出）")
	}
	if c.Trading.OrderQuantity <= 0 {
		return fmt.Errorf("订单金额必须大于0")
	}
	if c.Trading.PriceInterval <= 0 {
		return fmt.Errorf("价格间隔必须大于0")
	}
	if c.Trading.BuyWindowSize <= 0 {
		return fmt.Errorf("买单窗口大小必须大于0")
	}
	if c.Trading.SellWindowSize <= 0 {
		c.Trading.SellWindowSize = c.Trading.BuyWindowSize // 默认与买单窗口相同
	}
	if c.Trading.CleanupBatchSize <= 0 {
		c.Trading.CleanupBatchSize = 10 // 默认10
	}
	targetOrderCapacity := TargetOrderCapacity(c)
	if c.Trading.OrderCleanupThreshold <= 0 || c.Trading.OrderCleanupThreshold < targetOrderCapacity {
		c.Trading.OrderCleanupThreshold = targetOrderCapacity
	}
	// 注意：price_decimals 和 quantity_decimals 已从配置中移除，现在从交易所自动获取
	if c.Trading.MinOrderValue <= 0 {
		c.Trading.MinOrderValue = 20.0
	}

	// 设置默认时间间隔
	if c.Timing.WebSocketReconnectDelay <= 0 {
		c.Timing.WebSocketReconnectDelay = 5 // 默认5秒
	}
	if c.Timing.WebSocketWriteWait <= 0 {
		c.Timing.WebSocketWriteWait = 10 // 默认10秒
	}
	if c.Timing.WebSocketPongWait <= 0 {
		c.Timing.WebSocketPongWait = 60 // 默认60秒
	}
	if c.Timing.WebSocketPingInterval <= 0 {
		c.Timing.WebSocketPingInterval = 20 // 默认20秒
	}
	if c.Timing.ListenKeyKeepAliveInterval <= 0 {
		c.Timing.ListenKeyKeepAliveInterval = 30 // 默认30分钟
	}
	if c.Timing.PriceSendInterval <= 0 {
		c.Timing.PriceSendInterval = 50 // 默认50毫秒
	}
	if c.Timing.RateLimitRetryDelay <= 0 {
		c.Timing.RateLimitRetryDelay = 1 // 默认1秒
	}
	if c.Timing.OrderRetryDelay <= 0 {
		c.Timing.OrderRetryDelay = 500 // 默认500毫秒
	}
	if c.Timing.PricePollInterval <= 0 {
		c.Timing.PricePollInterval = 500 // 默认500毫秒
	}
	if c.Timing.StatusPrintInterval <= 0 {
		c.Timing.StatusPrintInterval = 1 // 默认1分钟
	}
	if c.Timing.OrderCleanupInterval <= 0 {
		c.Timing.OrderCleanupInterval = 60 // 默认60秒
	}

	// 验证风控配置并设置默认值
	if c.RiskControl.Interval == "" {
		c.RiskControl.Interval = "1m" // 默认1分钟
	}
	if c.RiskControl.VolumeMultiplier <= 0 {
		c.RiskControl.VolumeMultiplier = 3.0 // 默认3倍
	}
	if c.RiskControl.AverageWindow <= 0 {
		c.RiskControl.AverageWindow = 20 // 默认20根K线
	}
	if len(c.RiskControl.MonitorSymbols) == 0 {
		c.RiskControl.MonitorSymbols = []string{"BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT"}
	}

	// 验证恢复阈值配置
	monitorCount := len(c.RiskControl.MonitorSymbols)
	if c.RiskControl.RecoveryThreshold <= 0 {
		c.RiskControl.RecoveryThreshold = 3 // 默认3个币种
	}
	if c.RiskControl.RecoveryThreshold < 1 {
		c.RiskControl.RecoveryThreshold = 1 // 最小1个
	}
	if c.RiskControl.RecoveryThreshold > monitorCount {
		c.RiskControl.RecoveryThreshold = monitorCount // 最大为监控币种数量
	}

	// 合约网格硬风控默认值。enabled 需要显式开启；max_total_notional 和
	// max_position_layers 保持 0=不限制，避免老配置升级后被隐式窗口推导过度限制。
	if c.ContractRisk.MaxTotalNotional < 0 {
		return fmt.Errorf("contract_risk.max_total_notional 不能为负数")
	}
	if c.ContractRisk.MaxPositionLayers < 0 {
		return fmt.Errorf("contract_risk.max_position_layers 不能为负数")
	}
	if c.ContractRisk.MaxMarginUsagePct <= 0 {
		c.ContractRisk.MaxMarginUsagePct = 70
	}
	if c.ContractRisk.MaxUnrealizedLossPct <= 0 {
		c.ContractRisk.MaxUnrealizedLossPct = 20
	}
	if c.ContractRisk.MaxAccountDrawdownPct <= 0 {
		c.ContractRisk.MaxAccountDrawdownPct = 25
	}
	if c.ContractRisk.MaxMarginUsagePct > 100 {
		return fmt.Errorf("contract_risk.max_margin_usage_pct 不能大于100")
	}
	if c.ContractRisk.MaxUnrealizedLossPct > 100 {
		return fmt.Errorf("contract_risk.max_unrealized_loss_pct 不能大于100")
	}
	if c.ContractRisk.MaxAccountDrawdownPct > 100 {
		return fmt.Errorf("contract_risk.max_account_drawdown_pct 不能大于100")
	}
	if c.ContractRisk.LiquidationGuard.MinDistancePct <= 0 {
		c.ContractRisk.LiquidationGuard.MinDistancePct = 5
	}
	if c.ContractRisk.LiquidationGuard.MinDistancePct > 100 {
		return fmt.Errorf("contract_risk.liquidation_guard.min_distance_pct 不能大于100")
	}

	return nil
}
