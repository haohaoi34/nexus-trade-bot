package gate

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"nexus-trade-bot/logger"
	"nexus-trade-bot/utils"
)

type OrderUpdateCallback func(update OrderUpdate)

// GateAdapter Gate.io 交易所适配器
type GateAdapter struct {
	client         *Client
	wsManager      *WebSocketManager
	klineWSManager *KlineWebSocketManager
	symbol         string // 交易对（如 BTCUSDT）
	gateSymbol     string // Gate格式（如 BTC_USDT）
	settle         string // 结算币种：usdt 或 btc
	useWebSocket   bool   // 是否使用 WebSocket 下单

	// 订单ID到价格的映射注册回调
	orderMappingCallback func(orderID int64, price float64)

	posMode          string  // 持仓模式：dual_long_short 或 single
	quantoMultiplier float64 // 合约乘数
	orderPriceRound  int     // 价格精度
	orderSizeMin     float64 // 最小下单数量
	volumePlace      int     // 数量小数位
	pricePlace       int     // 价格小数位

	priceCacheMu   sync.RWMutex
	priceCache     float64
	priceCacheTime time.Time
}

// NewGateAdapter 创建 Gate.io 适配器
func NewGateAdapter(cfg map[string]string, symbol string) (*GateAdapter, error) {
	apiKey := cfg["api_key"]
	secretKey := cfg["secret_key"]
	settle := cfg["settle"] // usdt 或 btc，默认 usdt

	if apiKey == "" || secretKey == "" {
		return nil, fmt.Errorf("Gate.io API 配置不完整")
	}

	if settle == "" {
		settle = "usdt" // 默认 USDT 永续合约
	}

	// 转换交易对格式
	gateSymbol := convertToGateSymbol(symbol)

	client := NewClient(apiKey, secretKey)
	wsManager := NewWebSocketManager(apiKey, secretKey, settle)

	adapter := &GateAdapter{
		client:       client,
		wsManager:    wsManager,
		symbol:       symbol,
		gateSymbol:   gateSymbol,
		settle:       settle,
		useWebSocket: false, // 默认使用 REST API 下单
	}

	// 初始化获取合约信息和持仓模式
	ctxInit, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. 获取合约信息
	if err := adapter.fetchContractInfo(ctxInit); err != nil {
		logger.Warn("⚠️ [Gate] 获取合约信息失败: %v", err)
		// 使用默认值
		adapter.volumePlace = 0
		adapter.pricePlace = 2
		adapter.orderSizeMin = 1
	}

	// 2. 获取账户信息（判断持仓模式）
	acc, err := adapter.GetAccount(ctxInit)
	if err != nil {
		logger.Warn("⚠️ [Gate] 初始化获取账户信息失败: %v", err)
		adapter.posMode = "dual_long_short" // 默认双向持仓
	} else {
		if acc.PosMode == "dual_long_short" {
			adapter.posMode = "dual_long_short"
		} else {
			adapter.posMode = "single"
		}

		posModeDesc := "双向持仓"
		if adapter.posMode == "single" {
			posModeDesc = "单向持仓"
		}
		logger.Debug("ℹ️ [Gate] 持仓模式: %s (%s)", posModeDesc, adapter.posMode)
	}

	return adapter, nil
}

// GetName 获取交易所名称
func (g *GateAdapter) GetName() string {
	return "Gate.io"
}

func (g *GateAdapter) ValidatePositionMode(ctx context.Context, direction string) error {
	account, err := g.GetAccount(ctx)
	if err != nil {
		return fmt.Errorf("获取 Gate 持仓模式失败: %w", err)
	}
	g.posMode = account.PosMode
	if strings.EqualFold(strings.TrimSpace(direction), "neutral") && account.PosMode != "dual_long_short" {
		return fmt.Errorf("Gate 中性模式需要双向持仓 dual_long_short，当前为 %s，请切换双向持仓后再启动", account.PosMode)
	}
	return nil
}

// GetPriceDecimals 获取价格精度
func (g *GateAdapter) GetPriceDecimals() int {
	return g.pricePlace
}

// GetQuantityDecimals 获取数量精度
func (g *GateAdapter) GetQuantityDecimals() int {
	return g.volumePlace
}

func (g *GateAdapter) GetBaseAsset() string {
	base, _ := splitGateSymbol(g.gateSymbol)
	return base
}

func (g *GateAdapter) GetQuoteAsset() string {
	_, quote := splitGateSymbol(g.gateSymbol)
	if quote == "" {
		return strings.ToUpper(g.settle)
	}
	return quote
}

// fetchContractInfo 获取合约信息
func (g *GateAdapter) fetchContractInfo(ctx context.Context) error {
	contract, err := g.client.GetContract(ctx, g.settle, g.gateSymbol)
	if err != nil {
		return fmt.Errorf("获取合约信息失败: %w", err)
	}

	// 解析合约乘数
	if contract.QuantoMultiplier != "" {
		g.quantoMultiplier, _ = strconv.ParseFloat(contract.QuantoMultiplier, 64)
	}

	// 解析价格精度（如 "0.1" -> 1位小数）
	if contract.OrderPriceRound != "" {
		priceRound, _ := strconv.ParseFloat(contract.OrderPriceRound, 64)
		g.pricePlace = calculateDecimalPlaces(priceRound)
	}

	// 解析数量精度
	// Gate.io 的 order_size_round 字段可能为空,需要推断精度
	if contract.OrderSizeRound != "" {
		sizeRound, _ := strconv.ParseFloat(contract.OrderSizeRound, 64)
		g.volumePlace = calculateDecimalPlaces(sizeRound)
	} else {
		// 如果 order_size_round 为空,根据 order_size_min 推断
		// 对于 USDT 永续合约,通常支持小数下单
		// ETH_USDT 等主流币种一般支持 0.01 精度(2位小数)
		minSize := contract.OrderSizeMin
		if minSize >= 1 {
			// 最小量 >= 1,通常是整数合约(如 BTC)
			// 但也可能支持小数,使用 0.01 精度较安全
			g.volumePlace = 2 // 默认2位小数
		} else {
			// 最小量 < 1,根据最小量计算精度
			g.volumePlace = calculateDecimalPlaces(minSize)
		}
	}

	// 最小下单数量
	g.orderSizeMin = contract.OrderSizeMin

	// 计算实际最小下单量(张数 × 乘数 = 实际币数量)
	actualMinSize := g.orderSizeMin * g.quantoMultiplier
	if actualMinSize == 0 {
		actualMinSize = g.orderSizeMin // 如果乘数为0,直接用张数
	}

	logger.Debug("ℹ️ [Gate 合约信息] %s, 每张合约:%.2f, 价格精度:%d, 数量精度:%d, 最小下单量:%.2f (%.0f张)",
		g.gateSymbol, g.quantoMultiplier, g.pricePlace, g.volumePlace, actualMinSize, g.orderSizeMin)

	return nil
}

// PlaceOrder 下单
func (g *GateAdapter) PlaceOrder(ctx context.Context, req *OrderRequest) (*Order, error) {
	// 使用 REST API 下单（更可靠）
	return g.placeOrderViaREST(ctx, req)
}

// placeOrderViaREST 通过 REST API 下单
func (g *GateAdapter) placeOrderViaREST(ctx context.Context, req *OrderRequest) (*Order, error) {
	// Gate.io 的 size 是张数,需要从实际币数量换算
	// 如果合约乘数为 0,则直接使用数量
	contractSize := g.contractsFromBaseQuantity(req.Quantity)
	if contractSize == 0 {
		return nil, fmt.Errorf("订单数量 %.8f 低于 Gate 合约最小张数，拒绝自动放大到 1 张", req.Quantity)
	}

	// 转换方向和数量: Gate.io 使用正负数表示方向
	// BUY(买入) = 正数, SELL(卖出) = 负数
	// reduce_only参数会告诉交易所这是平仓单,不需要反转符号
	var size int64
	if req.Side == SideBuy {
		size = contractSize
	} else {
		size = -contractSize
	}

	// 格式化价格
	priceStr := fmt.Sprintf("%.*f", g.pricePlace, req.Price)

	// Gate.io 要求 text 字段必须以 "t-" 开头,且长度不超过30个字符
	// 使用统一的 utils 包添加返佣前缀（会自动处理长度限制）
	clientOrderID := req.ClientOrderID
	if clientOrderID != "" {
		clientOrderID = utils.AddBrokerPrefix("gate", clientOrderID)
	}

	// 构造订单参数
	order := map[string]interface{}{
		"contract": g.gateSymbol,
		"size":     size,
		"price":    priceStr,
		"tif":      "gtc", // Good Till Cancel
		"text":     clientOrderID,
	}

	// 只减仓标记 (Gate.io 使用 reduce_only,不需要 close 标记)
	if req.ReduceOnly {
		order["reduce_only"] = true
	}

	// 只做 Maker
	if req.PostOnly {
		order["tif"] = "poc" // Post Only
	}

	// 发送下单请求
	futuresOrder, err := g.client.PlaceOrder(ctx, g.settle, order)
	if err != nil {
		// 检查是否保证金不足
		if strings.Contains(err.Error(), "insufficient") || strings.Contains(err.Error(), "balance") {
			return nil, fmt.Errorf("保证金不足: %w", err)
		}
		return nil, err
	}

	// 转换为标准订单格式
	result := &Order{
		OrderID:       futuresOrder.ID,
		ClientOrderID: futuresOrder.Text,
		Symbol:        g.symbol,
		Side:          convertSide(float64(futuresOrder.Size)),
		Type:          OrderTypeLimit,
		Price:         req.Price,
		Quantity:      g.baseQuantityFromContracts(futuresOrder.Size),
		ExecutedQty:   g.baseQuantityFromContracts(futuresOrder.FillSize),
		Status:        convertStatus(futuresOrder.Status, futuresOrder.FinishAs, g.baseQuantityFromContracts(futuresOrder.FillSize), g.baseQuantityFromContracts(futuresOrder.Size)),
		CreatedAt:     time.Unix(int64(futuresOrder.CreateTime), 0),
		UpdateTime:    int64(futuresOrder.FinishTime * 1000),
	}

	// 解析成交均价
	if futuresOrder.FillPrice != "" {
		result.AvgPrice, _ = strconv.ParseFloat(futuresOrder.FillPrice, 64)
	}

	return result, nil
}

// BatchPlaceOrders 批量下单
func (g *GateAdapter) BatchPlaceOrders(ctx context.Context, orders []*OrderRequest) ([]*Order, bool) {
	placedOrders := make([]*Order, 0, len(orders))
	hasMarginError := false

	for _, orderReq := range orders {
		order, err := g.PlaceOrder(ctx, orderReq)
		if err != nil {
			logger.Warn("⚠️ [Gate] 下单失败 %.2f %s: %v",
				orderReq.Price, orderReq.Side, err)

			if strings.Contains(err.Error(), "保证金不足") {
				hasMarginError = true
			}
			continue
		}

		// 确保包含请求的价格
		order.Price = orderReq.Price

		// 注册订单ID到价格的映射
		if g.orderMappingCallback != nil && order.OrderID > 0 {
			g.orderMappingCallback(order.OrderID, orderReq.Price)
			logger.Debug("🔍 [Gate映射] 注册 订单ID=%d -> 价格=%.2f", order.OrderID, orderReq.Price)
		}

		placedOrders = append(placedOrders, order)
	}

	return placedOrders, hasMarginError
}

// CancelOrder 取消订单
func (g *GateAdapter) CancelOrder(ctx context.Context, symbol string, orderID int64) error {
	orderIDStr := strconv.FormatInt(orderID, 10)
	_, err := g.client.CancelOrder(ctx, g.settle, orderIDStr)
	if err != nil {
		// 订单不存在不算错误
		if strings.Contains(err.Error(), "ORDER_NOT_FOUND") || strings.Contains(err.Error(), "not found") {
			logger.Info("ℹ️ [Gate] 订单 %d 已不存在，跳过取消", orderID)
			return nil
		}
		return fmt.Errorf("取消订单失败: %w", err)
	}

	logger.Info("✅ [Gate] 取消订单成功: %d", orderID)
	return nil
}

// BatchCancelOrders 批量取消订单
func (g *GateAdapter) BatchCancelOrders(ctx context.Context, symbol string, orderIDs []int64) error {
	if len(orderIDs) == 0 {
		return nil
	}

	var failed []string

	// Gate.io 批量撤单API一次最多20个
	for i := 0; i < len(orderIDs); i += 20 {
		end := i + 20
		if end > len(orderIDs) {
			end = len(orderIDs)
		}

		batch := orderIDs[i:end]
		orderIDStrs := make([]string, len(batch))
		for j, id := range batch {
			orderIDStrs[j] = strconv.FormatInt(id, 10)
		}

		results, err := g.client.BatchCancelOrders(ctx, g.settle, orderIDStrs)
		if err != nil {
			logger.Warn("⚠️ [Gate] 批量撤单请求失败: %v", err)
			logger.Info("🔄 [Gate] 改为逐个撤单...")
			for _, orderID := range batch {
				if err := g.CancelOrder(ctx, symbol, orderID); err != nil {
					failed = append(failed, fmt.Sprintf("%d: %v", orderID, err))
				}
				time.Sleep(100 * time.Millisecond)
			}
			continue
		}

		// 处理结果并统计
		successCount := 0
		notFoundCount := 0
		failCount := 0

		for _, result := range results {
			orderID, _ := result["id"].(string)
			succeeded, _ := result["succeeded"].(bool)
			message, _ := result["message"].(string)

			if succeeded {
				successCount++
				logger.Info("✅ [Gate] 取消订单成功: %s", orderID)
			} else if strings.Contains(message, "not found") || strings.Contains(message, "ORDER_NOT_FOUND") {
				notFoundCount++
				logger.Debug("ℹ️ [Gate] 订单 %s 已不存在(可能已成交/已撤销)", orderID)
			} else {
				failCount++
				logger.Warn("⚠️ [Gate] 取消订单失败 %s: %s", orderID, message)
				failed = append(failed, fmt.Sprintf("%s: %s", orderID, message))
			}
		}

		// 批次汇总
		if len(batch) > 0 {
			logger.Info("📊 [Gate] 批次撤单: 成功%d个, 已不存在%d个, 失败%d个", successCount, notFoundCount, failCount)
		}

		// 批次间延迟
		if end < len(orderIDs) {
			time.Sleep(100 * time.Millisecond)
		}
	}

	if len(failed) > 0 {
		return fmt.Errorf("Gate 批量撤单后仍有 %d 个订单撤销失败: %s", len(failed), strings.Join(failed, "; "))
	}
	return nil
}

// GetOrder 查询订单
func (g *GateAdapter) GetOrder(ctx context.Context, symbol string, orderID int64) (*Order, error) {
	orderIDStr := strconv.FormatInt(orderID, 10)
	futuresOrder, err := g.client.GetOrder(ctx, g.settle, orderIDStr)
	if err != nil {
		return nil, err
	}

	// 转换为标准格式
	order := &Order{
		OrderID:       futuresOrder.ID,
		ClientOrderID: futuresOrder.Text,
		Symbol:        g.symbol,
		Side:          convertSide(float64(futuresOrder.Size)),
		Type:          OrderTypeLimit,
		Quantity:      g.baseQuantityFromContracts(futuresOrder.Size),
		ExecutedQty:   g.baseQuantityFromContracts(futuresOrder.FillSize),
		Status:        convertStatus(futuresOrder.Status, futuresOrder.FinishAs, g.baseQuantityFromContracts(futuresOrder.FillSize), g.baseQuantityFromContracts(futuresOrder.Size)),
		CreatedAt:     time.Unix(int64(futuresOrder.CreateTime), 0),
		UpdateTime:    int64(futuresOrder.FinishTime * 1000),
	}

	// 解析价格
	if futuresOrder.Price != "" {
		order.Price, _ = strconv.ParseFloat(futuresOrder.Price, 64)
	}

	// 解析成交均价
	if futuresOrder.FillPrice != "" {
		order.AvgPrice, _ = strconv.ParseFloat(futuresOrder.FillPrice, 64)
	}

	return order, nil
}

// GetOpenOrders 查询未完成订单
func (g *GateAdapter) GetOpenOrders(ctx context.Context, symbol string) ([]*Order, error) {
	futuresOrders, err := g.client.GetOpenOrders(ctx, g.settle, g.gateSymbol)
	if err != nil {
		return nil, err
	}

	orders := make([]*Order, 0, len(futuresOrders))
	for _, fo := range futuresOrders {
		order := &Order{
			OrderID:       fo.ID,
			ClientOrderID: fo.Text,
			Symbol:        g.symbol,
			Side:          convertSide(float64(fo.Size)),
			Type:          OrderTypeLimit,
			Quantity:      g.baseQuantityFromContracts(fo.Size),
			ExecutedQty:   g.baseQuantityFromContracts(fo.FillSize),
			Status:        convertStatus(fo.Status, fo.FinishAs, g.baseQuantityFromContracts(fo.FillSize), g.baseQuantityFromContracts(fo.Size)),
			CreatedAt:     time.Unix(int64(fo.CreateTime), 0),
			UpdateTime:    int64(fo.FinishTime * 1000),
		}

		// 解析价格
		if fo.Price != "" {
			order.Price, _ = strconv.ParseFloat(fo.Price, 64)
		}

		// 解析成交均价
		if fo.FillPrice != "" {
			order.AvgPrice, _ = strconv.ParseFloat(fo.FillPrice, 64)
		}

		orders = append(orders, order)
	}

	return orders, nil
}

// GetAccount 获取账户信息
func (g *GateAdapter) GetAccount(ctx context.Context) (*Account, error) {
	futuresAcc, err := g.client.GetAccount(ctx, g.settle)
	if err != nil {
		return nil, err
	}

	// 解析余额
	total, _ := strconv.ParseFloat(futuresAcc.Total, 64)
	available, _ := strconv.ParseFloat(futuresAcc.Available, 64)
	unrealisedPnl, _ := strconv.ParseFloat(futuresAcc.UnrealisedPnl, 64)

	posMode := "single"
	if futuresAcc.InDualMode {
		posMode = "dual_long_short"
	}

	// 获取当前合约的杠杆设置
	leverage := 1 // 默认1倍
	if fp, err := g.client.GetPosition(ctx, g.settle, g.gateSymbol); err == nil {
		// 检查是否为逐仓模式
		leverageValue, _ := strconv.Atoi(fp.Leverage)
		if leverageValue != 0 {
			// 逐仓模式
			leverage = leverageValue
			logger.Warn("⚠️ [Gate] 当前为逐仓模式(杠杆倍数=%dx),本系统仅支持全仓模式。请在 Gate.io 网站将持仓模式改为全仓", leverage)
		} else {
			// 全仓模式,从 CrossLeverageLimit 获取
			crossLeverage, _ := strconv.Atoi(fp.CrossLeverageLimit)
			if crossLeverage > 0 {
				leverage = crossLeverage
			}
		}
	}

	account := &Account{
		TotalWalletBalance: total,
		AvailableBalance:   available,
		TotalMarginBalance: total + unrealisedPnl,
		AccountLeverage:    leverage,
		PosMode:            posMode,
	}

	return account, nil
}

// GetPositions 获取持仓信息
func (g *GateAdapter) GetPositions(ctx context.Context, symbol string) ([]*Position, error) {
	// 使用单个持仓查询接口获取更详细的信息
	fp, err := g.client.GetPosition(ctx, g.settle, g.gateSymbol)
	if err != nil {
		return nil, err
	}

	positions := make([]*Position, 0)

	// 跳过空仓
	if fp.Size == 0 {
		return positions, nil
	}

	// 检查是否为逐仓模式
	leverage, _ := strconv.Atoi(fp.Leverage)
	if leverage != 0 {
		logger.Warn("⚠️ [Gate] 当前为逐仓模式(杠杆倍数=%dx),本系统仅支持全仓模式。请在 Gate.io 网站将持仓模式改为全仓", leverage)
		return nil, fmt.Errorf("不支持逐仓模式,请改为全仓模式")
	}

	// 全仓模式下,从 CrossLeverageLimit 获取杠杆倍数
	crossLeverage, _ := strconv.Atoi(fp.CrossLeverageLimit)
	if crossLeverage == 0 {
		crossLeverage = 1 // 默认1倍
	}

	entryPrice, _ := strconv.ParseFloat(fp.EntryPrice, 64)
	markPrice, _ := strconv.ParseFloat(fp.MarkPrice, 64)
	unrealisedPnl, _ := strconv.ParseFloat(fp.UnrealisedPnl, 64)
	realisedPnl, _ := strconv.ParseFloat(fp.RealisedPnl, 64)
	liqPrice, _ := strconv.ParseFloat(fp.LiqPrice, 64)

	position := &Position{
		Symbol:              g.symbol,
		Size:                g.signedBaseQuantityFromContracts(fp.Size),
		EntryPrice:          entryPrice,
		MarkPrice:           markPrice,
		UnrealizedPNL:       unrealisedPnl,
		HasUnrealizedPNL:    true,
		RealizedPNL:         realisedPnl,
		HasRealizedPNL:      fp.RealisedPnl != "",
		ClosedPNL:           realisedPnl,
		Leverage:            crossLeverage,
		MarginType:          "crossed", // 全仓模式
		LiquidationPrice:    liqPrice,
		HasLiquidationPrice: liqPrice > 0,
	}

	positions = append(positions, position)

	return positions, nil
}

// GetBalance 获取余额
func (g *GateAdapter) GetBalance(ctx context.Context, asset string) (float64, error) {
	acc, err := g.GetAccount(ctx)
	if err != nil {
		return 0, err
	}
	return acc.AvailableBalance, nil
}

// StartOrderStream 启动订单流
func (g *GateAdapter) StartOrderStream(ctx context.Context, callback func(interface{})) error {
	// 包装回调函数,将合约张数转换为币数量
	wrappedCallback := func(update interface{}) {
		if orderUpdate, ok := update.(OrderUpdate); ok {
			// Gate.io返回的是合约张数,需要乘以quanto_multiplier转换为币数量
			if g.quantoMultiplier > 0 {
				orderUpdate.Quantity = orderUpdate.Quantity * g.quantoMultiplier
				orderUpdate.ExecutedQty = orderUpdate.ExecutedQty * g.quantoMultiplier
			}
			callback(orderUpdate)
		} else {
			callback(update)
		}
	}

	g.wsManager.SetOrderCallback(wrappedCallback)

	// 如果 WebSocket 未运行，则启动
	if !g.wsManager.IsRunning() {
		return g.wsManager.Start(ctx, g.symbol)
	}

	return nil
}

// StopOrderStream 停止订单流
func (g *GateAdapter) StopOrderStream() error {
	return g.wsManager.Stop()
}

// StartPriceStream 启动价格流
func (g *GateAdapter) StartPriceStream(ctx context.Context, callback func(string, float64)) error {
	g.wsManager.SetPriceCallback(callback)

	// 如果 WebSocket 未运行，则启动
	if !g.wsManager.IsRunning() {
		return g.wsManager.Start(ctx, g.symbol)
	}

	return nil
}

// GetLatestPrice 获取最新价格
func (g *GateAdapter) GetLatestPrice(ctx context.Context, symbol string) (float64, error) {
	// 优先从 WebSocket 缓存获取
	price := g.wsManager.GetLatestPrice()
	if price > 0 {
		return price, nil
	}

	// 降级：使用 REST API 查询（这里需要实现 ticker 接口）
	// 暂时返回缓存价格
	g.priceCacheMu.RLock()
	if time.Since(g.priceCacheTime) < 5*time.Second && g.priceCache > 0 {
		g.priceCacheMu.RUnlock()
		return g.priceCache, nil
	}
	g.priceCacheMu.RUnlock()

	path := fmt.Sprintf("/futures/%s/tickers", g.settle)
	query := fmt.Sprintf("contract=%s", convertToGateSymbol(symbol))
	body, err := g.client.DoRequest(ctx, "GET", path, query, nil)
	if err != nil {
		return 0, fmt.Errorf("WebSocket 价格流未就绪，REST ticker 也失败: %w", err)
	}
	var items []struct {
		Last      string `json:"last"`
		MarkPrice string `json:"mark_price"`
	}
	if err := json.Unmarshal(body, &items); err != nil {
		return 0, fmt.Errorf("解析 Gate ticker 失败: %w", err)
	}
	if len(items) == 0 {
		return 0, fmt.Errorf("Gate ticker 为空")
	}
	priceText := items[0].Last
	if priceText == "" {
		priceText = items[0].MarkPrice
	}
	price, err = strconv.ParseFloat(priceText, 64)
	if err != nil || price <= 0 {
		return 0, fmt.Errorf("Gate ticker 价格为空")
	}
	return price, nil
}

// SetOrderMappingCallback 设置订单映射回调
func (g *GateAdapter) SetOrderMappingCallback(callback func(orderID int64, price float64)) {
	g.orderMappingCallback = callback
}

// GetHistoricalKlines 获取历史K线数据
func (g *GateAdapter) GetHistoricalKlines(ctx context.Context, symbol string, interval string, limit int) ([]*Candle, error) {
	// 转换交易对格式
	gateSymbol := convertToGateSymbol(symbol)

	// 转换K线周期格式
	gateInterval := interval
	if interval == "1m" {
		gateInterval = "1m"
	} else if interval == "5m" {
		gateInterval = "5m"
	} else if interval == "15m" {
		gateInterval = "15m"
	}

	// 调用REST API获取K线数据
	candlesticks, err := g.client.GetCandlesticks(ctx, g.settle, gateSymbol, gateInterval, limit)
	if err != nil {
		return nil, fmt.Errorf("获取历史K线失败: %w", err)
	}

	// 转换为标准格式
	candles := make([]*Candle, 0, len(candlesticks))
	for _, cs := range candlesticks {
		// 解析价格字符串
		open, _ := parseFloat(cs.Open)
		high, _ := parseFloat(cs.High)
		low, _ := parseFloat(cs.Low)
		close, _ := parseFloat(cs.Close)
		volume := float64(cs.Volume)

		candles = append(candles, &Candle{
			Symbol:    symbol,
			Open:      open,
			High:      high,
			Low:       low,
			Close:     close,
			Volume:    volume,
			Timestamp: cs.Timestamp,
			IsClosed:  true, // 历史K线都是已完结的
		})
	}

	return candles, nil
}

// StartKlineStream 启动K线流
func (g *GateAdapter) StartKlineStream(ctx context.Context, symbols []string, interval string, callback func(interface{})) error {
	if g.klineWSManager == nil {
		g.klineWSManager = NewKlineWebSocketManager(g.settle)
	}
	return g.klineWSManager.Start(ctx, symbols, interval, callback)
}

// StopKlineStream 停止K线流
func (g *GateAdapter) StopKlineStream() {
	if g.klineWSManager != nil {
		g.klineWSManager.Stop()
	}
}

func (g *GateAdapter) contractsFromBaseQuantity(quantity float64) int64 {
	if quantity <= 0 {
		return 0
	}
	if g.quantoMultiplier <= 0 {
		contracts := int64(math.Floor(quantity))
		return contracts
	}
	contracts := math.Floor(quantity / g.quantoMultiplier)
	if contracts < 1 {
		return 0
	}
	return int64(contracts)
}

func (g *GateAdapter) baseQuantityFromContracts(contracts int64) float64 {
	return math.Abs(g.signedBaseQuantityFromContracts(contracts))
}

func (g *GateAdapter) signedBaseQuantityFromContracts(contracts int64) float64 {
	if g.quantoMultiplier <= 0 {
		return float64(contracts)
	}
	return float64(contracts) * g.quantoMultiplier
}

func splitGateSymbol(symbol string) (string, string) {
	parts := strings.Split(strings.ToUpper(strings.TrimSpace(symbol)), "_")
	if len(parts) >= 2 {
		return parts[0], parts[1]
	}
	for _, quote := range []string{"USDT", "USDC", "USD"} {
		if strings.HasSuffix(symbol, quote) && len(symbol) > len(quote) {
			return strings.TrimSuffix(symbol, quote), quote
		}
	}
	return strings.ToUpper(symbol), ""
}

// calculateDecimalPlaces 计算小数位数
func calculateDecimalPlaces(value float64) int {
	if value >= 1 {
		return 0
	}

	str := fmt.Sprintf("%.10f", value)
	parts := strings.Split(str, ".")
	if len(parts) != 2 {
		return 0
	}

	// 计算小数点后第一个非零数字的位置
	for i, c := range parts[1] {
		if c != '0' {
			return i + 1
		}
	}

	return 0
}

// convertToBitgetSymbol 转换交易对格式（兼容性函数）
func convertToBitgetSymbol(symbol string) string {
	// Gate.io 使用下划线格式
	return convertToGateSymbol(symbol)
}
