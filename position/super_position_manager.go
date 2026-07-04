package position

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"nexus-trade-bot/config"
	"nexus-trade-bot/logger"
	"nexus-trade-bot/safety"
	"nexus-trade-bot/utils"
)

// OrderUpdate 订单更新事件（避免依赖 websocket 包）
type OrderUpdate struct {
	OrderID       int64
	ClientOrderID string
	Symbol        string
	Status        string
	ExecutedQty   float64
	Price         float64
	AvgPrice      float64
	Side          string
	Type          string
	UpdateTime    int64
}

type orderFillProgress struct {
	ExecutedQty float64
	Terminal    bool
	UpdatedAt   time.Time
}

// OrderExecutorInterface 订单执行器接口（避免循环导入）
type OrderExecutorInterface interface {
	PlaceOrder(req *OrderRequest) (*Order, error)
	BatchPlaceOrders(orders []*OrderRequest) ([]*Order, bool)
	BatchCancelOrders(orderIDs []int64) error
}

// OrderRequest 订单请求（避免循环导入）
type OrderRequest struct {
	Symbol        string
	Side          string
	Price         float64
	Quantity      float64
	PriceDecimals int    // 价格小数位数（用于格式化价格字符串）
	ReduceOnly    bool   // 是否只减仓（平仓单）
	PostOnly      bool   // 是否只做 Maker（Post Only）
	ClientOrderID string // 自定义订单ID
}

// Order 订单信息（避免循环导入）
type Order struct {
	OrderID       int64
	ClientOrderID string
	Symbol        string
	Side          string
	Price         float64
	Quantity      float64
	Status        string
	CreatedAt     time.Time
}

// 订单状态常量
const (
	OrderStatusNotPlaced       = "NOT_PLACED"       // 未下单
	OrderStatusPlaced          = "PLACED"           // 已下单
	OrderStatusConfirmed       = "CONFIRMED"        // 已确认（WebSocket确认）
	OrderStatusPartiallyFilled = "PARTIALLY_FILLED" // 部分成交
	OrderStatusFilled          = "FILLED"           // 全部成交
	OrderStatusCancelRequested = "CANCEL_REQUESTED" // 已申请撤单
	OrderStatusCanceled        = "CANCELED"         // 已撤单
)

// 持仓状态常量
const (
	PositionStatusEmpty  = "EMPTY"  // 空仓
	PositionStatusFilled = "FILLED" // 有仓
)

const (
	BookSideLong  = "LONG"
	BookSideShort = "SHORT"
)

// 槽位锁定状态
const (
	SlotStatusFree    = "FREE"    // 空闲，可操作
	SlotStatusPending = "PENDING" // 等待下单确认
	SlotStatusLocked  = "LOCKED"  // 已锁定，有活跃订单
)

const (
	pendingOrderIdentityGrace    = 30 * time.Second
	entryWindowSyncMinAge        = 2 * time.Second
	entryWindowSyncCooldown      = 2 * time.Second
	entryWindowStableDuration    = 2 * time.Second
	entryWindowFarStableDuration = 12 * time.Second
	priceDistanceEpsilon         = 1e-9
)

type orderQuota struct {
	Limit int
	Used  int
}

// InventorySlot 库存槽位（每个价格点一个）
type InventorySlot struct {
	Price float64 // 价格（作为key，支持高精度）

	// 持仓信息
	PositionStatus string  // 持仓状态：空仓/有仓
	PositionQty    float64 // 持仓数量（支持小数点后3位）
	EntryPrice     float64 // 实际成交均价成本；为空时回退到槽位价格
	BookSide       string  // 槽位归属方向：LONG/SHORT

	// 订单信息 (买卖互斥)
	OrderID        int64     // 订单ID
	ClientOID      string    // 自定义订单ID
	OrderSide      string    // 订单方向 (BUY/SELL)
	OrderStatus    string    // 订单状态
	OrderPrice     float64   // 订单价格
	OrderFilledQty float64   // 成交数量
	OrderCreatedAt time.Time // 创建时间

	// 🔥 新增：槽位锁定状态，防止并发重复操作
	SlotStatus string // FREE/PENDING/LOCKED

	// PostOnly失败计数（仅记录交易所拒绝/过期，不包含程序主动撤单）
	PostOnlyFailCount int

	mu sync.RWMutex // 槽位级别的锁（细粒度锁）
}

// PositionInfo 持仓信息（简化版，避免循环导入）
type PositionInfo struct {
	Symbol           string
	Size             float64
	EntryPrice       float64
	MarkPrice        float64
	UnrealizedPNL    float64
	HasUnrealizedPNL bool
}

// IExchange 交易所接口（避免循环导入）
// 注意：这里不能直接使用 exchange.IExchange，否则会循环导入
// 所以定义一个子集接口，只包含对账需要的方法
type IExchange interface {
	GetName() string // 获取交易所名称
	GetPositions(ctx context.Context, symbol string) (interface{}, error)
	GetOpenOrders(ctx context.Context, symbol string) (interface{}, error)
	GetOrder(ctx context.Context, symbol string, orderID int64) (interface{}, error)
	GetBaseAsset() string                                     // 获取基础资产（交易币种）
	CancelAllOrders(ctx context.Context, symbol string) error // 取消所有订单
}

// SuperPositionManager 超级仓位管理器
type SuperPositionManager struct {
	config   *config.Config
	executor OrderExecutorInterface
	exchange IExchange

	// 价格锚点（初始化时的市场价格）
	anchorPrice float64
	// 最后市场价格（用于打印状态）
	lastMarketPrice atomic.Value // float64
	// 价格精度（根据锚点价格检测得出的小数位数）
	priceDecimals int
	// 数量精度（从交易所获取）
	quantityDecimals int

	// 库存槽位：方向+价格 -> 槽位。neutral 模式下同一价格会同时有 LONG/SHORT 两套状态。
	slots sync.Map // map[string]*InventorySlot

	// 订单成交进度：ClientOrderID -> 已处理累计成交量。
	// 交易所（尤其是 Bitget 私有流）可能重复推送同一张订单的终态事件；槽位释放后仍需要这层记忆保证幂等。
	orderFills sync.Map // map[string]orderFillProgress

	// 保证金管理
	insufficientMargin bool
	marginLockTime     time.Time
	marginLockDuration time.Duration

	// 统计（注意：以下字段被 safety.Reconciler 和 PrintPositions 使用，不可删除）
	totalBuyQty       atomic.Value // float64 - 累计买入数量
	totalSellQty      atomic.Value // float64 - 累计卖出数量
	totalRealizedPNL  atomic.Value // float64 - 已实现净盈亏（按平仓成交价差累计）
	statsMu           sync.Mutex
	reconcileCount    atomic.Int64 // 对账次数
	lastReconcileTime atomic.Value // time.Time - 最后对账时间

	// 初始化标志
	isInitialized atomic.Bool

	haltMu        sync.Mutex
	haltCond      *sync.Cond
	tradingHalted bool
	activeAdjusts int
	adjustMu      sync.Mutex
	mu            sync.RWMutex // 全局锁（用于关键操作）

	lastEntryWindowSync        time.Time
	pendingEntryWindowSyncKey  string
	pendingEntryWindowSyncSeen time.Time

	entryRiskGuard interface {
		CheckEntryOrders(ctx context.Context, orders []safety.GridRiskOrder, markPrice float64, snapshot safety.GridRiskSnapshot) safety.GridRiskDecision
		IsTriggered() bool
		LastReason() string
	}
}

// NewSuperPositionManager 创建超级仓位管理器
func NewSuperPositionManager(cfg *config.Config, executor OrderExecutorInterface, exchange IExchange, priceDecimals, quantityDecimals int) *SuperPositionManager {
	marginLockSec := cfg.Trading.MarginLockDurationSec
	if marginLockSec <= 0 {
		marginLockSec = 10 // 默认10秒
	}

	spm := &SuperPositionManager{
		config:             cfg,
		executor:           executor,
		exchange:           exchange,
		insufficientMargin: false,
		marginLockDuration: time.Duration(marginLockSec) * time.Second,
		priceDecimals:      priceDecimals,
		quantityDecimals:   quantityDecimals,
	}
	spm.totalBuyQty.Store(0.0)
	spm.totalSellQty.Store(0.0)
	spm.totalRealizedPNL.Store(0.0)
	spm.lastReconcileTime.Store(time.Now())
	spm.lastMarketPrice.Store(0.0)
	spm.haltCond = sync.NewCond(&spm.haltMu)
	return spm
}

func (spm *SuperPositionManager) SetEntryRiskGuard(guard interface {
	CheckEntryOrders(ctx context.Context, orders []safety.GridRiskOrder, markPrice float64, snapshot safety.GridRiskSnapshot) safety.GridRiskDecision
	IsTriggered() bool
	LastReason() string
}) {
	spm.mu.Lock()
	defer spm.mu.Unlock()
	spm.entryRiskGuard = guard
}

func (spm *SuperPositionManager) tradingDirection() string {
	direction := strings.ToLower(spm.config.Trading.Direction)
	if direction == "" {
		return "long"
	}
	return direction
}

func (spm *SuperPositionManager) tradingMode() string {
	mode := strings.ToLower(strings.TrimSpace(spm.config.Trading.Mode))
	if mode == "" {
		return "normal"
	}
	return mode
}

func (spm *SuperPositionManager) isAggressiveMode() bool {
	return spm.tradingMode() == "aggressive"
}

func (spm *SuperPositionManager) isClassicMode() bool {
	return spm.tradingMode() == "classic"
}

func (spm *SuperPositionManager) enabledBookSides() []string {
	if spm.isClassicMode() {
		return []string{BookSideLong, BookSideShort}
	}
	if strings.EqualFold(strings.TrimSpace(spm.config.App.MarketType), "spot") {
		return []string{BookSideLong}
	}
	switch spm.tradingDirection() {
	case "long":
		return []string{BookSideLong}
	case "short":
		return []string{BookSideShort}
	case "neutral":
		return []string{BookSideLong, BookSideShort}
	default:
		return []string{BookSideLong}
	}
}

func (spm *SuperPositionManager) entryOrderSide(bookSide string) string {
	if bookSide == BookSideShort {
		return "SELL"
	}
	return "BUY"
}

func (spm *SuperPositionManager) entryWindowSize(bookSide string) int {
	if spm.isClassicMode() {
		return config.ClassicGridWindowSize
	}
	if spm.entryOrderSide(bookSide) == "SELL" {
		return spm.config.Trading.SellWindowSize
	}
	return spm.config.Trading.BuyWindowSize
}

func (spm *SuperPositionManager) exitOrderSide(bookSide string) string {
	if bookSide == BookSideShort {
		return "BUY"
	}
	return "SELL"
}

func (spm *SuperPositionManager) entrySlotDirection(bookSide string) string {
	if bookSide == BookSideShort {
		return "up"
	}
	return "down"
}

func (spm *SuperPositionManager) exitPrice(slotPrice float64, bookSide string) float64 {
	exitGap := spm.config.Trading.PriceInterval * 2
	price := slotPrice + exitGap
	if bookSide == BookSideShort {
		price = slotPrice - exitGap
	}
	return roundPrice(price, spm.priceDecimals)
}

func (spm *SuperPositionManager) slotBelongsToBook(slot *InventorySlot, bookSide string) bool {
	if slot.BookSide == "" {
		return bookSide == BookSideLong
	}
	return slot.BookSide == bookSide
}

func (spm *SuperPositionManager) isEntryOrder(side, bookSide string) bool {
	return side == spm.entryOrderSide(bookSide)
}

func (spm *SuperPositionManager) orderQuotaKey(bookSide, side string) string {
	role := "exit"
	if spm.isEntryOrder(side, bookSide) {
		role = "entry"
	}
	if bookSide == "" {
		bookSide = BookSideLong
	}
	return bookSide + ":" + role
}

func (spm *SuperPositionManager) desiredOrderQuotas(enabledBookSides []string) map[string]*orderQuota {
	quotas := make(map[string]*orderQuota, len(enabledBookSides)*2)
	for _, bookSide := range enabledBookSides {
		entrySide := spm.entryOrderSide(bookSide)
		exitSide := spm.exitOrderSide(bookSide)

		entryLimit := spm.config.Trading.BuyWindowSize
		if entrySide == "SELL" {
			entryLimit = spm.config.Trading.SellWindowSize
		}
		exitLimit := spm.config.Trading.SellWindowSize
		if exitSide == "BUY" {
			exitLimit = spm.config.Trading.BuyWindowSize
		}
		if spm.isClassicMode() {
			entryLimit = config.ClassicGridWindowSize
			exitLimit = config.ClassicGridWindowSize
		}
		quotas[spm.orderQuotaKey(bookSide, entrySide)] = &orderQuota{Limit: maxInt(entryLimit, 0)}
		quotas[spm.orderQuotaKey(bookSide, exitSide)] = &orderQuota{Limit: maxInt(exitLimit, 0)}
	}
	return quotas
}

func (spm *SuperPositionManager) classicOrderThreshold() int {
	threshold := config.TargetOrderCapacity(spm.config)
	if threshold <= 0 {
		threshold = spm.config.Trading.OrderCleanupThreshold
	}
	if threshold <= 0 {
		threshold = 100
	}
	return threshold
}

func (spm *SuperPositionManager) classicSideBudgets() map[string]*orderQuota {
	if spm.isClassicMode() {
		return map[string]*orderQuota{
			"BUY":  {Limit: config.ClassicGridWindowSize},
			"SELL": {Limit: config.ClassicGridWindowSize},
		}
	}
	buyWindowSize := spm.config.Trading.BuyWindowSize
	if buyWindowSize < 0 {
		buyWindowSize = 0
	}
	sellWindowSize := spm.config.Trading.SellWindowSize
	if sellWindowSize <= 0 {
		sellWindowSize = buyWindowSize
	}
	if sellWindowSize < 0 {
		sellWindowSize = 0
	}
	return map[string]*orderQuota{
		"BUY":  {Limit: buyWindowSize},
		"SELL": {Limit: sellWindowSize},
	}
}

func (spm *SuperPositionManager) ensureExitQuotasCoverFilledSlots(quotas map[string]*orderQuota) {
	filledByQuota := make(map[string]int)
	spm.slots.Range(func(key, value interface{}) bool {
		slot := value.(*InventorySlot)
		slot.mu.RLock()
		defer slot.mu.RUnlock()
		if slot.PositionStatus != PositionStatusFilled || slot.PositionQty <= 0 {
			return true
		}
		exitSide := spm.exitOrderSide(slot.BookSide)
		quotaKey := spm.orderQuotaKey(slot.BookSide, exitSide)
		if quotas[quotaKey] == nil {
			return true
		}
		filledByQuota[quotaKey]++
		return true
	})
	for quotaKey, filledCount := range filledByQuota {
		if quota := quotas[quotaKey]; quota != nil && quota.Limit < filledCount {
			quota.Limit = filledCount
		}
	}
}

func remainingQuota(quotas map[string]*orderQuota, key string) int {
	quota := quotas[key]
	if quota == nil {
		return 0
	}
	remaining := quota.Limit - quota.Used
	if remaining < 0 {
		return 0
	}
	return remaining
}

func consumeQuota(quotas map[string]*orderQuota, key string) bool {
	quota := quotas[key]
	if quota == nil || quota.Used >= quota.Limit {
		return false
	}
	quota.Used++
	return true
}

func totalRemainingQuota(quotas map[string]*orderQuota, suffix string) int {
	total := 0
	for key := range quotas {
		if strings.HasSuffix(key, suffix) {
			total += remainingQuota(quotas, key)
		}
	}
	return total
}

func totalQuotaLimitBySuffix(quotas map[string]*orderQuota, suffix string) int {
	total := 0
	for key, quota := range quotas {
		if strings.HasSuffix(key, suffix) && quota != nil {
			total += quota.Limit
		}
	}
	return total
}

func totalQuotaLimit(quotas map[string]*orderQuota) int {
	total := 0
	for _, quota := range quotas {
		if quota != nil {
			total += quota.Limit
		}
	}
	return total
}

func (spm *SuperPositionManager) addTradedQty(side string, deltaQty float64) {
	if deltaQty <= 0 {
		return
	}
	spm.statsMu.Lock()
	defer spm.statsMu.Unlock()
	if side == "BUY" {
		oldTotal := spm.totalBuyQty.Load().(float64)
		spm.totalBuyQty.Store(oldTotal + deltaQty)
		return
	}
	oldTotal := spm.totalSellQty.Load().(float64)
	spm.totalSellQty.Store(oldTotal + deltaQty)
}

func (spm *SuperPositionManager) addRealizedPNL(entryPrice, tradePrice, qty float64, bookSide string) {
	realized := gridRealizedPNL(entryPrice, tradePrice, qty, bookSide, spm.currentFeeRate())
	if realized == 0 {
		return
	}
	spm.statsMu.Lock()
	defer spm.statsMu.Unlock()
	oldTotal := spm.totalRealizedPNL.Load().(float64)
	spm.totalRealizedPNL.Store(oldTotal + realized)
}

func (spm *SuperPositionManager) slotEntryPrice(slot *InventorySlot) float64 {
	return firstPositiveFloat(slot.EntryPrice, slot.Price)
}

func (spm *SuperPositionManager) applyEntryFill(slot *InventorySlot, fillPrice, deltaQty float64) {
	if deltaQty <= 0 {
		return
	}
	fillPrice = firstPositiveFloat(fillPrice, slot.Price)
	slot.EntryPrice = weightedAveragePrice(slot.EntryPrice, slot.PositionQty, fillPrice, deltaQty)
	slot.PositionQty += deltaQty
	slot.PositionStatus = PositionStatusFilled
}

func (spm *SuperPositionManager) applyExitFill(slot *InventorySlot, fillPrice, deltaQty float64, bookSide string) {
	if deltaQty <= 0 {
		return
	}
	entryPrice := spm.slotEntryPrice(slot)
	slot.PositionQty -= deltaQty
	if slot.PositionQty <= 0.000001 {
		slot.PositionQty = 0
		slot.EntryPrice = 0
		slot.PositionStatus = PositionStatusEmpty
	}
	spm.addRealizedPNL(entryPrice, fillPrice, deltaQty, bookSide)
}

func (spm *SuperPositionManager) currentFeeRate() float64 {
	exchangeCfg := spm.config.Exchanges[spm.config.App.CurrentExchange]
	if exchangeCfg.FeeRate < 0 {
		return 0
	}
	if exchangeCfg.FeeRate == 0 {
		return config.DefaultFeeRate
	}
	return exchangeCfg.FeeRate
}

// Initialize 初始化管理器（设置价格锚点并创建初始槽位）
func (spm *SuperPositionManager) Initialize(initialPrice float64, initialPriceStr string) error {
	spm.mu.Lock()
	defer spm.mu.Unlock()

	if initialPrice <= 0 {
		return fmt.Errorf("初始价格无效: %.2f", initialPrice)
	}

	// 1. 设置价格锚点（精度信息已经在构造函数中设置，从交易所获取）
	spm.anchorPrice = initialPrice
	spm.lastMarketPrice.Store(initialPrice) // 初始化最后市场价格
	logger.Info("✅ 价格锚点已设置: %s, 价格精度:%d, 数量精度:%d",
		formatPrice(initialPrice, spm.priceDecimals), spm.priceDecimals, spm.quantityDecimals)

	// 2. 直接使用锚点价格作为网格价格（不再对齐到整数）
	initialGridPrice := spm.anchorPrice
	logger.Info("✅ 初始网格价格: %s (使用锚点价格)", formatPrice(initialGridPrice, spm.priceDecimals))

	for _, bookSide := range spm.enabledBookSides() {
		slotPrices := spm.calculateEntrySlotPrices(initialGridPrice, initialPrice, spm.entryWindowSize(bookSide), bookSide)
		for _, price := range slotPrices {
			slot := spm.getOrCreateSlot(price, bookSide)
			slot.mu.Lock()
			slot.BookSide = bookSide
			slot.mu.Unlock()
		}
		slotPricesStr := make([]string, len(slotPrices))
		for i, p := range slotPrices {
			slotPricesStr[i] = formatPrice(p, spm.priceDecimals)
		}
		logger.Info("✅ [初始化-%s] 计算出的槽位价格: %v", bookSide, slotPricesStr)
	}

	// 5. 为初始槽位下买单
	err := spm.placeInitialBuyOrders()
	if err == nil {
		// 标记为已初始化
		spm.isInitialized.Store(true)
		logger.Info("✅ 初始化完成，网格价格: %s", formatPrice(initialGridPrice, spm.priceDecimals))
	}
	return err
}

// generateClientOrderID 生成自定义订单ID
// 使用新的紧凑格式，最大长度不超过18字符
// 格式: {price_int}_{side}_{timestamp}{seq}
// price_int: price * 10^decimals (转为整数)
// side: B=Buy, S=Sell
func (spm *SuperPositionManager) generateClientOrderID(price float64, side, bookSide string) string {
	// 使用统一的 utils 包生成紧凑ID
	return utils.GenerateOrderIDWithTag(price, side, bookSide, spm.priceDecimals, spm.config.Trading.OrderTag)
}

// parseClientOrderID 解析 ClientOrderID
// 返回: price, side, bookSide, valid
func (spm *SuperPositionManager) parseClientOrderID(clientOrderID string) (float64, string, string, bool) {
	// 1. 先移除交易所前缀
	cleanID := spm.normalizeClientOrderID(clientOrderID)

	// 2. 使用统一的 utils 包解析
	price, side, bookSide, _, orderTag, valid := utils.ParseOrderIDWithTag(cleanID, spm.priceDecimals)
	if !valid {
		return 0, "", "", false
	}
	currentTag := utils.NormalizeOrderTag(spm.config.Trading.OrderTag)
	parsedTag := utils.NormalizeOrderTag(orderTag)
	if currentTag != "" && parsedTag != "" && parsedTag != currentTag {
		return 0, "", "", false
	}

	// 🔥 关键修复：不要对从ClientOrderID解析出的价格进行四舍五入！
	// 因为价格本身就是从整数还原的，已经是精确的值
	// 如果再次四舍五入，可能因为浮点数精度问题导致多个不同价格被映射到同一个槽位
	// 例如: 3116.85 和 3114.85 可能都被四舍五入成同一个值

	return price, side, bookSide, true
}

func (spm *SuperPositionManager) normalizeClientOrderID(clientOrderID string) string {
	clientOrderID = strings.TrimSpace(clientOrderID)
	if clientOrderID == "" {
		return ""
	}
	if spm.exchange != nil {
		exchangeName := strings.ToLower(strings.TrimSpace(spm.exchange.GetName()))
		if exchangeName != "" {
			if cleaned := utils.RemoveBrokerPrefix(exchangeName, clientOrderID); cleaned != clientOrderID {
				return cleaned
			}
		}
	}
	for _, exchangeName := range []string{"binance", "gate"} {
		if cleaned := utils.RemoveBrokerPrefix(exchangeName, clientOrderID); cleaned != clientOrderID {
			return cleaned
		}
	}
	return clientOrderID
}

// placeInitialBuyOrders 设定初始槽位（并恢复持仓槽位）
func (spm *SuperPositionManager) placeInitialBuyOrders() error {
	// 🔥 修改：只恢复持仓槽位，不再主动下单
	// 所有下单操作由 AdjustOrders 统一处理，避免时序问题
	existing := spm.getExistingPositions()
	if existing.LongQty > 0 {
		logger.Info("🔄 [持仓恢复] 检测到现有多仓: %.4f，参考价: %s，开始初始化多头平仓槽位", existing.LongQty, formatPrice(existing.LongEntry, spm.priceDecimals))
		spm.initializeSlotsFromPosition(existing.LongQty, BookSideLong, existing.LongEntry)
	}
	if existing.ShortQty > 0 {
		logger.Info("🔄 [持仓恢复] 检测到现有空仓: %.4f，参考价: %s，开始初始化空头平仓槽位", existing.ShortQty, formatPrice(existing.ShortEntry, spm.priceDecimals))
		spm.initializeSlotsFromPosition(existing.ShortQty, BookSideShort, existing.ShortEntry)
	}

	logger.Info("✅ [初始化] 槽位已创建，订单下达将由 AdjustOrders 统一处理")
	return nil
}

// AdjustOrders 调整订单（交易入口）
func (spm *SuperPositionManager) AdjustOrders(currentPrice float64) error {
	return spm.AdjustOrdersWithRebalance(currentPrice, false)
}

func (spm *SuperPositionManager) AdjustOrdersWithRebalance(currentPrice float64, allowWindowRebalance bool) error {
	spm.adjustMu.Lock()
	defer spm.adjustMu.Unlock()
	if !spm.beginAdjust() {
		logger.Info("⏸️ [停止下单] 交易已进入停止状态，跳过订单调整")
		return nil
	}
	defer spm.endAdjust()
	return spm.adjustOrders(currentPrice, allowWindowRebalance)
}

func (spm *SuperPositionManager) HaltTrading() {
	spm.haltMu.Lock()
	spm.tradingHalted = true
	for spm.activeAdjusts > 0 {
		spm.haltCond.Wait()
	}
	spm.haltMu.Unlock()
}

func (spm *SuperPositionManager) beginAdjust() bool {
	spm.haltMu.Lock()
	defer spm.haltMu.Unlock()
	if spm.tradingHalted {
		return false
	}
	spm.activeAdjusts++
	return true
}

func (spm *SuperPositionManager) endAdjust() {
	spm.haltMu.Lock()
	spm.activeAdjusts--
	if spm.activeAdjusts <= 0 {
		spm.activeAdjusts = 0
		spm.haltCond.Broadcast()
	}
	spm.haltMu.Unlock()
}

func (spm *SuperPositionManager) adjustOrders(currentPrice float64, allowWindowRebalance bool) error {
	return spm.adjustOrdersInternal(currentPrice, allowWindowRebalance, 0)
}

func (spm *SuperPositionManager) adjustOrdersInternal(currentPrice float64, allowWindowRebalance bool, followUpDepth int) error {
	// 🔥 移除初始化检查：现在完全由 AdjustOrders 控制所有下单
	// 初始化只负责恢复持仓状态，不再下单

	spm.mu.Lock()

	// 验证价格有效性
	if currentPrice <= 0 {
		logger.Warn("⚠️ 收到无效价格: %.2f，跳过订单调整", currentPrice)
		spm.mu.Unlock()
		return nil
	}

	// 对当前价格进行精度处理
	currentPrice = roundPrice(currentPrice, spm.priceDecimals)

	// 更新最后市场价格（用于打印状态）
	spm.lastMarketPrice.Store(currentPrice)

	// 检查保证金不足状态
	if spm.insufficientMargin {
		if time.Since(spm.marginLockTime) >= spm.marginLockDuration {
			logger.Info("✅ [保证金恢复] 锁定时间已过，恢复下单功能")
			spm.insufficientMargin = false
		} else {
			remainingTime := spm.marginLockDuration - time.Since(spm.marginLockTime)
			logger.Warn("⏸️ [暂停下单] 保证金不足，暂停下单中... (剩余时间: %.0f秒)", remainingTime.Seconds())
			spm.mu.Unlock()
			return nil
		}
	}

	buyWindowSize := spm.config.Trading.BuyWindowSize
	sellWindowSize := spm.config.Trading.SellWindowSize
	currentGridPrice := spm.findNearestGridPrice(currentPrice)
	spm.pruneEmptyFarSlots(currentGridPrice, maxInt(buyWindowSize, sellWindowSize)*4+10)
	spm.cleanupGhostOrderStates()
	if canceled, err := spm.cancelOrphanExitOrders(); err != nil {
		spm.mu.Unlock()
		return err
	} else if canceled {
		spm.mu.Unlock()
		return spm.adjustOrdersInternal(currentPrice, false, followUpDepth)
	}

	enabledBookSides := spm.enabledBookSides()
	orderQuotas := spm.desiredOrderQuotas(enabledBookSides)
	spm.ensureExitQuotasCoverFilledSlots(orderQuotas)
	classicMode := spm.isClassicMode()
	var sideQuotas map[string]*orderQuota
	if classicMode {
		sideQuotas = spm.classicSideBudgets()
	}

	// 计算允许创建的订单数量上限。中性模式下，多头开/平仓与空头开/平仓独立占用额度。
	threshold := spm.config.Trading.OrderCleanupThreshold
	if classicMode {
		threshold = spm.classicOrderThreshold()
	} else {
		targetOrderLimit := totalQuotaLimit(orderQuotas)
		if threshold <= 0 || threshold < targetOrderLimit {
			threshold = targetOrderLimit
		}
	}

	desiredEntrySlots := make(map[string][]float64, len(enabledBookSides))
	targetEntrySlots := make(map[string][]float64, len(enabledBookSides))
	maxExitOrdersByQuota := make(map[string]int, 2)

	for _, bookSide := range enabledBookSides {
		targetEntrySlots[bookSide] = spm.calculateEntrySlotPrices(currentGridPrice, currentPrice, spm.entryWindowSize(bookSide), bookSide)
		slotPrices := spm.calculateAvailableEntrySlotPrices(currentGridPrice, currentPrice, spm.entryWindowSize(bookSide), bookSide)
		for _, price := range slotPrices {
			desiredEntrySlots[bookSide] = append(desiredEntrySlots[bookSide], price)
		}

		exitSide := spm.exitOrderSide(bookSide)
		quotaKey := spm.orderQuotaKey(bookSide, exitSide)
		if quota := orderQuotas[quotaKey]; quota != nil {
			maxExitOrdersByQuota[quotaKey] = quota.Limit
		}
	}

	if allowWindowRebalance {
		if rebalanced, err := spm.rebalanceExitWindow(currentPrice, maxExitOrdersByQuota, nil); err != nil {
			spm.mu.Unlock()
			return err
		} else if rebalanced {
			spm.mu.Unlock()
			return spm.adjustOrdersInternal(currentPrice, false, followUpDepth)
		}
	}
	if !spm.isAggressiveMode() {
		if rebalanced, err := spm.repairEntryWindowHoles(currentPrice, targetEntrySlots); err != nil {
			spm.mu.Unlock()
			return err
		} else if rebalanced {
			spm.mu.Unlock()
			return spm.adjustOrdersInternal(currentPrice, false, followUpDepth)
		}
	}

	var ordersToPlace []*OrderRequest
	plannedOrderKeys := make(map[string]bool)

	// 统计当前所有订单数量（分别统计买单和卖单）
	var currentOrderCount int
	var currentBuyOrderCount int
	var currentSellOrderCount int
	var currentEntryOrderCount int
	var currentExitOrderCount int
	spm.slots.Range(func(key, value interface{}) bool {
		slot := value.(*InventorySlot)
		slot.mu.RLock()
		if spm.slotHasActiveOrder(slot) {
			currentOrderCount++
			if slot.OrderSide == "BUY" {
				currentBuyOrderCount++
			} else if slot.OrderSide == "SELL" {
				currentSellOrderCount++
			}
			if classicMode {
				if quota := sideQuotas[strings.ToUpper(strings.TrimSpace(slot.OrderSide))]; quota != nil {
					quota.Used++
				}
			}
			if spm.isEntryOrder(slot.OrderSide, slot.BookSide) {
				currentEntryOrderCount++
			} else {
				currentExitOrderCount++
			}
			if quota := orderQuotas[spm.orderQuotaKey(slot.BookSide, slot.OrderSide)]; quota != nil {
				quota.Used++
			}
		}
		if key, blocksDuplicate, ok := spm.activeOrderPlacementKey(slot); ok {
			if blocksDuplicate || !plannedOrderKeys[key] {
				plannedOrderKeys[key] = blocksDuplicate
			}
		}
		slot.mu.RUnlock()
		return true
	})

	// 🔥 核心改进：不预留空间，允许订单数达到threshold上限
	// 剩余可用订单数 = 阈值 - 当前订单数
	remainingOrders := threshold - currentOrderCount
	if remainingOrders < 0 {
		remainingOrders = 0
	}

	var exitCandidates []exitOrderCandidate
	spm.slots.Range(func(key, value interface{}) bool {
		slot := value.(*InventorySlot)
		slotPrice := slot.Price
		slot.mu.Lock()
		defer slot.mu.Unlock()
		if slot.PositionStatus != PositionStatusFilled || slot.SlotStatus != SlotStatusFree || slot.OrderID != 0 || slot.ClientOID != "" || slot.PositionQty <= 0 {
			return true
		}
		exitPrice := spm.exitPrice(slotPrice, slot.BookSide)
		if !spm.isAggressiveMode() || spm.tradingDirection() == "neutral" {
			exitPrice = roundPrice(exitPrice, spm.priceDecimals)
			if !spm.isMakerSafeExitPrice(exitPrice, currentPrice, slot.BookSide) {
				return true
			}
		} else {
			exitPrice = roundPrice(exitPrice, spm.priceDecimals)
			windowMaxDistance := float64(sellWindowSize) * spm.config.Trading.PriceInterval
			if math.Abs(slotPrice-currentPrice) > windowMaxDistance {
				return true
			}
		}
		exitQty, ok := spm.exitOrderQuantity(slot.PositionQty, exitPrice)
		if !ok {
			return true
		}
		orderValue := exitPrice * exitQty
		minValue := spm.minOrderValue()
		if orderValue >= minValue {
			exitCandidates = append(exitCandidates, exitOrderCandidate{
				SlotPrice:         slotPrice,
				ExitPrice:         exitPrice,
				Quantity:          exitQty,
				DistanceToMid:     math.Abs(slotPrice - currentPrice),
				ExitDistanceToMid: math.Abs(exitPrice - currentPrice),
				BookSide:          slot.BookSide,
			})
		}
		return true
	})
	sort.Slice(exitCandidates, func(i, j int) bool {
		if exitCandidates[i].ExitDistanceToMid == exitCandidates[j].ExitDistanceToMid {
			return exitCandidates[i].DistanceToMid < exitCandidates[j].DistanceToMid
		}
		return exitCandidates[i].ExitDistanceToMid < exitCandidates[j].ExitDistanceToMid
	})

	exitPressure := make(map[string]exitRebalancePressure)
	for _, candidate := range exitCandidates {
		exitSide := spm.exitOrderSide(candidate.BookSide)
		quotaKey := spm.orderQuotaKey(candidate.BookSide, exitSide)
		if remainingQuota(orderQuotas, quotaKey) > 0 {
			continue
		}
		pressure := exitPressure[quotaKey]
		if pressure.Count == 0 || candidate.ExitDistanceToMid < pressure.ClosestDistance {
			pressure.ClosestDistance = candidate.ExitDistanceToMid
		}
		pressure.Count++
		exitPressure[quotaKey] = pressure
	}
	if len(exitPressure) > 0 {
		if rebalanced, err := spm.rebalanceExitWindow(currentPrice, maxExitOrdersByQuota, exitPressure); err != nil {
			spm.mu.Unlock()
			return err
		} else if rebalanced {
			spm.mu.Unlock()
			return spm.adjustOrdersInternal(currentPrice, false, followUpDepth)
		}
	}
	if classicMode {
		if rebalanced, err := spm.rebalanceClassicSideBudgetForExits(currentPrice, exitCandidates, sideQuotas, threshold, currentOrderCount); err != nil {
			spm.mu.Unlock()
			return err
		} else if rebalanced {
			spm.mu.Unlock()
			return spm.adjustOrdersInternal(currentPrice, false, followUpDepth)
		}
	}

	exitOrdersToCreate := 0
	entryOrdersToCreate := 0
	skippedDuplicateExitKeys := make(map[string]struct{})
	remainingOrderBudget := func() int {
		remaining := threshold - currentOrderCount - exitOrdersToCreate - entryOrdersToCreate
		if remaining < 0 {
			return 0
		}
		return remaining
	}
	remainingSideQuota := func(side string) int {
		if !classicMode {
			return int(^uint(0) >> 1)
		}
		return remainingQuota(sideQuotas, strings.ToUpper(strings.TrimSpace(side)))
	}
	canCreateOrder := func(quotaKey, side string) bool {
		if remainingOrderBudget() <= 0 {
			return false
		}
		if remainingQuota(orderQuotas, quotaKey) <= 0 {
			return false
		}
		return remainingSideQuota(side) > 0
	}
	consumeOrderBudget := func(quotaKey, side string) bool {
		if !canCreateOrder(quotaKey, side) {
			return false
		}
		if !consumeQuota(orderQuotas, quotaKey) {
			return false
		}
		if classicMode && !consumeQuota(sideQuotas, strings.ToUpper(strings.TrimSpace(side))) {
			quota := orderQuotas[quotaKey]
			if quota != nil && quota.Used > 0 {
				quota.Used--
			}
			return false
		}
		return true
	}

	createExitOrders := func(limit int) {
		if limit <= 0 {
			return
		}
		for i := 0; i < len(exitCandidates) && exitOrdersToCreate < limit; i++ {
			candidate := exitCandidates[i]
			exitSide := spm.exitOrderSide(candidate.BookSide)
			quotaKey := spm.orderQuotaKey(candidate.BookSide, exitSide)
			if !canCreateOrder(quotaKey, exitSide) {
				continue
			}
			slot := spm.getOrCreateSlot(candidate.SlotPrice, candidate.BookSide)
			slot.mu.Lock()
			if slot.SlotStatus != SlotStatusFree || slot.PositionStatus != PositionStatusFilled || slot.PositionQty <= 0 {
				slot.mu.Unlock()
				continue
			}
			slot.SlotStatus = SlotStatusPending
			usePostOnly := true
			clientOID := spm.generateClientOrderID(candidate.SlotPrice, exitSide, candidate.BookSide)
			placementKey := spm.orderPlacementKey(exitSide, candidate.BookSide, candidate.ExitPrice)
			if blocksDuplicate := plannedOrderKeys[placementKey]; blocksDuplicate {
				slot.SlotStatus = SlotStatusFree
				slot.mu.Unlock()
				if _, logged := skippedDuplicateExitKeys[placementKey]; !logged {
					logger.Warn("⚠️ [跳过重复平仓单] %s %s %s 已存在待提交订单，保持固定平仓目标不挪价",
						candidate.BookSide, exitSide, formatPrice(candidate.ExitPrice, spm.priceDecimals))
					skippedDuplicateExitKeys[placementKey] = struct{}{}
				}
				continue
			}
			if !consumeOrderBudget(quotaKey, exitSide) {
				slot.SlotStatus = SlotStatusFree
				slot.mu.Unlock()
				continue
			}
			plannedOrderKeys[placementKey] = false
			slot.ClientOID = clientOID
			slot.OrderSide = exitSide
			slot.OrderPrice = candidate.ExitPrice
			slot.OrderStatus = OrderStatusPlaced
			slot.OrderCreatedAt = time.Now()
			slot.mu.Unlock()
			ordersToPlace = append(ordersToPlace, &OrderRequest{Symbol: spm.config.Trading.Symbol, Side: exitSide, Price: candidate.ExitPrice, Quantity: candidate.Quantity, PriceDecimals: spm.priceDecimals, ReduceOnly: true, PostOnly: usePostOnly, ClientOrderID: clientOID})
			exitOrdersToCreate++
		}
	}

	createEntryOrders := func(limit int) {
		if limit <= 0 {
			return
		}
		for _, bookSide := range enabledBookSides {
			entrySide := spm.entryOrderSide(bookSide)
			quotaKey := spm.orderQuotaKey(bookSide, entrySide)
			for _, price := range desiredEntrySlots[bookSide] {
				if entryOrdersToCreate >= limit {
					break
				}
				if !canCreateOrder(quotaKey, entrySide) {
					break
				}
				slot := spm.getOrCreateSlot(price, bookSide)
				slot.mu.Lock()
				slot.BookSide = bookSide
				if slot.SlotStatus != SlotStatusFree || slot.PositionStatus != PositionStatusEmpty {
					slot.mu.Unlock()
					continue
				}
				if spm.slotHasActiveOrder(slot) || slot.OrderID != 0 || slot.ClientOID != "" {
					slot.mu.Unlock()
					continue
				}

				quantity, ok := spm.entryOrderQuantity(price)
				if !ok {
					slot.mu.Unlock()
					continue
				}
				clientOID := spm.generateClientOrderID(price, entrySide, bookSide)
				placementKey := spm.orderPlacementKey(entrySide, bookSide, price)
				if _, exists := plannedOrderKeys[placementKey]; exists {
					slot.mu.Unlock()
					logger.Warn("⚠️ [跳过重复开仓单] %s %s %s 已存在活跃/待提交订单",
						bookSide, entrySide, formatPrice(price, spm.priceDecimals))
					continue
				}
				if !consumeOrderBudget(quotaKey, entrySide) {
					slot.mu.Unlock()
					break
				}
				plannedOrderKeys[placementKey] = true
				slot.SlotStatus = SlotStatusPending
				slot.ClientOID = clientOID
				slot.OrderSide = entrySide
				slot.OrderPrice = price
				slot.OrderStatus = OrderStatusPlaced
				slot.OrderCreatedAt = time.Now()
				usePostOnly := true
				slot.mu.Unlock()

				ordersToPlace = append(ordersToPlace, &OrderRequest{
					Symbol:        spm.config.Trading.Symbol,
					Side:          entrySide,
					Price:         price,
					Quantity:      quantity,
					PriceDecimals: spm.priceDecimals,
					PostOnly:      usePostOnly,
					ClientOrderID: clientOID,
				})
				entryOrdersToCreate++
			}
		}
	}

	if spm.isAggressiveMode() {
		allowedNewEntryOrders := remainingOrders
		entryQuotaRemaining := totalRemainingQuota(orderQuotas, ":entry")
		if allowedNewEntryOrders > entryQuotaRemaining {
			allowedNewEntryOrders = entryQuotaRemaining
		}
		if allowedNewEntryOrders < 0 {
			allowedNewEntryOrders = 0
		}
		createEntryOrders(allowedNewEntryOrders)

		remainingOrdersForExit := remainingOrderBudget()
		allowedNewExitOrders := totalRemainingQuota(orderQuotas, ":exit")
		if allowedNewExitOrders > remainingOrdersForExit {
			allowedNewExitOrders = remainingOrdersForExit
		}
		createExitOrders(allowedNewExitOrders)
	} else {
		allowedNewExitOrders := totalRemainingQuota(orderQuotas, ":exit")
		if allowedNewExitOrders > remainingOrderBudget() {
			allowedNewExitOrders = remainingOrderBudget()
		}
		createExitOrders(allowedNewExitOrders)

		allowedNewEntryOrders := totalRemainingQuota(orderQuotas, ":entry")
		if allowedNewEntryOrders > remainingOrderBudget() {
			allowedNewEntryOrders = remainingOrderBudget()
		}
		createEntryOrders(allowedNewEntryOrders)
	}

	entryQuotaLimit := totalQuotaLimitBySuffix(orderQuotas, ":entry")
	exitQuotaLimit := totalQuotaLimitBySuffix(orderQuotas, ":exit")
	logger.Info("📊 [订单配额] 阈值:%d, 当前订单:%d(开仓:%d/%d 平仓保护:%d/%d, BUY:%d SELL:%d), 剩余:%d, 平仓候选:%d, 新增平仓:%d, 新增开仓:%d",
		threshold, currentOrderCount, currentEntryOrderCount, entryQuotaLimit, currentExitOrderCount, exitQuotaLimit, currentBuyOrderCount, currentSellOrderCount, remainingOrders, len(exitCandidates), exitOrdersToCreate, entryOrdersToCreate)

	riskGuard := spm.entryRiskGuard
	riskSnapshot := spm.gridRiskSnapshotLocked(currentPrice)
	// 下单是网络 I/O，不能持有全局锁。槽位已经标记为 PENDING，结果回写依赖槽位锁。
	spm.mu.Unlock()

	if riskGuard != nil && len(ordersToPlace) > 0 {
		entryRiskOrders := spm.toGridRiskEntryOrders(ordersToPlace)
		if len(entryRiskOrders) > 0 {
			decision := riskGuard.CheckEntryOrders(context.Background(), entryRiskOrders, currentPrice, riskSnapshot)
			if !decision.Allowed {
				logger.Warn("⛔ [合约风控拒绝新开仓] 原因: %s；本轮只提交 reduce-only 平仓保护单，阻止开仓单=%d",
					strings.Join(decision.Reasons, "; "), len(entryRiskOrders))
				ordersToPlace = spm.filterReduceOnlyOrdersAndReleaseEntries(ordersToPlace)
				spm.CancelEntryOrders()
			}
		}
	}

	// 执行下单
	if len(ordersToPlace) > 0 {
		logger.Info("🔄 [实时调整] 需要新增: %d 个订单", len(ordersToPlace))
		placedOrders, marginError := spm.executor.BatchPlaceOrders(ordersToPlace)
		spm.attachMissingClientOrderIDs(ordersToPlace, placedOrders)
		classicFollowUpNeeded := false

		if marginError {
			logger.Warn("⚠️ [保证金不足] 检测到保证金不足错误，暂停下单 %d 秒", int(spm.marginLockDuration.Seconds()))
			spm.mu.Lock()
			spm.insufficientMargin = true
			spm.marginLockTime = time.Now()
			spm.mu.Unlock()
			spm.CancelEntryOrders()
		}

		// 🔥 构建成功订单的ClientOrderID集合
		placedClientOIDs := make(map[string]bool)
		for _, ord := range placedOrders {
			if strings.TrimSpace(ord.ClientOrderID) != "" {
				placedClientOIDs[ord.ClientOrderID] = true
			}
		}
		if classicMode && !marginError && len(placedClientOIDs) < len(ordersToPlace) {
			classicFollowUpNeeded = true
		}

		// 🔥 释放未成功提交订单的槽位锁
		for _, req := range ordersToPlace {
			if !placedClientOIDs[req.ClientOrderID] {
				// 这个订单没有成功提交，需要释放槽位锁
				price, _, bookSide, valid := spm.parseClientOrderID(req.ClientOrderID)
				if valid {
					slot := spm.getOrCreateSlot(price, bookSide)
					slot.mu.Lock()
					slot.BookSide = bookSide
					if slot.SlotStatus == SlotStatusPending {
						slot.SlotStatus = SlotStatusFree
						if slot.ClientOID == req.ClientOrderID {
							clearSlotOrderTracking(slot, OrderStatusNotPlaced)
						}
						logger.Debug("🔓 [释放槽位] 订单提交失败，释放槽位 %s 的锁 (ClientOID: %s)",
							formatPrice(price, spm.priceDecimals), req.ClientOrderID)
					}
					slot.mu.Unlock()
				}
			}
		}

		var conflictOrderIDs []int64
		for _, ord := range placedOrders {
			// 解析 ClientOrderID
			price, side, bookSide, valid := spm.parseClientOrderID(ord.ClientOrderID)

			if !valid {
				logger.Warn("⚠️ [实时调整] 无法解析 ClientOID: %s", ord.ClientOrderID)
				continue
			}

			// 获取槽位 (注意：无论是买单还是卖单，ID中编码的都是 SlotPrice)
			slot := spm.getOrCreateSlot(price, bookSide)
			slot.mu.Lock()
			slot.BookSide = bookSide

			// 🔥 关键修复：检查是否是秒成交场景（买单或卖单都可能）
			// 秒成交的特征:
			// 1. 买单秒成交: PositionStatus=FILLED (刚成交) 且 OrderID=0 (已被WebSocket清空) 且 OrderSide=""
			// 2. 卖单秒成交: PositionStatus=EMPTY (已清空) 且 OrderID=0 (已被WebSocket清空) 且 OrderSide=""
			isInstantFill := false
			if spm.isEntryOrder(side, bookSide) {
				isInstantFill = (slot.PositionStatus == PositionStatusFilled && slot.OrderID == 0 && slot.OrderSide == "")
			} else {
				isInstantFill = (slot.PositionStatus == PositionStatusEmpty && slot.OrderID == 0 && slot.OrderSide == "" && slot.SlotStatus == SlotStatusFree)
			}

			if !isInstantFill {
				// 正常情况: 更新订单状态
				// 🔥 检查OrderID冲突：只有当ClientOID已设置且不匹配时才是真正的冲突
				// 如果ClientOID为空或匹配，说明是正常的WebSocket先到或批量处理顺序问题
				if slot.OrderID != 0 && slot.OrderID != ord.OrderID {
					if slot.ClientOID != "" && slot.ClientOID != ord.ClientOrderID {
						// 真正的冲突：槽位已被其他订单占用
						logger.Warn("⚠️ [OrderID冲突] 槽位 %.2f: 下单返回OrderID=%d (ClientOID=%s)，但槽位已被OrderID=%d (ClientOID=%s)占用，将立即撤销新单避免同价双挂",
							price, ord.OrderID, ord.ClientOrderID, slot.OrderID, slot.ClientOID)
						conflictOrderIDs = append(conflictOrderIDs, ord.OrderID)
						slot.mu.Unlock()
						continue
					} else {
						// WebSocket推送先到达，这是正常现象
						logger.Debug("📝 [覆盖OrderID] 槽位 %.2f: WebSocket已设置OrderID=%d，现用下单返回的OrderID=%d (ClientOID: %s)",
							price, slot.OrderID, ord.OrderID, ord.ClientOrderID)
					}
				}

				slot.OrderID = ord.OrderID
				slot.ClientOID = ord.ClientOrderID
				slot.OrderSide = side // "BUY" or "SELL"
				slot.OrderStatus = OrderStatusPlaced
				slot.OrderPrice = ord.Price
				slot.OrderCreatedAt = time.Now()
				// 🔥 订单提交成功，设置为LOCKED状态
				slot.SlotStatus = SlotStatusLocked
				// 注意：不在这里重置PostOnlyFailCount，因为订单可能立即被撤销
				// PostOnly计数只在订单真正成交时重置

				logger.Info("✅ [实时新增] 槽位价格: %s, %s订单, 订单价格: %s, 订单ID: %d, ClientOID: %s",
					formatPrice(price, spm.priceDecimals), side, formatPrice(ord.Price, spm.priceDecimals), ord.OrderID, ord.ClientOrderID)
			} else {
				// 🔍 秒成交场景：WebSocket已经处理了FILLED,跳过状态更新
				logger.Debug("🔍 [%s单秒成交] 槽位 %s 的订单已被WebSocket处理，跳过状态更新 (持仓: %.4f, SlotStatus: %s)",
					side, formatPrice(price, spm.priceDecimals), slot.PositionQty, slot.SlotStatus)
			}

			slot.mu.Unlock()
		}
		if len(conflictOrderIDs) > 0 {
			if classicMode && !marginError {
				classicFollowUpNeeded = true
			}
			if err := spm.executor.BatchCancelOrders(conflictOrderIDs); err != nil {
				logger.Error("❌ [重复订单保护] 撤销冲突新单失败: %v (orderIDs=%v)", err, conflictOrderIDs)
			} else {
				logger.Warn("🧹 [重复订单保护] 已撤销 %d 张同价冲突新单: %v", len(conflictOrderIDs), conflictOrderIDs)
			}
		}
		if classicFollowUpNeeded && followUpDepth < 1 {
			logger.Info("🔁 [经典网格自愈] 本轮下单未完全落地，立即追加一次补单检查")
			return spm.adjustOrdersInternal(currentPrice, false, followUpDepth+1)
		}
	}

	return nil
}

func (spm *SuperPositionManager) entryWindowSyncKey(targetEntrySlots map[string][]float64, holePrices []float64) string {
	parts := make([]string, 0, len(targetEntrySlots)+1)
	bookSides := make([]string, 0, len(targetEntrySlots))
	for bookSide := range targetEntrySlots {
		bookSides = append(bookSides, bookSide)
	}
	sort.Strings(bookSides)
	for _, bookSide := range bookSides {
		prices := append([]float64(nil), targetEntrySlots[bookSide]...)
		sort.Float64s(prices)
		formatted := make([]string, 0, len(prices))
		for _, price := range prices {
			formatted = append(formatted, formatPrice(roundPrice(price, spm.priceDecimals), spm.priceDecimals))
		}
		parts = append(parts, bookSide+":"+strings.Join(formatted, ","))
	}
	holes := append([]float64(nil), holePrices...)
	sort.Float64s(holes)
	formattedHoles := make([]string, 0, len(holes))
	for _, price := range holes {
		formattedHoles = append(formattedHoles, formatPrice(roundPrice(price, spm.priceDecimals), spm.priceDecimals))
	}
	parts = append(parts, "holes:"+strings.Join(formattedHoles, ","))
	return strings.Join(parts, "|")
}

func (spm *SuperPositionManager) entryWindowSyncStable(syncKey string, now time.Time, stableDuration time.Duration) bool {
	if syncKey == "" {
		spm.pendingEntryWindowSyncKey = ""
		spm.pendingEntryWindowSyncSeen = time.Time{}
		return false
	}
	if stableDuration <= 0 {
		stableDuration = entryWindowStableDuration
	}
	if spm.pendingEntryWindowSyncKey != syncKey {
		spm.pendingEntryWindowSyncKey = syncKey
		spm.pendingEntryWindowSyncSeen = now
		return false
	}
	if spm.pendingEntryWindowSyncSeen.IsZero() {
		spm.pendingEntryWindowSyncSeen = now
		return false
	}
	return now.Sub(spm.pendingEntryWindowSyncSeen) >= stableDuration
}

func (spm *SuperPositionManager) repairEntryWindowHoles(currentPrice float64, targetEntrySlots map[string][]float64) (bool, error) {
	if len(targetEntrySlots) == 0 {
		return false, nil
	}
	now := time.Now()
	classicMode := spm.isClassicMode()
	if !classicMode && !spm.lastEntryWindowSync.IsZero() && now.Sub(spm.lastEntryWindowSync) < entryWindowSyncCooldown {
		return false, nil
	}

	targetByBook := make(map[string]map[float64]bool, len(targetEntrySlots))
	holeCountByBook := make(map[string]int, len(targetEntrySlots))
	holesByBook := make(map[string][]entryWindowHole, len(targetEntrySlots))
	holePrices := make([]float64, 0)
	for bookSide, prices := range targetEntrySlots {
		if len(prices) == 0 {
			continue
		}
		targetByBook[bookSide] = make(map[float64]bool, len(prices))
		for _, price := range prices {
			price = roundPrice(price, spm.priceDecimals)
			targetByBook[bookSide][price] = true
			if spm.entryWindowSlotNeedsOrder(price, bookSide) {
				holeCountByBook[bookSide]++
				holesByBook[bookSide] = append(holesByBook[bookSide], entryWindowHole{
					Price:    price,
					Distance: math.Abs(price - currentPrice),
				})
				holePrices = append(holePrices, price)
			}
		}
	}

	totalHoles := 0
	for _, count := range holeCountByBook {
		totalHoles += count
	}
	if totalHoles == 0 {
		spm.pendingEntryWindowSyncKey = ""
		spm.pendingEntryWindowSyncSeen = time.Time{}
		return false, nil
	}

	candidatesByBook := make(map[string][]entryWindowSyncCandidate, len(holeCountByBook))
	spm.slots.Range(func(key, value interface{}) bool {
		slot := value.(*InventorySlot)
		slot.mu.RLock()
		defer slot.mu.RUnlock()
		if holeCountByBook[slot.BookSide] <= 0 {
			return true
		}
		if targetByBook[slot.BookSide][roundPrice(slot.Price, spm.priceDecimals)] {
			return true
		}
		if !spm.isEntryOrder(slot.OrderSide, slot.BookSide) || !spm.slotHasActiveOrder(slot) {
			return true
		}
		if slot.PositionStatus != PositionStatusEmpty || slot.OrderID == 0 {
			return true
		}
		if !slot.OrderCreatedAt.IsZero() && now.Sub(slot.OrderCreatedAt) < entryWindowSyncMinAge {
			return true
		}
		candidatesByBook[slot.BookSide] = append(candidatesByBook[slot.BookSide], entryWindowSyncCandidate{
			SlotPrice: slot.Price,
			BookSide:  slot.BookSide,
			OrderID:   slot.OrderID,
			ClientOID: slot.ClientOID,
			Distance:  math.Abs(slot.Price - currentPrice),
		})
		return true
	})

	var candidates []entryWindowSyncCandidate
	selectedHolePrices := make([]float64, 0, totalHoles)
	requiredStableDuration := entryWindowStableDuration
	for bookSide, bookCandidates := range candidatesByBook {
		holes := holesByBook[bookSide]
		if len(holes) == 0 || len(bookCandidates) == 0 {
			continue
		}
		sort.Slice(holes, func(i, j int) bool {
			if holes[i].Distance == holes[j].Distance {
				return holes[i].Price < holes[j].Price
			}
			return holes[i].Distance < holes[j].Distance
		})
		sort.Slice(bookCandidates, func(i, j int) bool {
			if bookCandidates[i].Distance == bookCandidates[j].Distance {
				return bookCandidates[i].OrderID > bookCandidates[j].OrderID
			}
			return bookCandidates[i].Distance > bookCandidates[j].Distance
		})
		for i := 0; i < len(holes) && i < len(bookCandidates); i++ {
			// Holes closer to the market are repaired quickly. Holes farther away
			// are just window extension, so they wait longer to avoid edge churn.
			recentSingleStepRoll := !spm.lastEntryWindowSync.IsZero() &&
				now.Sub(spm.lastEntryWindowSync) < entryWindowFarStableDuration &&
				spm.isSingleStepEntryWindowRoll(holes, bookCandidates[i], len(targetByBook[bookSide]))
			if !classicMode && (bookCandidates[i].Distance <= holes[i].Distance+priceDistanceEpsilon ||
				recentSingleStepRoll) {
				requiredStableDuration = maxDuration(requiredStableDuration, entryWindowFarStableDuration)
			}
			candidates = append(candidates, bookCandidates[i])
			selectedHolePrices = append(selectedHolePrices, holes[i].Price)
		}
	}
	if len(candidates) == 0 {
		spm.pendingEntryWindowSyncKey = ""
		spm.pendingEntryWindowSyncSeen = time.Time{}
		logger.Debug("🧩 [开仓补洞等待] 没有可撤换的开仓单，暂不撤单: 缺口=%s",
			spm.formatPriceList(holePrices, len(holePrices)))
		return false, nil
	}
	syncKey := spm.entryWindowSyncKey(targetEntrySlots, selectedHolePrices)
	if !classicMode && !spm.entryWindowSyncStable(syncKey, now, requiredStableDuration) {
		logger.Debug("🧩 [开仓补洞等待] 目标窗口尚未稳定，暂不撤单: 等待=%s, 缺口=%s",
			requiredStableDuration, spm.formatPriceList(selectedHolePrices, len(selectedHolePrices)))
		return false, nil
	}
	if classicMode {
		spm.pendingEntryWindowSyncKey = ""
		spm.pendingEntryWindowSyncSeen = time.Time{}
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Distance == candidates[j].Distance {
			return candidates[i].OrderID > candidates[j].OrderID
		}
		return candidates[i].Distance > candidates[j].Distance
	})
	orderIDs := make([]int64, 0, len(candidates))
	for _, candidate := range candidates {
		orderIDs = append(orderIDs, candidate.OrderID)
	}
	if len(orderIDs) < totalHoles {
		logger.Info("🧩 [开仓补洞] 当前窗口缺 %d 个开仓价位，本轮只补 %d 个更贴近现价的缺口: 缺口=%s",
			totalHoles, len(orderIDs), spm.formatPriceList(selectedHolePrices, len(orderIDs)))
	} else {
		logger.Info("🧩 [开仓补洞] 当前窗口缺 %d 个开仓价位，一次性撤销 %d 个远端开仓单补齐: 缺口=%s",
			totalHoles, len(orderIDs), spm.formatPriceList(selectedHolePrices, len(orderIDs)))
	}

	spm.mu.Unlock()
	err := spm.executor.BatchCancelOrders(orderIDs)
	spm.mu.Lock()
	if err != nil {
		logger.Warn("⚠️ [开仓补洞] 撤销远端开仓单失败: %v", err)
		return false, nil
	}
	spm.lastEntryWindowSync = now
	spm.pendingEntryWindowSyncKey = ""
	spm.pendingEntryWindowSyncSeen = time.Time{}
	for _, candidate := range candidates {
		spm.UpdateSlotOrderStatusIfCurrent(candidate.SlotPrice, candidate.BookSide, OrderStatusCanceled, candidate.OrderID, candidate.ClientOID)
	}
	return true, nil
}

func (spm *SuperPositionManager) isSingleStepEntryWindowRoll(holes []entryWindowHole, candidate entryWindowSyncCandidate, targetCount int) bool {
	if len(holes) != 1 || targetCount <= 1 {
		return false
	}
	priceInterval := spm.config.Trading.PriceInterval
	if priceInterval <= 0 {
		return false
	}
	windowSpan := priceInterval * float64(targetCount)
	delta := math.Abs(candidate.SlotPrice - holes[0].Price)
	return math.Abs(delta-windowSpan) <= math.Max(priceDistanceEpsilon, priceInterval*0.001)
}

func (spm *SuperPositionManager) cancelOrphanExitOrders() (bool, error) {
	if spm.executor == nil {
		return false, nil
	}
	type orphanExitOrder struct {
		SlotPrice float64
		BookSide  string
		OrderID   int64
		ClientOID string
	}
	var orphans []orphanExitOrder
	spm.slots.Range(func(key, value interface{}) bool {
		slot := value.(*InventorySlot)
		slot.mu.RLock()
		defer slot.mu.RUnlock()
		if !spm.slotHasActiveOrder(slot) || spm.isEntryOrder(slot.OrderSide, slot.BookSide) {
			return true
		}
		if slot.PositionStatus == PositionStatusFilled && slot.PositionQty > 0 {
			return true
		}
		if slot.OrderID == 0 {
			return true
		}
		orphans = append(orphans, orphanExitOrder{
			SlotPrice: slot.Price,
			BookSide:  slot.BookSide,
			OrderID:   slot.OrderID,
			ClientOID: slot.ClientOID,
		})
		return true
	})
	if len(orphans) == 0 {
		return false, nil
	}
	orderIDs := make([]int64, 0, len(orphans))
	for _, orphan := range orphans {
		orderIDs = append(orderIDs, orphan.OrderID)
	}
	logger.Warn("🧹 [平仓孤儿单清理] 发现 %d 个没有本地持仓的平仓单，占用平仓额度，先撤销再补真正缺失的平仓单", len(orderIDs))

	spm.mu.Unlock()
	err := spm.executor.BatchCancelOrders(orderIDs)
	spm.mu.Lock()
	if err != nil {
		logger.Warn("⚠️ [平仓孤儿单清理] 撤销孤儿平仓单失败: %v", err)
		return false, nil
	}
	for _, orphan := range orphans {
		spm.UpdateSlotOrderStatusIfCurrent(orphan.SlotPrice, orphan.BookSide, OrderStatusCanceled, orphan.OrderID, orphan.ClientOID)
	}
	return true, nil
}

func (spm *SuperPositionManager) entryWindowSlotNeedsOrder(price float64, bookSide string) bool {
	value, ok := spm.slots.Load(spm.slotKey(price, bookSide))
	if !ok {
		return true
	}
	slot := value.(*InventorySlot)
	slot.mu.RLock()
	defer slot.mu.RUnlock()
	if spm.slotHasActiveOrder(slot) {
		return false
	}
	return slot.PositionStatus == PositionStatusEmpty && slot.SlotStatus == SlotStatusFree
}

func (spm *SuperPositionManager) formatPriceList(prices []float64, limit int) string {
	if len(prices) == 0 {
		return "[]"
	}
	if limit <= 0 || limit > len(prices) {
		limit = len(prices)
	}
	formatted := make([]string, 0, limit+1)
	for i := 0; i < limit; i++ {
		formatted = append(formatted, formatPrice(prices[i], spm.priceDecimals))
	}
	if len(prices) > limit {
		formatted = append(formatted, "...")
	}
	return "[" + strings.Join(formatted, " ") + "]"
}

func (spm *SuperPositionManager) rebalanceExitWindow(currentPrice float64, maxActiveExitOrdersByQuota map[string]int, pressure map[string]exitRebalancePressure) (bool, error) {
	if len(maxActiveExitOrdersByQuota) == 0 {
		return false, nil
	}
	exitsByQuota := make(map[string][]activeOrderCandidate, len(maxActiveExitOrdersByQuota))
	spm.slots.Range(func(key, value interface{}) bool {
		slot := value.(*InventorySlot)
		slot.mu.RLock()
		defer slot.mu.RUnlock()
		if spm.isEntryOrder(slot.OrderSide, slot.BookSide) {
			return true
		}
		if !spm.slotHasActiveOrder(slot) {
			return true
		}
		if slot.OrderID == 0 || slot.PositionQty <= 0 {
			return true
		}
		orderSide := strings.ToUpper(strings.TrimSpace(slot.OrderSide))
		quotaKey := spm.orderQuotaKey(slot.BookSide, orderSide)
		if maxActiveExitOrdersByQuota[quotaKey] <= 0 {
			return true
		}
		orderPrice := roundPrice(slot.OrderPrice, spm.priceDecimals)
		exitsByQuota[quotaKey] = append(exitsByQuota[quotaKey], activeOrderCandidate{
			SlotPrice:     slot.Price,
			BookSide:      slot.BookSide,
			OrderSide:     orderSide,
			OrderPrice:    orderPrice,
			OrderID:       slot.OrderID,
			ClientOID:     slot.ClientOID,
			DistanceToMid: math.Abs(orderPrice - currentPrice),
		})
		return true
	})

	var toCancel []activeOrderCandidate
	for quotaKey, exits := range exitsByQuota {
		maxActiveExitOrders := maxActiveExitOrdersByQuota[quotaKey]
		if maxActiveExitOrders <= 0 || len(exits) == 0 {
			continue
		}
		sort.Slice(exits, func(i, j int) bool {
			if exits[i].DistanceToMid == exits[j].DistanceToMid {
				return exits[i].OrderID > exits[j].OrderID
			}
			return exits[i].DistanceToMid > exits[j].DistanceToMid
		})

		if len(exits) > maxActiveExitOrders {
			toCancel = append(toCancel, exits[:len(exits)-maxActiveExitOrders]...)
			continue
		}

		if pressure != nil {
			if sidePressure, ok := pressure[quotaKey]; ok && sidePressure.Count > 0 && len(exits) >= maxActiveExitOrders {
				farthest := exits[0]
				if farthest.DistanceToMid > sidePressure.ClosestDistance {
					toCancel = append(toCancel, farthest)
					continue
				}
			}
		}

	}
	if len(toCancel) == 0 {
		return false, nil
	}
	sort.Slice(toCancel, func(i, j int) bool {
		if toCancel[i].OrderSide != toCancel[j].OrderSide {
			return toCancel[i].OrderSide < toCancel[j].OrderSide
		}
		if toCancel[i].DistanceToMid == toCancel[j].DistanceToMid {
			return toCancel[i].OrderID > toCancel[j].OrderID
		}
		return toCancel[i].DistanceToMid > toCancel[j].DistanceToMid
	})
	batchSize := spm.config.Trading.CleanupBatchSize
	if batchSize <= 0 {
		batchSize = 10
	}
	if len(toCancel) > batchSize {
		toCancel = toCancel[:batchSize]
	}
	orderIDs := make([]int64, 0, len(toCancel))
	for _, candidate := range toCancel {
		orderIDs = append(orderIDs, candidate.OrderID)
	}
	logger.Info("🧭 [平仓自检] 平仓窗口需要刷新，撤销 %d 个远端平仓单后优先补回现价周边", len(orderIDs))

	spm.mu.Unlock()
	err := spm.executor.BatchCancelOrders(orderIDs)
	spm.mu.Lock()
	if err != nil {
		logger.Warn("⚠️ [平仓自检] 撤销超额平仓单失败: %v", err)
		return false, nil
	}
	for _, candidate := range toCancel {
		spm.UpdateSlotOrderStatusIfCurrent(candidate.SlotPrice, candidate.BookSide, OrderStatusCanceled, candidate.OrderID, candidate.ClientOID)
	}
	return true, nil
}

func (spm *SuperPositionManager) rebalanceClassicSideBudgetForExits(currentPrice float64, exitCandidates []exitOrderCandidate, sideQuotas map[string]*orderQuota, threshold, currentOrderCount int) (bool, error) {
	if !spm.isClassicMode() || len(exitCandidates) == 0 || len(sideQuotas) == 0 {
		return false, nil
	}

	sideDeficit := make(map[string]int, 2)
	availableSideQuota := make(map[string]int, 2)
	remainingTotalFree := maxInt(threshold-currentOrderCount, 0)
	totalDeficit := 0
	for _, candidate := range exitCandidates {
		side := spm.exitOrderSide(candidate.BookSide)
		if _, initialized := availableSideQuota[side]; !initialized {
			availableSideQuota[side] = remainingQuota(sideQuotas, side)
		}
		if availableSideQuota[side] > 0 {
			availableSideQuota[side]--
		} else {
			sideDeficit[side]++
		}
		if remainingTotalFree > 0 {
			remainingTotalFree--
		} else {
			totalDeficit++
		}
	}
	overThreshold := maxInt(currentOrderCount-threshold, 0)
	requiredTotalCancel := totalDeficit + overThreshold
	sideDeficitTotal := 0
	for _, missing := range sideDeficit {
		sideDeficitTotal += missing
	}
	if requiredTotalCancel < sideDeficitTotal {
		requiredTotalCancel = sideDeficitTotal
	}
	if requiredTotalCancel <= 0 {
		return false, nil
	}

	entriesBySide := make(map[string][]activeOrderCandidate, 2)
	var allEntries []activeOrderCandidate
	spm.slots.Range(func(key, value interface{}) bool {
		slot := value.(*InventorySlot)
		slot.mu.RLock()
		defer slot.mu.RUnlock()
		if !spm.slotHasActiveOrder(slot) || !spm.isEntryOrder(slot.OrderSide, slot.BookSide) {
			return true
		}
		if slot.OrderID == 0 || slot.PositionStatus != PositionStatusEmpty {
			return true
		}
		side := strings.ToUpper(strings.TrimSpace(slot.OrderSide))
		orderPrice := roundPrice(firstPositiveFloat(slot.OrderPrice, slot.Price), spm.priceDecimals)
		candidate := activeOrderCandidate{
			SlotPrice:     slot.Price,
			BookSide:      slot.BookSide,
			OrderSide:     side,
			OrderPrice:    orderPrice,
			OrderID:       slot.OrderID,
			ClientOID:     slot.ClientOID,
			DistanceToMid: math.Abs(orderPrice - currentPrice),
		}
		entriesBySide[side] = append(entriesBySide[side], candidate)
		allEntries = append(allEntries, candidate)
		return true
	})

	var toCancel []activeOrderCandidate
	selectedOrderIDs := make(map[int64]struct{})
	for side, missing := range sideDeficit {
		entries := entriesBySide[side]
		if missing <= 0 || len(entries) == 0 {
			continue
		}
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].DistanceToMid == entries[j].DistanceToMid {
				return entries[i].OrderID > entries[j].OrderID
			}
			return entries[i].DistanceToMid > entries[j].DistanceToMid
		})
		cancelNeeded := missing
		if cancelNeeded > len(entries) {
			cancelNeeded = len(entries)
		}
		for _, entry := range entries[:cancelNeeded] {
			toCancel = append(toCancel, entry)
			selectedOrderIDs[entry.OrderID] = struct{}{}
		}
	}
	extraCancelNeeded := requiredTotalCancel - len(toCancel)
	if extraCancelNeeded > 0 {
		sort.Slice(allEntries, func(i, j int) bool {
			if allEntries[i].DistanceToMid == allEntries[j].DistanceToMid {
				return allEntries[i].OrderID > allEntries[j].OrderID
			}
			return allEntries[i].DistanceToMid > allEntries[j].DistanceToMid
		})
		for _, entry := range allEntries {
			if extraCancelNeeded <= 0 {
				break
			}
			if _, exists := selectedOrderIDs[entry.OrderID]; exists {
				continue
			}
			toCancel = append(toCancel, entry)
			selectedOrderIDs[entry.OrderID] = struct{}{}
			extraCancelNeeded--
		}
	}
	if len(toCancel) == 0 {
		return false, nil
	}
	sort.Slice(toCancel, func(i, j int) bool {
		if toCancel[i].OrderSide != toCancel[j].OrderSide {
			return toCancel[i].OrderSide < toCancel[j].OrderSide
		}
		if toCancel[i].DistanceToMid == toCancel[j].DistanceToMid {
			return toCancel[i].OrderID > toCancel[j].OrderID
		}
		return toCancel[i].DistanceToMid > toCancel[j].DistanceToMid
	})
	batchSize := spm.config.Trading.CleanupBatchSize
	if batchSize <= 0 {
		batchSize = 10
	}
	if len(toCancel) > batchSize {
		toCancel = toCancel[:batchSize]
	}
	orderIDs := make([]int64, 0, len(toCancel))
	for _, candidate := range toCancel {
		orderIDs = append(orderIDs, candidate.OrderID)
	}
	logger.Info("🧭 [经典网格平仓让位] 平仓保护缺少侧边预算，撤销 %d 个远端开仓单后立即补回平仓单", len(orderIDs))

	spm.mu.Unlock()
	err := spm.executor.BatchCancelOrders(orderIDs)
	spm.mu.Lock()
	if err != nil {
		logger.Warn("⚠️ [经典网格平仓让位] 撤销远端开仓单失败: %v", err)
		return false, nil
	}
	for _, candidate := range toCancel {
		spm.UpdateSlotOrderStatusIfCurrent(candidate.SlotPrice, candidate.BookSide, OrderStatusCanceled, candidate.OrderID, candidate.ClientOID)
	}
	return true, nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// OnOrderUpdate 订单更新回调（异步订单同步流）
func (spm *SuperPositionManager) OnOrderUpdate(update OrderUpdate) bool {
	update.ClientOrderID = spm.normalizeClientOrderID(update.ClientOrderID)

	// 🔥 重构：完全依赖 ClientOrderID 解析
	price, side, bookSide, valid := spm.parseClientOrderID(update.ClientOrderID)

	if !valid {
		logger.Debug("⏳ [忽略] 无法识别的订单更新: ID=%d, ClientOID=%s", update.OrderID, update.ClientOrderID)
		return false
	}

	slot := spm.getOrCreateSlot(price, bookSide)
	slot.mu.Lock()
	defer slot.mu.Unlock()
	slot.BookSide = bookSide
	status := normalizeOrderUpdateStatus(update.Status)
	isTerminalStatus := status == "FILLED" || status == "CANCELED" || status == "EXPIRED" || status == "REJECTED"
	if isTerminalStatus && !spm.orderUpdateMatchesCurrentSlot(slot, update) {
		spm.rememberOrderFillProgress(update.ClientOrderID, update.ExecutedQty, true)
		logger.Debug("⏳ [订单更新被忽略] 旧订单终态不匹配当前槽位: slot=%s, currentOrderID=%d, currentClientOID=%s, updateOrderID=%d, updateClientOID=%s, status=%s",
			formatPrice(price, spm.priceDecimals), slot.OrderID, slot.ClientOID, update.OrderID, update.ClientOrderID, status)
		return false
	}
	if spm.isStaleAfterTerminalOrderUpdate(update.ClientOrderID, update.ExecutedQty, status) {
		logger.Debug("⏳ [订单更新被忽略] 已处理终态后的延迟推送: slot=%s, clientOID=%s, status=%s, executed=%.8f",
			formatPrice(price, spm.priceDecimals), update.ClientOrderID, status, update.ExecutedQty)
		return false
	}

	// 校验：确保这个更新属于当前的订单 (防止旧订单的延迟推送干扰新订单)
	// 优先使用 ClientOrderID 匹配 (某些交易所如 Gate.io 的 OrderID 可能略有差异)
	if slot.ClientOID != "" && slot.ClientOID != update.ClientOrderID {
		// ClientOrderID 不匹配，忽略此更新
		logger.Info("⚠️ [订单更新被忽略] 槽位 %.2f: ClientOID不匹配 (槽位: %s, 推送: %s, OrderID: %d)",
			price, slot.ClientOID, update.ClientOrderID, update.OrderID)
		return false
	}
	if slot.ClientOID == "" && slot.OrderStatus == OrderStatusCanceled && slot.SlotStatus != SlotStatusPending && status == "NEW" {
		logger.Debug("⏳ [订单更新被忽略] 已取消槽位收到延迟NEW: slot=%s, clientOID=%s, orderID=%d",
			formatPrice(price, spm.priceDecimals), update.ClientOrderID, update.OrderID)
		return false
	}

	// 更新订单ID (如果是首个推送)
	if slot.OrderID == 0 && update.OrderID != 0 {
		logger.Debug("📝 [首次设置OrderID] 槽位 %.2f: OrderID=%d, ClientOID=%s", price, update.OrderID, update.ClientOrderID)
		slot.OrderID = update.OrderID
		slot.ClientOID = update.ClientOrderID
		slot.OrderSide = side
	} else if slot.OrderID != 0 && update.OrderID != 0 && slot.OrderID != update.OrderID {
		// OrderID 不一致但 ClientOrderID 匹配，更新 OrderID (Gate.io 批量下单可能出现此情况)
		logger.Debug("📝 [更新OrderID] 槽位 %.2f: %d -> %d (ClientOID: %s)", price, slot.OrderID, update.OrderID, update.ClientOrderID)
		slot.OrderID = update.OrderID
	}
	if slot.ClientOID == "" {
		slot.ClientOID = update.ClientOrderID
	}
	if slot.OrderSide == "" {
		slot.OrderSide = side
	}

	deltaQty := 0.0
	if isFillBearingOrderUpdateStatus(status) {
		deltaQty = spm.orderUpdateDeltaQty(update.ClientOrderID, update.ExecutedQty, slot.OrderFilledQty, isTerminalStatus)
		if update.ExecutedQty > slot.OrderFilledQty {
			slot.OrderFilledQty = update.ExecutedQty
		}
	}

	// 处理状态转换
	switch status {
	case "NEW":
		slot.OrderStatus = OrderStatusConfirmed
		slot.SlotStatus = SlotStatusLocked
		if update.Price > 0 {
			slot.OrderPrice = update.Price
		}
		if slot.OrderCreatedAt.IsZero() {
			slot.OrderCreatedAt = time.Now()
		}

	case "PARTIALLY_FILLED", "FILLED":
		isEntry := spm.isEntryOrder(side, bookSide)
		if isEntry {
			if deltaQty > 0 {
				tradePrice := firstPositiveFloat(update.AvgPrice, update.Price, price)
				spm.applyEntryFill(slot, tradePrice, deltaQty)
				spm.addTradedQty(side, deltaQty)
			}

			if status == "FILLED" {
				clearSlotOrderTracking(slot, OrderStatusNotPlaced)

				slot.PositionStatus = PositionStatusFilled
				slot.SlotStatus = SlotStatusFree
				slot.PostOnlyFailCount = 0
				logger.Info("✅ [开仓成交-%s] 价格: %s, 持仓: %.4f, 订单方向: %s", bookSide, formatPrice(price, spm.priceDecimals), slot.PositionQty, side)
				return true
			} else {
				slot.OrderStatus = OrderStatusPartiallyFilled
			}

		} else {
			if deltaQty > 0 {
				spm.addTradedQty(side, deltaQty)
				tradePrice := firstPositiveFloat(update.AvgPrice, update.Price, spm.exitPrice(price, bookSide))
				spm.applyExitFill(slot, tradePrice, deltaQty, bookSide)
			}

			if status == "FILLED" {
				clearSlotOrderTracking(slot, OrderStatusNotPlaced)

				if slot.PositionQty < 0.000001 {
					slot.PositionStatus = PositionStatusEmpty
				}
				slot.SlotStatus = SlotStatusFree
				slot.PostOnlyFailCount = 0
				logger.Info("✅ [平仓成交-%s] 价格: %s, 剩余持仓: %.4f, 订单方向: %s", bookSide, formatPrice(price, spm.priceDecimals), slot.PositionQty, side)
				return true
			} else {
				slot.OrderStatus = OrderStatusPartiallyFilled
			}
		}

	case "CANCELED", "EXPIRED", "REJECTED":
		isEntry := spm.isEntryOrder(side, bookSide)
		if deltaQty > 0 {
			if isEntry {
				tradePrice := firstPositiveFloat(update.AvgPrice, update.Price, price)
				spm.applyEntryFill(slot, tradePrice, deltaQty)
			} else {
				tradePrice := firstPositiveFloat(update.AvgPrice, update.Price, spm.exitPrice(price, bookSide))
				spm.applyExitFill(slot, tradePrice, deltaQty, bookSide)
			}
			spm.addTradedQty(side, deltaQty)
		}

		logger.Info("⚠️ [订单%s] 价格: %s, 方向: %s, 原因: %s, 已成交: %.4f",
			status, formatPrice(price, spm.priceDecimals), side, status, slot.OrderFilledQty)

		// 🔥 核心修复：根据订单方向和成交情况处理槽位状态
		if isEntry {
			// 买单被取消/拒绝
			if slot.PositionQty > 0 || slot.OrderFilledQty > 0 {
				logger.Info("💡 [开仓单部分成交后取消-%s] 价格: %s, 持仓: %.4f",
					bookSide,
					formatPrice(price, spm.priceDecimals), slot.PositionQty)
				slot.PositionStatus = PositionStatusFilled
				slot.SlotStatus = SlotStatusFree
			} else {
				logger.Info("🔄 [开仓单未成交取消-%s] 价格: %s, 重置槽位为空闲",
					bookSide,
					formatPrice(price, spm.priceDecimals))
				slot.PositionStatus = PositionStatusEmpty
				slot.SlotStatus = SlotStatusFree
			}
		} else {
			if slot.PositionQty > 0 {
				if status == "REJECTED" || status == "EXPIRED" {
					slot.PostOnlyFailCount++
					logger.Info("🔄 [平仓单%s-%s] 价格: %s, 保持持仓状态: %.4f, PostOnly失败计数: %d",
						status, bookSide, formatPrice(price, spm.priceDecimals), slot.PositionQty, slot.PostOnlyFailCount)
				} else {
					logger.Info("🔄 [平仓单取消-%s] 价格: %s, 保持持仓状态: %.4f",
						bookSide, formatPrice(price, spm.priceDecimals), slot.PositionQty)
				}
				slot.PositionStatus = PositionStatusFilled
				slot.SlotStatus = SlotStatusFree
			} else {
				logger.Warn("⚠️ [异常] 平仓单取消但无持仓，价格: %s, 重置为空", formatPrice(price, spm.priceDecimals))
				slot.PositionStatus = PositionStatusEmpty
				slot.SlotStatus = SlotStatusFree
			}
		}

		clearSlotOrderTracking(slot, OrderStatusCanceled)
		return true
	}
	return false
}

func normalizeOrderUpdateStatus(status string) string {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "NEW", "OPEN", "LIVE", "CREATED":
		return "NEW"
	case "PARTIALLY_FILLED_CANCELED", "PARTIAL_FILLED_CANCELED", "PARTIALLY_FILLED_CANCELLED", "PARTIAL_FILLED_CANCELLED":
		return "CANCELED"
	case "PARTIALLY_FILLED", "PARTIALLYFILLED", "PARTIAL_FILL", "PARTIAL_FILLED":
		return "PARTIALLY_FILLED"
	case "FILLED", "FULLY_FILLED":
		return "FILLED"
	case "CANCELED", "CANCELLED":
		return "CANCELED"
	case "EXPIRED":
		return "EXPIRED"
	case "REJECTED":
		return "REJECTED"
	default:
		return strings.ToUpper(strings.TrimSpace(status))
	}
}

func isFillBearingOrderUpdateStatus(status string) bool {
	switch status {
	case "PARTIALLY_FILLED", "FILLED", "CANCELED", "EXPIRED", "REJECTED":
		return true
	default:
		return false
	}
}

func (spm *SuperPositionManager) isStaleAfterTerminalOrderUpdate(clientOrderID string, executedQty float64, status string) bool {
	value, ok := spm.orderFills.Load(strings.TrimSpace(clientOrderID))
	if !ok {
		return false
	}
	progress := value.(orderFillProgress)
	if !progress.Terminal {
		return false
	}
	if status == "FILLED" || status == "CANCELED" || status == "EXPIRED" || status == "REJECTED" {
		return false
	}
	return executedQty <= progress.ExecutedQty
}

func (spm *SuperPositionManager) orderUpdateDeltaQty(clientOrderID string, executedQty, slotFilledQty float64, terminal bool) float64 {
	if executedQty <= 0 {
		if terminal {
			spm.rememberOrderFillProgress(clientOrderID, 0, true)
		}
		return 0
	}
	processedQty := slotFilledQty
	if value, ok := spm.orderFills.Load(clientOrderID); ok {
		progress := value.(orderFillProgress)
		if progress.ExecutedQty > processedQty {
			processedQty = progress.ExecutedQty
		}
	}
	deltaQty := executedQty - processedQty
	if deltaQty <= 0 {
		if terminal {
			spm.rememberOrderFillProgress(clientOrderID, executedQty, true)
		}
		return 0
	}
	spm.rememberOrderFillProgress(clientOrderID, executedQty, terminal)
	return deltaQty
}

func (spm *SuperPositionManager) orderUpdateMatchesCurrentSlot(slot *InventorySlot, update OrderUpdate) bool {
	clientOID := strings.TrimSpace(update.ClientOrderID)
	if slot.ClientOID != "" && clientOID != "" {
		return slot.ClientOID == clientOID
	}
	if slot.OrderID != 0 && update.OrderID != 0 {
		return slot.OrderID == update.OrderID
	}
	if slot.OrderID == 0 && slot.ClientOID == "" {
		return true
	}
	return false
}

func clearSlotOrderTracking(slot *InventorySlot, status string) {
	slot.OrderStatus = status
	slot.OrderID = 0
	slot.ClientOID = ""
	slot.OrderSide = ""
	slot.OrderPrice = 0
	slot.OrderFilledQty = 0
	slot.OrderCreatedAt = time.Time{}
}

func (spm *SuperPositionManager) activeOrderPlacementKey(slot *InventorySlot) (string, bool, bool) {
	if slot == nil {
		return "", false, false
	}
	if !spm.slotHasActiveOrder(slot) {
		return "", false, false
	}
	if slot.OrderSide == "" || slot.BookSide == "" {
		return "", false, false
	}
	price := slot.OrderPrice
	if price <= 0 {
		price = slot.Price
	}
	hasIdentity := strings.TrimSpace(slot.ClientOID) != "" || slot.OrderID != 0
	blocksDuplicate := spm.isEntryOrder(slot.OrderSide, slot.BookSide) || !hasIdentity
	return spm.orderPlacementKey(slot.OrderSide, slot.BookSide, price), blocksDuplicate, true
}

func (spm *SuperPositionManager) slotHasActiveOrder(slot *InventorySlot) bool {
	if slot == nil {
		return false
	}
	if slot.SlotStatus == SlotStatusPending {
		if strings.TrimSpace(slot.ClientOID) != "" || slot.OrderID != 0 {
			return true
		}
		return isActiveOrderStatus(slot.OrderStatus) && strings.TrimSpace(slot.OrderSide) != ""
	}
	if !isActiveOrderStatus(slot.OrderStatus) {
		return false
	}
	return slot.OrderID != 0 || strings.TrimSpace(slot.ClientOID) != ""
}

func (spm *SuperPositionManager) cleanupGhostOrderStates() {
	var cleaned int
	now := time.Now()
	spm.slots.Range(func(key, value interface{}) bool {
		slot := value.(*InventorySlot)
		slot.mu.Lock()
		clientOID := strings.TrimSpace(slot.ClientOID)
		if slot.SlotStatus == SlotStatusPending &&
			slot.OrderID == 0 &&
			clientOID == "" &&
			slot.OrderFilledQty == 0 &&
			(slot.OrderCreatedAt.IsZero() || now.Sub(slot.OrderCreatedAt) > pendingOrderIdentityGrace) {
			logger.Warn("🧹 [槽位修复] 清理无标识待提交订单状态: price=%s book=%s side=%s status=%s",
				formatPrice(slot.Price, spm.priceDecimals), slot.BookSide, slot.OrderSide, slot.OrderStatus)
			slot.SlotStatus = SlotStatusFree
			slot.OrderStatus = OrderStatusNotPlaced
			slot.OrderSide = ""
			slot.OrderPrice = 0
			slot.OrderCreatedAt = time.Time{}
			cleaned++
		}
		if slot.SlotStatus == SlotStatusFree &&
			slot.OrderID == 0 &&
			clientOID == "" &&
			slot.OrderFilledQty == 0 &&
			isActiveOrderStatus(slot.OrderStatus) {
			logger.Warn("🧹 [槽位修复] 清理幽灵订单状态: price=%s book=%s side=%s status=%s",
				formatPrice(slot.Price, spm.priceDecimals), slot.BookSide, slot.OrderSide, slot.OrderStatus)
			slot.OrderStatus = OrderStatusNotPlaced
			slot.OrderSide = ""
			slot.OrderPrice = 0
			slot.OrderCreatedAt = time.Time{}
			cleaned++
		}
		slot.mu.Unlock()
		return true
	})
	if cleaned > 0 {
		logger.Warn("🧹 [槽位修复] 已清理 %d 个幽灵订单状态，允许立即补单", cleaned)
	}
}

func (spm *SuperPositionManager) orderPlacementKey(side, bookSide string, orderPrice float64) string {
	return fmt.Sprintf("%s:%s:%s", bookSide, strings.ToUpper(strings.TrimSpace(side)), formatPrice(roundPrice(orderPrice, spm.priceDecimals), spm.priceDecimals))
}

func (spm *SuperPositionManager) attachMissingClientOrderIDs(requests []*OrderRequest, placed []*Order) {
	if len(requests) == 0 || len(placed) == 0 {
		return
	}
	unused := make(map[int]struct{}, len(requests))
	for i := range requests {
		unused[i] = struct{}{}
	}
	for _, ord := range placed {
		if ord == nil || strings.TrimSpace(ord.ClientOrderID) == "" {
			continue
		}
		for i, req := range requests {
			if _, ok := unused[i]; !ok {
				continue
			}
			if req.ClientOrderID == ord.ClientOrderID {
				delete(unused, i)
				break
			}
		}
	}
	for _, ord := range placed {
		if ord == nil || strings.TrimSpace(ord.ClientOrderID) != "" {
			continue
		}
		for i, req := range requests {
			if _, ok := unused[i]; !ok {
				continue
			}
			if !sameOrderRequestAndPlaced(req, ord, spm.priceDecimals) {
				continue
			}
			ord.ClientOrderID = req.ClientOrderID
			delete(unused, i)
			logger.Warn("⚠️ [下单回填] 交易所返回缺少 ClientOID，已按请求回填: orderID=%d clientOID=%s",
				ord.OrderID, ord.ClientOrderID)
			break
		}
	}
}

func sameOrderRequestAndPlaced(req *OrderRequest, ord *Order, priceDecimals int) bool {
	if req == nil || ord == nil {
		return false
	}
	if !strings.EqualFold(req.Side, ord.Side) {
		return false
	}
	if roundPrice(req.Price, priceDecimals) != roundPrice(ord.Price, priceDecimals) {
		return false
	}
	if math.Abs(req.Quantity-ord.Quantity) > 1e-12 {
		return false
	}
	if req.Symbol != "" && ord.Symbol != "" && !sameTradingSymbol(req.Symbol, ord.Symbol) {
		return false
	}
	return true
}

func (spm *SuperPositionManager) rememberOrderFillProgress(clientOrderID string, executedQty float64, terminal bool) {
	clientOrderID = strings.TrimSpace(clientOrderID)
	if clientOrderID == "" {
		return
	}
	next := orderFillProgress{ExecutedQty: executedQty, Terminal: terminal, UpdatedAt: time.Now()}
	if value, ok := spm.orderFills.Load(clientOrderID); ok {
		prev := value.(orderFillProgress)
		if prev.ExecutedQty > next.ExecutedQty {
			next.ExecutedQty = prev.ExecutedQty
		}
		next.Terminal = prev.Terminal || terminal
	}
	spm.orderFills.Store(clientOrderID, next)
	spm.pruneOrderFillProgress(next.UpdatedAt)
}

func (spm *SuperPositionManager) pruneOrderFillProgress(now time.Time) {
	const maxRememberedOrderFills = 4096
	const orderFillProgressTTL = 24 * time.Hour

	count := 0
	var oldestKey string
	var oldestTime time.Time
	spm.orderFills.Range(func(key, value interface{}) bool {
		progress := value.(orderFillProgress)
		count++
		if now.Sub(progress.UpdatedAt) > orderFillProgressTTL {
			spm.orderFills.Delete(key)
			return true
		}
		if oldestTime.IsZero() || progress.UpdatedAt.Before(oldestTime) {
			oldestTime = progress.UpdatedAt
			oldestKey = key.(string)
		}
		return true
	})
	if count > maxRememberedOrderFills && oldestKey != "" {
		spm.orderFills.Delete(oldestKey)
	}
}

// getOrCreateSlot 获取或创建槽位
func (spm *SuperPositionManager) slotKey(price float64, bookSide string) string {
	if bookSide == "" {
		bookSide = BookSideLong
	}
	return fmt.Sprintf("%s:%s", bookSide, formatPrice(price, spm.priceDecimals))
}

func (spm *SuperPositionManager) getOrCreateSlot(price float64, bookSide string) *InventorySlot {
	if bookSide == "" {
		bookSide = BookSideLong
	}
	key := spm.slotKey(price, bookSide)
	if slot, exists := spm.slots.Load(key); exists {
		return slot.(*InventorySlot)
	}

	// 创建新槽位
	slot := &InventorySlot{
		Price:          price,
		PositionStatus: PositionStatusEmpty,
		PositionQty:    0,
		BookSide:       bookSide,
		OrderStatus:    OrderStatusNotPlaced,
		SlotStatus:     SlotStatusFree, // 🔥 初始化为FREE状态
	}
	actual, _ := spm.slots.LoadOrStore(key, slot)
	return actual.(*InventorySlot)
}

func (spm *SuperPositionManager) pruneEmptyFarSlots(currentGridPrice float64, retainIntervals int) {
	if retainIntervals <= 0 || spm.config.Trading.PriceInterval <= 0 {
		return
	}
	maxDistance := spm.config.Trading.PriceInterval * float64(retainIntervals)
	pruned := 0
	spm.slots.Range(func(key, value interface{}) bool {
		slot := value.(*InventorySlot)
		if math.Abs(slot.Price-currentGridPrice) <= maxDistance {
			return true
		}
		slot.mu.RLock()
		canDelete := slot.PositionStatus == PositionStatusEmpty &&
			slot.PositionQty <= 0 &&
			slot.OrderID == 0 &&
			slot.ClientOID == "" &&
			slot.OrderFilledQty == 0 &&
			slot.SlotStatus == SlotStatusFree &&
			!spm.slotHasActiveOrder(slot)
		slot.mu.RUnlock()
		if canDelete {
			spm.slots.Delete(key)
			pruned++
		}
		return true
	})
	if pruned > 0 {
		logger.Debug("🧹 [槽位剪枝] 清理远端空槽位: %d", pruned)
	}
}

// findNearestGridPrice returns the nearest interval-aligned grid point.
// Keeping the grid aligned to the startup anchor matches the reference bot:
// the window moves as soon as the latest price maps to a new nearest grid.
func (spm *SuperPositionManager) findNearestGridPrice(currentPrice float64) float64 {
	currentPrice = roundPrice(currentPrice, spm.priceDecimals)
	priceInterval := spm.config.Trading.PriceInterval
	if priceInterval <= 0 || spm.anchorPrice <= 0 {
		return currentPrice
	}
	intervals := math.Round((currentPrice - spm.anchorPrice) / priceInterval)
	gridPrice := spm.anchorPrice + intervals*priceInterval
	return roundPrice(gridPrice, spm.priceDecimals)
}

// calculateSlotPrices 计算槽位价格列表（统一的网格计算方法）
// 这个方法确保初始化和实时调整计算出完全相同的槽位价格
// 参数：
//   - gridPrice: 网格价格（使用锚点价格）
//   - count: 需要计算的槽位数量
//   - direction: 方向，"down"表示向下（买单），"up"表示向上（卖单）
//
// 返回：槽位价格列表，从网格价格开始，按价格间隔递减或递增，使用检测到的价格精度
func (spm *SuperPositionManager) calculateSlotPrices(gridPrice float64, count int, direction string) []float64 {
	var prices []float64
	priceInterval := spm.config.Trading.PriceInterval

	for i := 0; i < count; i++ {
		var price float64
		if direction == "down" {
			// 向下：网格价格 - i * 间隔
			price = gridPrice - float64(i)*priceInterval
		} else {
			// 向上：网格价格 + i * 间隔
			price = gridPrice + float64(i)*priceInterval
		}
		// 使用检测到的价格精度进行舍入
		price = roundPrice(price, spm.priceDecimals)
		prices = append(prices, price)
	}

	return prices
}

func (spm *SuperPositionManager) calculateEntrySlotPrices(gridPrice, currentPrice float64, count int, bookSide string) []float64 {
	if spm.isAggressiveMode() && spm.tradingDirection() != "neutral" {
		direction := "down"
		if bookSide == BookSideShort {
			direction = "up"
		}
		raw := spm.calculateSlotPrices(gridPrice, count, direction)
		prices := make([]float64, 0, len(raw))
		for _, price := range raw {
			if price <= 0 || math.IsNaN(price) || math.IsInf(price, 0) {
				continue
			}
			if !spm.isMakerSafeEntryPrice(price, currentPrice, bookSide) {
				continue
			}
			prices = append(prices, price)
		}
		return prices
	}
	if count <= 0 {
		return nil
	}
	prices := make([]float64, 0, count)
	seen := make(map[float64]struct{}, count)
	priceInterval := spm.config.Trading.PriceInterval
	if priceInterval <= 0 {
		return prices
	}
	direction := spm.entrySlotDirection(bookSide)
	maxAttempts := count*4 + 20
	for i := 1; len(prices) < count && i <= maxAttempts; i++ {
		price := gridPrice
		if direction == "down" {
			price = gridPrice - float64(i)*priceInterval
		} else {
			price = gridPrice + float64(i)*priceInterval
		}
		price = roundPrice(price, spm.priceDecimals)
		if price <= 0 || math.IsNaN(price) || math.IsInf(price, 0) {
			if direction == "down" {
				break
			}
			continue
		}
		if _, exists := seen[price]; exists {
			continue
		}
		if !spm.isMakerSafeEntryPrice(price, currentPrice, bookSide) {
			continue
		}
		seen[price] = struct{}{}
		prices = append(prices, price)
	}
	return prices
}

func (spm *SuperPositionManager) calculateAvailableEntrySlotPrices(gridPrice, currentPrice float64, count int, bookSide string) []float64 {
	if count <= 0 {
		return nil
	}
	maxCandidates := count*4 + 20
	if maxCandidates < count {
		maxCandidates = count
	}
	raw := spm.calculateEntrySlotPrices(gridPrice, currentPrice, maxCandidates, bookSide)
	prices := make([]float64, 0, count)
	for _, price := range raw {
		if !spm.entrySlotCanHostWindowOrder(price, bookSide) {
			continue
		}
		prices = append(prices, price)
		if len(prices) >= count {
			break
		}
	}
	return prices
}

func (spm *SuperPositionManager) entrySlotCanHostWindowOrder(price float64, bookSide string) bool {
	value, ok := spm.slots.Load(spm.slotKey(price, bookSide))
	if !ok {
		return true
	}
	slot := value.(*InventorySlot)
	slot.mu.RLock()
	defer slot.mu.RUnlock()
	if spm.slotHasActiveOrder(slot) {
		return spm.isEntryOrder(slot.OrderSide, slot.BookSide)
	}
	return slot.PositionStatus == PositionStatusEmpty && slot.SlotStatus == SlotStatusFree
}

func (spm *SuperPositionManager) calculateExitSlotPrices(gridPrice, currentPrice float64, count int, bookSide string) []float64 {
	if count <= 0 {
		return nil
	}
	prices := make([]float64, 0, count)
	seen := make(map[float64]struct{}, count)
	priceInterval := spm.config.Trading.PriceInterval
	if priceInterval <= 0 {
		return prices
	}
	direction := "up"
	if bookSide == BookSideShort {
		direction = "down"
	}
	maxAttempts := count*4 + 20
	for i := 2; len(prices) < count && i <= maxAttempts+1; i++ {
		price := gridPrice
		if direction == "down" {
			price = gridPrice - float64(i)*priceInterval
		} else {
			price = gridPrice + float64(i)*priceInterval
		}
		price = roundPrice(price, spm.priceDecimals)
		if price <= 0 || math.IsNaN(price) || math.IsInf(price, 0) {
			if direction == "down" {
				break
			}
			continue
		}
		if _, exists := seen[price]; exists {
			continue
		}
		if !spm.isMakerSafeExitPrice(price, currentPrice, bookSide) {
			continue
		}
		seen[price] = struct{}{}
		prices = append(prices, price)
	}
	return prices
}

func (spm *SuperPositionManager) nextDesiredExitPrice(bookSide string, desiredExitSlots map[string][]float64, used map[string]map[float64]bool) (float64, bool) {
	for _, price := range desiredExitSlots[bookSide] {
		price = roundPrice(price, spm.priceDecimals)
		if used[bookSide] != nil && used[bookSide][price] {
			continue
		}
		return price, true
	}
	return 0, false
}

func (spm *SuperPositionManager) entryOrderQuantity(price float64) (float64, bool) {
	if price <= 0 || math.IsNaN(price) || math.IsInf(price, 0) {
		return 0, false
	}
	quantity := spm.maxConfiguredOrderQuantity(price)
	if quantity <= 0 || math.IsNaN(quantity) || math.IsInf(quantity, 0) {
		logger.Warn("⚠️ [跳过开仓] 价格 %s 的数量按精度取整后为 0，请提高单笔金额或调整交易对",
			formatPrice(price, spm.priceDecimals))
		return 0, false
	}
	orderValue := price * quantity
	minValue := spm.minOrderValue()
	if orderValue+1e-9 < minValue {
		logger.Warn("⚠️ [跳过开仓] 价格 %s 数量 %.8f 订单价值 %.4f 小于最小订单额 %.4f",
			formatPrice(price, spm.priceDecimals), quantity, orderValue, minValue)
		return 0, false
	}
	return quantity, true
}

func (spm *SuperPositionManager) exitOrderQuantity(positionQty, price float64) (float64, bool) {
	if positionQty <= 0 || price <= 0 || math.IsNaN(positionQty) || math.IsInf(positionQty, 0) || math.IsNaN(price) || math.IsInf(price, 0) {
		return 0, false
	}
	if spm.isAggressiveMode() && spm.tradingDirection() != "neutral" {
		quantity := roundQuantity(positionQty, spm.quantityDecimals)
		if quantity <= 0 {
			return 0, false
		}
		return quantity, true
	}
	maxQty := spm.maxConfiguredOrderQuantity(price)
	if maxQty <= 0 {
		logger.Warn("⚠️ [跳过平仓] 价格 %s 的单笔平仓数量按配置金额取整后为 0，避免放大底仓平仓单",
			formatPrice(price, spm.priceDecimals))
		return 0, false
	}
	quantity := positionQty
	if quantity > maxQty {
		quantity = maxQty
	}
	quantity = roundQuantity(quantity, spm.quantityDecimals)
	if quantity <= 0 {
		return 0, false
	}
	remainingAfterOrder := roundQuantity(positionQty-quantity, spm.quantityDecimals)
	if remainingAfterOrder > 0 && remainingAfterOrder*price < spm.minOrderValue() {
		quantity = roundQuantity(positionQty, spm.quantityDecimals)
		if quantity <= 0 {
			return 0, false
		}
	}
	return quantity, true
}

func (spm *SuperPositionManager) maxConfiguredOrderQuantity(price float64) float64 {
	if price <= 0 {
		return 0
	}
	return floorQuantity(spm.config.Trading.OrderQuantity/price, spm.quantityDecimals)
}

func (spm *SuperPositionManager) minOrderValue() float64 {
	minValue := spm.config.Trading.MinOrderValue
	if minValue <= 0 {
		minValue = 6.0
	}
	return minValue
}

func (spm *SuperPositionManager) isMakerSafeEntryPrice(price, currentPrice float64, bookSide string) bool {
	safetyBuffer := spm.config.Trading.PriceInterval * 0.1
	if bookSide == BookSideShort {
		return price > currentPrice+safetyBuffer
	}
	return price < currentPrice-safetyBuffer
}

func (spm *SuperPositionManager) isMakerSafeExitPrice(price, currentPrice float64, bookSide string) bool {
	safetyBuffer := spm.config.Trading.PriceInterval * 0.1
	if bookSide == BookSideShort {
		return price < currentPrice-safetyBuffer
	}
	return price > currentPrice+safetyBuffer
}

func (spm *SuperPositionManager) makerSafeExitPrice(targetPrice, currentPrice float64, bookSide string) float64 {
	priceInterval := spm.config.Trading.PriceInterval
	if priceInterval <= 0 || currentPrice <= 0 {
		return roundPrice(targetPrice, spm.priceDecimals)
	}
	safetyBuffer := priceInterval * 0.1
	if bookSide == BookSideShort {
		if targetPrice < currentPrice-safetyBuffer {
			return roundPrice(targetPrice, spm.priceDecimals)
		}
		return spm.firstGridBelow(currentPrice, safetyBuffer)
	}
	if targetPrice > currentPrice+safetyBuffer {
		return roundPrice(targetPrice, spm.priceDecimals)
	}
	return spm.firstGridAbove(currentPrice, safetyBuffer)
}

func (spm *SuperPositionManager) nextUnusedMakerSafeExitPrice(targetPrice, currentPrice float64, bookSide, exitSide string, used map[string]bool) (float64, bool) {
	priceInterval := spm.config.Trading.PriceInterval
	if priceInterval <= 0 {
		return 0, false
	}
	price := spm.makerSafeExitPrice(targetPrice, currentPrice, bookSide)
	maxAttempts := spm.config.Trading.BuyWindowSize + spm.config.Trading.SellWindowSize + 20
	if maxAttempts < 20 {
		maxAttempts = 20
	}
	for i := 0; i < maxAttempts; i++ {
		if price <= 0 || math.IsNaN(price) || math.IsInf(price, 0) {
			return 0, false
		}
		key := spm.orderPlacementKey(exitSide, bookSide, price)
		if !used[key] {
			return price, true
		}
		if bookSide == BookSideShort {
			price = roundPrice(price-priceInterval, spm.priceDecimals)
		} else {
			price = roundPrice(price+priceInterval, spm.priceDecimals)
		}
	}
	return 0, false
}

func (spm *SuperPositionManager) firstGridAbove(currentPrice, safetyBuffer float64) float64 {
	priceInterval := spm.config.Trading.PriceInterval
	price := roundPrice(currentPrice+priceInterval, spm.priceDecimals)
	for price <= currentPrice+safetyBuffer {
		price = roundPrice(price+priceInterval, spm.priceDecimals)
	}
	return price
}

func (spm *SuperPositionManager) firstGridBelow(currentPrice, safetyBuffer float64) float64 {
	priceInterval := spm.config.Trading.PriceInterval
	price := roundPrice(currentPrice-priceInterval, spm.priceDecimals)
	for price >= currentPrice-safetyBuffer {
		price = roundPrice(price-priceInterval, spm.priceDecimals)
	}
	return price
}

// ===== IPositionManager 接口实现（供 safety.Reconciler 使用）=====
// 注意：以下方法是 safety/reconciler.go 中 IPositionManager 接口的实现，
// 被 Reconciler 对账器调用，不可删除或修改签名

// SlotData 槽位数据结构（用于传递给外部）
type SlotData struct {
	Price          float64
	PositionStatus string
	PositionQty    float64
	BookSide       string
	OrderID        int64
	ClientOID      string
	OrderSide      string
	OrderStatus    string
	OrderCreatedAt time.Time
	SlotStatus     string
}

type exchangeOrderSnapshot struct {
	OrderID       int64
	ClientOrderID string
	Side          string
	Status        string
	Price         float64
	Quantity      float64
	ExecutedQty   float64
	AvgPrice      float64
	UpdateTime    int64
}

type exchangePositionSnapshot struct {
	Symbol     string
	Size       float64
	EntryPrice float64
	MarkPrice  float64
}

type staleOrderRef struct {
	OrderID       int64
	ClientOrderID string
	CreatedAt     time.Time
}

type entryWindowSyncCandidate struct {
	SlotPrice float64
	BookSide  string
	OrderID   int64
	ClientOID string
	Distance  float64
}

type entryWindowHole struct {
	Price    float64
	Distance float64
}

type exitRebalancePressure struct {
	Count           int
	ClosestDistance float64
}

type activeOrderCandidate struct {
	SlotPrice     float64
	BookSide      string
	OrderSide     string
	OrderPrice    float64
	OrderID       int64
	ClientOID     string
	DistanceToMid float64
}

type exitOrderCandidate struct {
	SlotPrice         float64
	ExitPrice         float64
	Quantity          float64
	DistanceToMid     float64
	ExitDistanceToMid float64
	BookSide          string
}

// IterateSlots 遍历所有槽位（封装 sync.Map.Range）
// 注意：为了避免类型冲突，这里使用 interface{} 返回槽位数据
// 调用者需要将其转换为具体的槽位信息
func (spm *SuperPositionManager) IterateSlots(fn func(price float64, slot interface{}) bool) {
	spm.slots.Range(func(key, value interface{}) bool {
		slot := value.(*InventorySlot)
		price := slot.Price
		slot.mu.RLock()
		defer slot.mu.RUnlock()

		// 构造槽位数据
		data := SlotData{
			Price:          price,
			PositionStatus: slot.PositionStatus,
			PositionQty:    slot.PositionQty,
			OrderID:        slot.OrderID,
			ClientOID:      slot.ClientOID,
			OrderSide:      slot.OrderSide,
			OrderStatus:    slot.OrderStatus,
			OrderCreatedAt: slot.OrderCreatedAt,
			BookSide:       slot.BookSide,
			SlotStatus:     slot.SlotStatus,
		}

		// 返回槽位数据
		return fn(price, data)
	})
}

// ApplyExchangeSnapshot 用交易所 REST 快照修正本地槽位，补上 WS 漏推或重启后丢失的活跃订单。
func (spm *SuperPositionManager) ApplyExchangeSnapshot(positionsRaw interface{}, openOrdersRaw interface{}) {
	orders := spm.extractExchangeOrders(openOrdersRaw)
	positions := spm.extractExchangePositions(positionsRaw)
	entryPriceByBook := make(map[string]float64, 2)
	for _, pos := range positions {
		if pos.Symbol != "" && !sameTradingSymbol(pos.Symbol, spm.config.Trading.Symbol) {
			continue
		}
		if pos.Size > 0 && pos.EntryPrice > 0 {
			entryPriceByBook[BookSideLong] = pos.EntryPrice
		} else if pos.Size < 0 && pos.EntryPrice > 0 {
			entryPriceByBook[BookSideShort] = pos.EntryPrice
		}
	}
	openOrderIDs := make(map[int64]struct{}, len(orders))
	openClientOIDs := make(map[string]struct{}, len(orders))
	var cancelSnapshotOrderIDs []int64

	for _, order := range orders {
		if order.OrderID != 0 {
			openOrderIDs[order.OrderID] = struct{}{}
		}
		if order.ClientOrderID != "" {
			openClientOIDs[order.ClientOrderID] = struct{}{}
		}
	}
	filteredOrders, duplicateOrderIDs := spm.filterDuplicateSnapshotOrders(orders)
	cancelSnapshotOrderIDs = append(cancelSnapshotOrderIDs, duplicateOrderIDs...)

	for _, order := range filteredOrders {
		price, side, bookSide, valid := spm.parseClientOrderID(order.ClientOrderID)
		if !valid {
			continue
		}
		slot := spm.getOrCreateSlot(price, bookSide)
		slot.mu.Lock()
		if spm.snapshotOrderConflictsWithCurrentSlot(slot, order) {
			logger.Warn("⚠️ [对账] 快照挂单与当前槽位订单冲突，保留当前跟踪订单并准备撤销冲突单: slot=%s, currentOrderID=%d, currentClientOID=%s, snapshotOrderID=%d, snapshotClientOID=%s",
				formatPrice(price, spm.priceDecimals), slot.OrderID, slot.ClientOID, order.OrderID, order.ClientOrderID)
			if order.OrderID != 0 {
				cancelSnapshotOrderIDs = append(cancelSnapshotOrderIDs, order.OrderID)
			}
			slot.mu.Unlock()
			continue
		}
		slot.BookSide = bookSide
		slot.OrderID = order.OrderID
		slot.ClientOID = order.ClientOrderID
		slot.OrderSide = side
		remoteStatus := normalizeRemoteOrderStatus(order.Status)
		slot.OrderStatus = remoteStatus
		slot.OrderPrice = order.Price
		if slot.OrderCreatedAt.IsZero() {
			slot.OrderCreatedAt = time.Now()
		}
		if !spm.isEntryOrder(side, bookSide) && isActiveOrderStatus(remoteStatus) {
			spm.adoptExitSnapshotPosition(slot, order, bookSide, entryPriceByBook[bookSide])
		}
		spm.applySnapshotFillDelta(slot, side, bookSide, order.ExecutedQty, firstPositiveFloat(order.AvgPrice, order.Price, spm.exitPrice(price, bookSide)))
		if isActiveOrderStatus(remoteStatus) {
			slot.SlotStatus = SlotStatusLocked
		}
		slot.mu.Unlock()
	}
	if len(cancelSnapshotOrderIDs) > 0 && spm.executor != nil {
		if err := spm.executor.BatchCancelOrders(cancelSnapshotOrderIDs); err != nil {
			logger.Error("❌ [对账] 撤销快照冲突订单失败: %v (orderIDs=%v)", err, cancelSnapshotOrderIDs)
		} else {
			logger.Warn("🧹 [对账] 已撤销 %d 张快照冲突/重复订单: %v", len(cancelSnapshotOrderIDs), cancelSnapshotOrderIDs)
		}
	}

	staleActive := 0
	var staleOrders []staleOrderRef
	now := time.Now()
	spm.slots.Range(func(key, value interface{}) bool {
		slot := value.(*InventorySlot)
		slot.mu.Lock()
		defer slot.mu.Unlock()
		if !spm.slotHasActiveOrder(slot) || (slot.OrderID == 0 && slot.ClientOID == "") {
			return true
		}
		_, hasID := openOrderIDs[slot.OrderID]
		_, hasClientOID := openClientOIDs[slot.ClientOID]
		if !hasID && !hasClientOID {
			if slot.OrderID == 0 && slot.ClientOID != "" && !slot.OrderCreatedAt.IsZero() && now.Sub(slot.OrderCreatedAt) <= pendingOrderIdentityGrace {
				logger.Debug("⏳ [对账] 新建订单仍在等待远端 OrderID，同步保护期内暂不清理: slot=%s, clientOID=%s, status=%s",
					formatPrice(slot.Price, spm.priceDecimals), slot.ClientOID, slot.OrderStatus)
				return true
			}
			staleActive++
			staleOrders = append(staleOrders, staleOrderRef{OrderID: slot.OrderID, ClientOrderID: slot.ClientOID, CreatedAt: slot.OrderCreatedAt})
			logger.Warn("⚠️ [对账] 本地订单不在交易所挂单中，等待订单详情/WS确认: slot=%s, orderID=%d, clientOID=%s, status=%s",
				formatPrice(slot.Price, spm.priceDecimals), slot.OrderID, slot.ClientOID, slot.OrderStatus)
		}
		return true
	})
	if staleActive > 0 {
		logger.Warn("⚠️ [对账] 发现 %d 个本地活跃订单不在交易所挂单列表中", staleActive)
		spm.refreshStaleOrders(staleOrders)
	}

	if positionsRaw != nil {
		spm.reconcilePositionSnapshot(positions)
	}
}

func (spm *SuperPositionManager) adoptExitSnapshotPosition(slot *InventorySlot, order exchangeOrderSnapshot, bookSide string, entryPrice float64) {
	if slot == nil || slot.PositionStatus == PositionStatusFilled || slot.PositionQty > 0 {
		return
	}
	qty := firstPositiveFloat(order.Quantity, order.ExecutedQty)
	if qty <= 0 {
		return
	}
	qty = roundQuantity(qty, spm.quantityDecimals)
	if qty <= 0 {
		return
	}
	slot.PositionStatus = PositionStatusFilled
	slot.PositionQty = qty
	slot.EntryPrice = firstPositiveFloat(entryPrice, slot.Price)
	slot.BookSide = bookSide
	logger.Warn("🧩 [对账接管-%s] 快照发现已有平仓单但本地槽位为空，按 ClientOID 恢复槽位 %s 持仓 %.8f",
		bookSide, formatPrice(slot.Price, spm.priceDecimals), qty)
}

type snapshotOrderSelection struct {
	Index int
	Order exchangeOrderSnapshot
	Score int
}

func (spm *SuperPositionManager) filterDuplicateSnapshotOrders(orders []exchangeOrderSnapshot) ([]exchangeOrderSnapshot, []int64) {
	if len(orders) == 0 {
		return nil, nil
	}
	filtered := make([]exchangeOrderSnapshot, 0, len(orders))
	seen := make(map[string]snapshotOrderSelection, len(orders))
	var duplicateOrderIDs []int64

	for _, order := range orders {
		price, side, bookSide, valid := spm.parseClientOrderID(order.ClientOrderID)
		if !valid || !isActiveOrderStatus(normalizeRemoteOrderStatus(order.Status)) || order.Price <= 0 {
			filtered = append(filtered, order)
			continue
		}
		if !spm.isEntryOrder(side, bookSide) {
			filtered = append(filtered, order)
			continue
		}
		key := spm.orderPlacementKey(side, bookSide, order.Price)
		score := spm.snapshotOrderKeepScore(price, bookSide, order)
		if kept, exists := seen[key]; exists {
			if score > kept.Score {
				if kept.Order.OrderID != 0 {
					duplicateOrderIDs = append(duplicateOrderIDs, kept.Order.OrderID)
				}
				filtered[kept.Index] = order
				seen[key] = snapshotOrderSelection{Index: kept.Index, Order: order, Score: score}
			} else if order.OrderID != 0 {
				duplicateOrderIDs = append(duplicateOrderIDs, order.OrderID)
			}
			logger.Warn("⚠️ [对账] 发现同价重复挂单，保留一张并撤销多余订单: key=%s", key)
			continue
		}
		seen[key] = snapshotOrderSelection{Index: len(filtered), Order: order, Score: score}
		filtered = append(filtered, order)
	}
	return filtered, duplicateOrderIDs
}

func (spm *SuperPositionManager) snapshotOrderKeepScore(slotPrice float64, bookSide string, order exchangeOrderSnapshot) int {
	score := 0
	if order.ExecutedQty > 0 {
		score++
	}
	slot := spm.getOrCreateSlot(slotPrice, bookSide)
	slot.mu.RLock()
	if spm.slotHasActiveOrder(slot) {
		clientOID := strings.TrimSpace(order.ClientOrderID)
		if slot.ClientOID != "" && clientOID != "" && slot.ClientOID == clientOID {
			score += 4
		}
		if slot.OrderID != 0 && order.OrderID != 0 && slot.OrderID == order.OrderID {
			score += 4
		}
	}
	slot.mu.RUnlock()
	return score
}

func (spm *SuperPositionManager) snapshotOrderConflictsWithCurrentSlot(slot *InventorySlot, order exchangeOrderSnapshot) bool {
	if slot == nil || !spm.slotHasActiveOrder(slot) {
		return false
	}
	clientOID := strings.TrimSpace(order.ClientOrderID)
	if slot.ClientOID != "" && clientOID != "" {
		return slot.ClientOID != clientOID
	}
	if slot.OrderID != 0 && order.OrderID != 0 {
		return slot.OrderID != order.OrderID
	}
	return false
}

func (spm *SuperPositionManager) applySnapshotFillDelta(slot *InventorySlot, side, bookSide string, executedQty, tradePrice float64) {
	deltaQty := executedQty - slot.OrderFilledQty
	if deltaQty <= 0 {
		return
	}

	if spm.isEntryOrder(side, bookSide) {
		spm.applyEntryFill(slot, tradePrice, deltaQty)
	} else {
		spm.applyExitFill(slot, tradePrice, deltaQty, bookSide)
	}
	spm.addTradedQty(side, deltaQty)
	slot.OrderFilledQty = executedQty
}

func (spm *SuperPositionManager) refreshStaleOrders(ordersToRefresh []staleOrderRef) {
	now := time.Now()
	for _, ref := range ordersToRefresh {
		if ref.OrderID == 0 {
			if ref.ClientOrderID != "" {
				if !ref.CreatedAt.IsZero() && now.Sub(ref.CreatedAt) <= pendingOrderIdentityGrace {
					logger.Debug("⏳ [对账] ClientOID-only 订单仍在保护期内，暂不清理: clientOID=%s", ref.ClientOrderID)
					continue
				}
				logger.Warn("🧹 [对账] 本地订单缺少远端 OrderID 且不在交易所挂单中，清理本地槽位: clientOID=%s", ref.ClientOrderID)
				spm.OnOrderUpdate(OrderUpdate{
					ClientOrderID: ref.ClientOrderID,
					Status:        "CANCELED",
				})
			}
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		orderRaw, err := spm.exchange.GetOrder(ctx, spm.config.Trading.Symbol, ref.OrderID)
		cancel()
		if err != nil {
			if isRemoteOrderGoneError(err) && ref.ClientOrderID != "" {
				logger.Warn("🧹 [对账] 交易所确认订单不存在，清理本地槽位: orderID=%d, clientOID=%s", ref.OrderID, ref.ClientOrderID)
				spm.OnOrderUpdate(OrderUpdate{
					OrderID:       ref.OrderID,
					ClientOrderID: ref.ClientOrderID,
					Status:        "CANCELED",
				})
				continue
			}
			logger.Warn("⚠️ [对账] 查询疑似丢失订单失败: orderID=%d, err=%v", ref.OrderID, err)
			continue
		}
		orders := spm.extractExchangeOrders([]interface{}{orderRaw})
		if len(orders) == 0 {
			continue
		}
		order := orders[0]
		status := normalizeRemoteOrderStatus(order.Status)
		if status == OrderStatusConfirmed {
			status = "NEW"
		}
		spm.OnOrderUpdate(OrderUpdate{
			OrderID:       order.OrderID,
			ClientOrderID: order.ClientOrderID,
			Status:        status,
			ExecutedQty:   order.ExecutedQty,
			Price:         order.Price,
			AvgPrice:      order.AvgPrice,
			Side:          strings.ToUpper(order.Side),
			UpdateTime:    order.UpdateTime,
		})
	}
}

func isRemoteOrderGoneError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "unknown order") ||
		strings.Contains(errStr, "does not exist") ||
		strings.Contains(errStr, "not exist") ||
		strings.Contains(errStr, "not found") ||
		strings.Contains(errStr, "order not found") ||
		strings.Contains(errStr, "-2011") ||
		strings.Contains(errStr, "40004")
}

func (spm *SuperPositionManager) reconcilePositionSnapshot(positions []exchangePositionSnapshot) {
	remoteLong := 0.0
	remoteShort := 0.0
	longEntry := 0.0
	shortEntry := 0.0
	for _, pos := range positions {
		if pos.Symbol != "" && !strings.EqualFold(pos.Symbol, spm.config.Trading.Symbol) {
			continue
		}
		if pos.Size > 0 {
			remoteLong += pos.Size
			if pos.EntryPrice > 0 {
				longEntry = pos.EntryPrice
			}
		} else if pos.Size < 0 {
			remoteShort += math.Abs(pos.Size)
			if pos.EntryPrice > 0 {
				shortEntry = pos.EntryPrice
			}
		}
	}
	localLong, localShort := spm.localPositionTotals()
	tolerance := math.Pow(10, -float64(maxInt(spm.quantityDecimals, 0)))
	if tolerance <= 0 {
		tolerance = 0.000001
	}
	if math.Abs(localLong-remoteLong) > tolerance {
		spm.reconcileBookPositionSnapshot(BookSideLong, localLong, remoteLong, longEntry, tolerance)
	}
	if math.Abs(localShort-remoteShort) > tolerance {
		spm.reconcileBookPositionSnapshot(BookSideShort, localShort, remoteShort, shortEntry, tolerance)
	}
}

func (spm *SuperPositionManager) reconcileBookPositionSnapshot(bookSide string, localQty, remoteQty, entryPrice, tolerance float64) {
	if localQty <= tolerance {
		if remoteQty <= tolerance {
			return
		}
		logger.Warn("⚠️ [对账接管-%s] 检测到交易所已有持仓 %.8f，但本地无槽位；按默认策略接管为机器人库存槽位", bookSide, remoteQty)
		spm.adoptRemotePositionSnapshot(bookSide, remoteQty, entryPrice, tolerance)
		return
	}
	if remoteQty+tolerance < localQty {
		logger.Warn("⚠️ [对账修正-%s] 交易所持仓 %.8f 小于本地机器人持仓 %.8f，按较小值修正，避免平仓单超过真实仓位", bookSide, remoteQty, localQty)
		spm.adoptRemotePositionSnapshot(bookSide, remoteQty, entryPrice, tolerance)
		return
	}
	if remoteQty-localQty <= tolerance {
		return
	}
	deltaQty := remoteQty - localQty
	logger.Warn("⚠️ [对账补仓-%s] 交易所持仓 %.8f 大于本地槽位持仓 %.8f，按当前网格补齐差额 %.8f",
		bookSide, remoteQty, localQty, deltaQty)
	spm.adoptRemotePositionDelta(bookSide, deltaQty, entryPrice, tolerance)
}

func (spm *SuperPositionManager) localPositionTotals() (float64, float64) {
	longTotal := 0.0
	shortTotal := 0.0
	spm.slots.Range(func(key, value interface{}) bool {
		slot := value.(*InventorySlot)
		slot.mu.RLock()
		defer slot.mu.RUnlock()
		if slot.PositionStatus != PositionStatusFilled || slot.PositionQty <= 0 {
			return true
		}
		if slot.BookSide == BookSideShort {
			shortTotal += slot.PositionQty
		} else {
			longTotal += slot.PositionQty
		}
		return true
	})
	return longTotal, shortTotal
}

func (spm *SuperPositionManager) resetFilledSlots(bookSide string) {
	spm.slots.Range(func(key, value interface{}) bool {
		slot := value.(*InventorySlot)
		slot.mu.Lock()
		defer slot.mu.Unlock()
		if slot.BookSide != bookSide || slot.PositionStatus != PositionStatusFilled {
			return true
		}
		if spm.slotHasActiveOrder(slot) && slot.OrderID != 0 {
			return true
		}
		slot.PositionStatus = PositionStatusEmpty
		slot.PositionQty = 0
		slot.SlotStatus = SlotStatusFree
		slot.OrderFilledQty = 0
		return true
	})
}

func (spm *SuperPositionManager) adoptRemotePositionSnapshot(bookSide string, remoteQty, entryPrice, tolerance float64) {
	retainedQty := 0.0
	spm.slots.Range(func(key, value interface{}) bool {
		slot := value.(*InventorySlot)
		slot.mu.Lock()
		defer slot.mu.Unlock()
		if slot.BookSide != bookSide || slot.PositionStatus != PositionStatusFilled {
			return true
		}
		if spm.slotHasActiveOrder(slot) {
			retainedQty += slot.PositionQty
			return true
		}
		slot.PositionStatus = PositionStatusEmpty
		slot.PositionQty = 0
		slot.SlotStatus = SlotStatusFree
		slot.OrderFilledQty = 0
		return true
	})

	remaining := roundQuantity(remoteQty-retainedQty, spm.quantityDecimals)
	if remaining <= tolerance {
		return
	}
	spm.initializeSlotsFromPosition(remaining, bookSide, entryPrice)
}

func (spm *SuperPositionManager) adoptRemotePositionDelta(bookSide string, deltaQty, entryPrice, tolerance float64) {
	remaining := roundQuantity(deltaQty, spm.quantityDecimals)
	if remaining <= tolerance {
		return
	}
	referencePrice := spm.currentRecoveryReferencePrice(entryPrice)
	spm.initializeSlotsFromPositionAtReference(remaining, bookSide, entryPrice, referencePrice)
}

func (spm *SuperPositionManager) currentRecoveryReferencePrice(entryPrice float64) float64 {
	if lastPrice, ok := spm.lastMarketPrice.Load().(float64); ok && lastPrice > 0 {
		return lastPrice
	}
	return firstPositiveFloat(spm.anchorPrice, entryPrice)
}

// GetTotalBuyQty 获取累计买入数量（IPositionManager 接口方法，供 Reconciler 使用）
func (spm *SuperPositionManager) GetTotalBuyQty() float64 {
	return spm.totalBuyQty.Load().(float64)
}

// GetTotalSellQty 获取累计卖出数量（IPositionManager 接口方法，供 Reconciler 使用）
func (spm *SuperPositionManager) GetTotalSellQty() float64 {
	return spm.totalSellQty.Load().(float64)
}

func (spm *SuperPositionManager) GetRealizedPNL() float64 {
	return spm.totalRealizedPNL.Load().(float64)
}

func (spm *SuperPositionManager) EstimateUnrealizedPNL(markPrice float64) float64 {
	if markPrice <= 0 {
		return 0
	}
	total := 0.0
	spm.slots.Range(func(key, value interface{}) bool {
		slot := value.(*InventorySlot)
		slot.mu.RLock()
		defer slot.mu.RUnlock()
		if slot.PositionStatus != PositionStatusFilled || slot.PositionQty <= 0 {
			return true
		}
		entryPrice := spm.slotEntryPrice(slot)
		if slot.BookSide == BookSideShort {
			total += (entryPrice - markPrice) * slot.PositionQty
			return true
		}
		total += (markPrice - entryPrice) * slot.PositionQty
		return true
	})
	return total
}

func (spm *SuperPositionManager) GridRiskSnapshot(markPrice float64) safety.GridRiskSnapshot {
	spm.mu.RLock()
	defer spm.mu.RUnlock()
	return spm.gridRiskSnapshotLocked(markPrice)
}

func (spm *SuperPositionManager) gridRiskSnapshotLocked(markPrice float64) safety.GridRiskSnapshot {
	if markPrice <= 0 {
		if lastPrice, ok := spm.lastMarketPrice.Load().(float64); ok && lastPrice > 0 {
			markPrice = lastPrice
		}
	}
	var snapshot safety.GridRiskSnapshot
	spm.slots.Range(func(key, value interface{}) bool {
		slot := value.(*InventorySlot)
		slot.mu.RLock()
		defer slot.mu.RUnlock()
		if slot.PositionStatus != PositionStatusFilled || slot.PositionQty <= 0 {
			return true
		}
		snapshot.FilledLayers++
		entryPrice := spm.slotEntryPrice(slot)
		notionalPrice := firstPositiveFloat(markPrice, entryPrice)
		snapshot.TotalNotional += math.Abs(slot.PositionQty * notionalPrice)
		if markPrice > 0 {
			if slot.BookSide == BookSideShort {
				snapshot.UnrealizedPNL += (entryPrice - markPrice) * slot.PositionQty
			} else {
				snapshot.UnrealizedPNL += (markPrice - entryPrice) * slot.PositionQty
			}
		}
		return true
	})
	return snapshot
}

func (spm *SuperPositionManager) toGridRiskEntryOrders(orders []*OrderRequest) []safety.GridRiskOrder {
	riskOrders := make([]safety.GridRiskOrder, 0, len(orders))
	for _, order := range orders {
		if order == nil || order.ReduceOnly {
			continue
		}
		riskOrders = append(riskOrders, safety.GridRiskOrder{
			Symbol:     order.Symbol,
			Side:       order.Side,
			Price:      order.Price,
			Quantity:   order.Quantity,
			ReduceOnly: order.ReduceOnly,
		})
	}
	return riskOrders
}

func (spm *SuperPositionManager) filterReduceOnlyOrdersAndReleaseEntries(orders []*OrderRequest) []*OrderRequest {
	filtered := make([]*OrderRequest, 0, len(orders))
	for _, req := range orders {
		if req == nil {
			continue
		}
		if req.ReduceOnly {
			filtered = append(filtered, req)
			continue
		}
		price, _, bookSide, valid := spm.parseClientOrderID(req.ClientOrderID)
		if !valid {
			continue
		}
		slot := spm.getOrCreateSlot(price, bookSide)
		slot.mu.Lock()
		if slot.ClientOID == req.ClientOrderID && slot.SlotStatus == SlotStatusPending {
			slot.SlotStatus = SlotStatusFree
			clearSlotOrderTracking(slot, OrderStatusNotPlaced)
		}
		slot.mu.Unlock()
	}
	return filtered
}

// GetReconcileCount 获取对账次数（IPositionManager 接口方法，供 Reconciler 使用）
func (spm *SuperPositionManager) GetReconcileCount() int64 {
	return spm.reconcileCount.Load()
}

// IncrementReconcileCount 增加对账次数（IPositionManager 接口方法，供 Reconciler 使用）
func (spm *SuperPositionManager) IncrementReconcileCount() {
	spm.reconcileCount.Add(1)
}

// UpdateLastReconcileTime 更新最后对账时间（IPositionManager 接口方法，供 Reconciler 使用）
func (spm *SuperPositionManager) UpdateLastReconcileTime(t time.Time) {
	spm.lastReconcileTime.Store(t)
}

// GetSymbol 获取交易符号
func (spm *SuperPositionManager) GetSymbol() string {
	return spm.config.Trading.Symbol
}

// GetPriceInterval 获取价格间隔
func (spm *SuperPositionManager) GetPriceInterval() float64 {
	return spm.config.Trading.PriceInterval
}

// ===== 订单清理功能已迁移到 safety.OrderCleaner =====
// StartOrderCleanup 和 cleanupOrders 方法已移至 safety/order_cleaner.go

// UpdateSlotOrderStatus 更新槽位订单状态（供 OrderCleaner 使用）
func (spm *SuperPositionManager) UpdateSlotOrderStatus(price float64, bookSide, status string) {
	slot := spm.getOrCreateSlot(price, bookSide)
	slot.mu.Lock()
	slot.OrderStatus = status
	if status == OrderStatusCancelRequested {
		slot.SlotStatus = SlotStatusLocked
	} else if status == OrderStatusCanceled || status == OrderStatusNotPlaced {
		slot.SlotStatus = SlotStatusFree
		clearSlotOrderTracking(slot, status)
	}
	slot.mu.Unlock()
}

func (spm *SuperPositionManager) UpdateSlotOrderStatusIfCurrent(price float64, bookSide, status string, orderID int64, clientOID string) {
	slot := spm.getOrCreateSlot(price, bookSide)
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.OrderID != 0 && orderID != 0 && slot.OrderID != orderID {
		return
	}
	if slot.ClientOID != "" && clientOID != "" && slot.ClientOID != clientOID {
		return
	}
	slot.OrderStatus = status
	if status == OrderStatusCancelRequested {
		slot.SlotStatus = SlotStatusLocked
		return
	}
	if status == OrderStatusCanceled || status == OrderStatusNotPlaced {
		slot.SlotStatus = SlotStatusFree
		clearSlotOrderTracking(slot, status)
	}
}

// CancelEntryOrders 撤销所有开仓单（风控触发时使用）
func (spm *SuperPositionManager) CancelEntryOrders() {
	var orderIDs []int64
	var targets []struct {
		price     float64
		bookSide  string
		orderID   int64
		clientOID string
	}

	spm.slots.Range(func(key, value interface{}) bool {
		slot := value.(*InventorySlot)
		price := slot.Price

		slot.mu.RLock()
		if slot.OrderID > 0 && spm.isEntryOrder(slot.OrderSide, slot.BookSide) {
			orderIDs = append(orderIDs, slot.OrderID)
			targets = append(targets, struct {
				price     float64
				bookSide  string
				orderID   int64
				clientOID string
			}{price: price, bookSide: slot.BookSide, orderID: slot.OrderID, clientOID: slot.ClientOID})
		}
		slot.mu.RUnlock()
		return true
	})

	if len(orderIDs) == 0 {
		return
	}

	logger.Info("🔄 [撤销开仓单] 准备撤销 %d 个开仓单以释放保证金", len(orderIDs))

	// 🔥 重复尝试3次，确保撤单干净
	for attempt := 1; attempt <= 3; attempt++ {
		if len(orderIDs) == 0 {
			break
		}

		logger.Info("🔄 [撤销开仓单] 第 %d 次尝试，剩余 %d 个订单", attempt, len(orderIDs))

		if err := spm.executor.BatchCancelOrders(orderIDs); err != nil {
			logger.Error("❌ [撤销开仓单] 批量撤单失败: %v", err)
			if attempt == 3 {
				break
			}
			time.Sleep(2 * time.Second)
			continue
		}

		// 更新槽位状态
		for _, target := range targets {
			spm.UpdateSlotOrderStatusIfCurrent(target.price, target.bookSide, OrderStatusCancelRequested, target.orderID, target.clientOID)
		}

		// 等待2秒让撤单生效（WebSocket推送通知）
		time.Sleep(2 * time.Second)

		// 🔥 二次检查：重新扫描本地槽位状态
		if attempt < 3 {
			orderIDs = nil
			targets = nil

			spm.slots.Range(func(key, value interface{}) bool {
				slot := value.(*InventorySlot)
				price := slot.Price

				slot.mu.RLock()
				if slot.OrderID > 0 && spm.isEntryOrder(slot.OrderSide, slot.BookSide) &&
					slot.OrderStatus != OrderStatusCanceled {
					orderIDs = append(orderIDs, slot.OrderID)
					targets = append(targets, struct {
						price     float64
						bookSide  string
						orderID   int64
						clientOID string
					}{price: price, bookSide: slot.BookSide, orderID: slot.OrderID, clientOID: slot.ClientOID})
				}
				slot.mu.RUnlock()
				return true
			})

			if len(orderIDs) > 0 {
				logger.Warn("⚠️ [撤销开仓单] 检测到 %d 个残留订单，继续清理", len(orderIDs))
			} else {
				logger.Info("✅ [撤销开仓单] 所有开仓单已清理完成")
				break
			}
		}
	}

	logger.Info("✅ [撤销开仓单] 清理完成")
}

// ===== 对账功能已迁移到 safety.Reconciler =====
// StartReconciliation 和 Reconcile 方法已移至 safety/reconciler.go
// SetPauseChecker 也已移至 Reconciler

// CancelAllOrders 撤销所有订单（退出时使用）
// 委托给交易所适配器实现具体逻辑
func (spm *SuperPositionManager) CancelAllOrders() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := spm.exchange.CancelAllOrders(ctx, spm.config.Trading.Symbol); err != nil {
		logger.Error("❌ [%s] 撤销所有订单失败: %v", spm.exchange.GetName(), err)
	} else {
		logger.Info("✅ [%s] 撤销所有订单完成", spm.exchange.GetName())
	}
}

type existingPositions struct {
	LongQty    float64
	ShortQty   float64
	LongEntry  float64
	ShortEntry float64
}

func (p *existingPositions) add(size, entryPrice, fallbackPrice float64) {
	if size == 0 {
		return
	}
	if entryPrice <= 0 {
		entryPrice = fallbackPrice
	}
	if entryPrice <= 0 {
		entryPrice = 0
	}
	qty := math.Abs(size)
	if size > 0 {
		p.LongEntry = weightedAveragePrice(p.LongEntry, p.LongQty, entryPrice, qty)
		p.LongQty += qty
		return
	}
	p.ShortEntry = weightedAveragePrice(p.ShortEntry, p.ShortQty, entryPrice, qty)
	p.ShortQty += qty
}

func weightedAveragePrice(currentPrice, currentQty, nextPrice, nextQty float64) float64 {
	if nextPrice <= 0 {
		return currentPrice
	}
	if currentPrice <= 0 || currentQty <= 0 {
		return nextPrice
	}
	totalQty := currentQty + nextQty
	if totalQty <= 0 {
		return currentPrice
	}
	return (currentPrice*currentQty + nextPrice*nextQty) / totalQty
}

// getExistingPositions 获取当前多空持仓与交易所参考价（容错处理）
func (spm *SuperPositionManager) getExistingPositions() existingPositions {
	result := existingPositions{}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	positionsInterface, err := spm.exchange.GetPositions(ctx, spm.config.Trading.Symbol)
	if err != nil || positionsInterface == nil {
		logger.Debug("🔍 [持仓恢复] 无法获取持仓信息: %v", err)
		return result
	}

	positions := spm.extractExchangePositions(positionsInterface)
	for _, pos := range positions {
		if pos.Size == 0 {
			continue
		}
		if pos.Symbol != "" && !sameTradingSymbol(pos.Symbol, spm.config.Trading.Symbol) {
			continue
		}
		entryPrice := pos.EntryPrice
		if entryPrice <= 0 {
			entryPrice = pos.MarkPrice
		}
		if entryPrice <= 0 {
			entryPrice = spm.anchorPrice
		}
		logger.Debug("🔍 [持仓恢复] 找到持仓: symbol=%s, size=%.8f, entry=%.8f, mark=%.8f",
			pos.Symbol, pos.Size, pos.EntryPrice, pos.MarkPrice)
		result.add(pos.Size, entryPrice, spm.anchorPrice)
	}

	if result.LongQty == 0 && result.ShortQty == 0 {
		logger.Debug("🔍 [持仓恢复] 未找到匹配的持仓")
	}
	return result
}

// initializeSlotsFromPosition 从现有持仓初始化平仓槽位（用于程序重启后恢复状态）
func (spm *SuperPositionManager) initializeSlotsFromPosition(totalPosition float64, bookSide string, entryPrice float64) {
	spm.initializeSlotsFromPositionAtReference(totalPosition, bookSide, entryPrice, firstPositiveFloat(spm.anchorPrice, entryPrice))
}

func (spm *SuperPositionManager) initializeSlotsFromPositionAtReference(totalPosition float64, bookSide string, entryPrice, reference float64) {
	if totalPosition <= 0 {
		return
	}
	referencePrice := spm.findNearestGridPrice(reference)
	if referencePrice <= 0 {
		return
	}
	direction := "down"
	if bookSide == BookSideLong {
		direction = "up"
	}

	remaining := roundQuantity(totalPosition, spm.quantityDecimals)
	allocatedQty := 0.0
	created := 0
	maxSlots := 100000
	for i := 0; remaining > 0 && i < maxSlots; i++ {
		price := referencePrice
		if direction == "down" {
			price = referencePrice - float64(i)*spm.config.Trading.PriceInterval
		} else {
			price = referencePrice + float64(i)*spm.config.Trading.PriceInterval
		}
		price = roundPrice(price, spm.priceDecimals)
		if price <= 0 {
			break
		}
		slotQty := spm.maxConfiguredOrderQuantity(price)
		if slotQty <= 0 {
			logger.Warn("⚠️ [持仓恢复-%s] 价格 %s 的单格数量为 0，停止拆分底仓以避免放大订单",
				bookSide, formatPrice(price, spm.priceDecimals))
			break
		}
		if slotQty > remaining {
			slotQty = remaining
		}
		slotQty = roundQuantity(slotQty, spm.quantityDecimals)
		nextRemaining := roundQuantity(remaining-slotQty, spm.quantityDecimals)
		if nextRemaining > 0 && nextRemaining*price < spm.minOrderValue() {
			slotQty = roundQuantity(remaining, spm.quantityDecimals)
			nextRemaining = 0
		}

		slot := spm.getOrCreateSlot(price, bookSide)
		slot.mu.Lock()
		if spm.slotHasActiveOrder(slot) {
			slot.mu.Unlock()
			continue
		}
		slot.PositionStatus = PositionStatusFilled
		slot.PositionQty = slotQty
		slot.EntryPrice = firstPositiveFloat(entryPrice, price)
		slot.BookSide = bookSide
		clearSlotOrderTracking(slot, OrderStatusNotPlaced)
		slot.SlotStatus = SlotStatusFree
		slot.mu.Unlock()

		allocatedQty += slotQty
		remaining = nextRemaining
		created++
		if i < 10 {
			logger.Info("✅ [持仓恢复] 槽位 %s: 分配持仓 %.4f", formatPrice(price, spm.priceDecimals), slotQty)
		} else if i == 10 {
			logger.Info("... （省略后续底仓拆分槽位日志）")
		}
	}

	if remaining > 0 {
		logger.Warn("⚠️ [持仓恢复-%s] 仍有 %.8f 仓位未能按单笔金额拆分，将不会自动放大成大单",
			bookSide, remaining)
	}
	logger.Info("✅ [持仓恢复-%s] 按单笔金额拆分底仓: 参考价 %s, 创建 %d 个槽位, 总持仓 %.4f, 已分配 %.4f",
		bookSide, formatPrice(referencePrice, spm.priceDecimals), created, totalPosition, allocatedQty)
}

// ===== 状态打印功能 =====

// PrintPositions 打印持仓状态（由 main.go 定期调用和退出时调用）
// 注意：该方法内部使用 totalBuyQty 和 totalSellQty 统计数据
func (spm *SuperPositionManager) PrintPositions() {
	spm.PrintPositionsWithMarkPrice(0)
}

// PrintPositionsWithMarkPrice 使用外部实时价格打印状态，避免状态日志继续使用上次调仓价。
func (spm *SuperPositionManager) PrintPositionsWithMarkPrice(markPrice float64) {
	logger.Debug("📊 ===== 当前持仓 =====")
	debugEnabled := logger.GetLevel() <= logger.DEBUG
	total := 0.0
	count := 0

	// 收集所有持仓数据
	type positionInfo struct {
		Price       float64
		Qty         float64
		BookSide    string
		OrderStatus string
		OrderSide   string
		OrderID     int64
		SlotStatus  string
	}
	var positions []positionInfo

	spm.slots.Range(func(key, value interface{}) bool {
		slot := value.(*InventorySlot)
		price := slot.Price
		slot.mu.RLock()
		if slot.PositionStatus == PositionStatusFilled && slot.PositionQty > 0.001 {
			if debugEnabled {
				positions = append(positions, positionInfo{
					Price:       price,
					Qty:         slot.PositionQty,
					BookSide:    slot.BookSide,
					OrderStatus: slot.OrderStatus,
					OrderSide:   slot.OrderSide,
					OrderID:     slot.OrderID,
					SlotStatus:  slot.SlotStatus,
				})
			}
			total += slot.PositionQty
			count++
		}
		slot.mu.RUnlock()
		return true
	})

	// 从交易所接口获取基础币种（支持U本位和币本位合约）
	baseCurrency := spm.exchange.GetBaseAsset()
	lastPrice, ok := spm.lastMarketPrice.Load().(float64)
	if !ok || lastPrice <= 0 {
		lastPrice = spm.anchorPrice
	}
	if markPrice > 0 {
		lastPrice = roundPrice(markPrice, spm.priceDecimals)
		spm.lastMarketPrice.Store(lastPrice)
	}

	if debugEnabled {
		sort.Slice(positions, func(i, j int) bool {
			return positions[i].Price > positions[j].Price
		})
		for _, pos := range positions {
			statusIcon := "🟢" // 有持仓
			priceStr := formatPrice(pos.Price, spm.priceDecimals)
			positionDesc := fmt.Sprintf("%s持仓: %.4f %s", pos.BookSide, pos.Qty, baseCurrency)

			orderInfo := ""
			if spm.slotHasActiveOrder(&InventorySlot{OrderID: pos.OrderID, OrderSide: pos.OrderSide, OrderStatus: pos.OrderStatus, SlotStatus: pos.SlotStatus}) {
				orderInfo = fmt.Sprintf(", 订单: %s/%s (ID:%d)", pos.OrderSide, pos.OrderStatus, pos.OrderID)
			}

			slotStatusInfo := ""
			if pos.SlotStatus != "" {
				slotStatusInfo = fmt.Sprintf(" [槽位:%s]", pos.SlotStatus)
			} else {
				slotStatusInfo = " [槽位:空]"
			}

			logger.Debug("  %s %s: %s%s%s",
				statusIcon, priceStr, positionDesc, orderInfo, slotStatusInfo)
		}
	}

	totalBuyQty := spm.totalBuyQty.Load().(float64)
	totalSellQty := spm.totalSellQty.Load().(float64)
	realizedPNL := spm.totalRealizedPNL.Load().(float64)
	unrealizedPNL := spm.EstimateUnrealizedPNL(lastPrice)
	logger.Info("📊 [统计] 持仓: %.4f %s (%d 个槽位), 当前价格: %s, 累计买入: %.4f, 累计卖出: %.4f, 已实现盈亏: %.4f USD, 未实现盈亏: %.4f USD",
		total, baseCurrency, count, formatPrice(lastPrice, spm.priceDecimals), totalBuyQty, totalSellQty, realizedPNL, unrealizedPNL)
	// === 打印当前窗口槽位，便于 CLI 观察机器人是否按预期补单 ===
	logger.Info("🔍 ===== 当前窗口槽位 =====")
	logger.Info("当前市场价格: %s", formatPrice(lastPrice, spm.priceDecimals))

	// 收集所有槽位信息（包括买单和空槽位）
	type slotInfo struct {
		Price          float64
		PositionStatus string
		PositionQty    float64
		BookSide       string
		OrderSide      string
		OrderStatus    string
		OrderID        int64
		ClientOID      string
		OrderPrice     float64
		PostOnlyFails  int
		SlotStatus     string
	}
	var allSlots []slotInfo
	slotByKey := make(map[string]slotInfo)

	spm.slots.Range(func(key, value interface{}) bool {
		slot := value.(*InventorySlot)
		price := slot.Price
		slot.mu.RLock()
		info := slotInfo{
			Price:          price,
			PositionStatus: slot.PositionStatus,
			PositionQty:    slot.PositionQty,
			BookSide:       slot.BookSide,
			OrderSide:      slot.OrderSide,
			OrderStatus:    slot.OrderStatus,
			OrderID:        slot.OrderID,
			ClientOID:      slot.ClientOID,
			OrderPrice:     slot.OrderPrice,
			PostOnlyFails:  slot.PostOnlyFailCount,
			SlotStatus:     slot.SlotStatus,
		}
		allSlots = append(allSlots, info)
		slotByKey[spm.slotKey(price, slot.BookSide)] = info
		slot.mu.RUnlock()
		return true
	})

	// 按价格从高到低排序
	sort.Slice(allSlots, func(i, j int) bool {
		return allSlots[i].Price > allSlots[j].Price
	})

	currentGridPrice := spm.findNearestGridPrice(lastPrice)
	logger.Info("当前网格价格: %s", formatPrice(currentGridPrice, spm.priceDecimals))

	// 计算开仓窗口范围；多头在当前价下方，空头在当前价上方。
	buyWindowSize := spm.config.Trading.BuyWindowSize
	sellWindowSize := spm.config.Trading.SellWindowSize
	exitWindowPriceMap := make(map[string]bool)
	enabledBookSides := spm.enabledBookSides()
	entryWindowSize := 0
	var windowSlots []slotInfo
	windowSeen := make(map[string]bool)
	for _, bookSide := range enabledBookSides {
		bookEntryWindowSize := spm.entryWindowSize(bookSide)
		if bookEntryWindowSize > entryWindowSize {
			entryWindowSize = bookEntryWindowSize
		}
		for _, p := range spm.calculateEntrySlotPrices(currentGridPrice, lastPrice, bookEntryWindowSize, bookSide) {
			key := spm.slotKey(p, bookSide)
			if windowSeen[key] {
				continue
			}
			windowSeen[key] = true
			if info, ok := slotByKey[key]; ok {
				windowSlots = append(windowSlots, info)
			} else {
				windowSlots = append(windowSlots, slotInfo{
					Price:          p,
					PositionStatus: PositionStatusEmpty,
					BookSide:       bookSide,
					SlotStatus:     SlotStatusFree,
				})
			}
		}
		bookExitWindowSize := sellWindowSize
		if spm.exitOrderSide(bookSide) == "BUY" {
			bookExitWindowSize = buyWindowSize
		}
		for _, p := range spm.calculateExitSlotPrices(currentGridPrice, lastPrice, bookExitWindowSize, bookSide) {
			exitWindowPriceMap[formatPrice(p, spm.priceDecimals)] = true
		}
	}
	sort.Slice(windowSlots, func(i, j int) bool {
		return windowSlots[i].Price > windowSlots[j].Price
	})

	// 打印开仓窗口内的所有槽位
	logger.Info("开仓窗口大小: %d 个槽位", entryWindowSize)
	entryOrderCount := 0
	emptySlotCount := 0
	filledSlotCount := 0

	for _, slot := range windowSlots {
		priceStr := formatPrice(slot.Price, spm.priceDecimals)
		statusIcon := "⚪" // 空槽位
		statusDesc := ""

		if slot.PositionStatus == PositionStatusFilled {
			statusIcon = "🟢" // 有持仓
			exitTarget := spm.exitPrice(slot.Price, slot.BookSide)
			statusDesc = fmt.Sprintf("%s持仓: %.4f %s, 平仓目标:%s", slot.BookSide, slot.PositionQty, baseCurrency, formatPrice(exitTarget, spm.priceDecimals))
			if !spm.slotHasActiveOrder(&InventorySlot{OrderID: slot.OrderID, ClientOID: slot.ClientOID, OrderStatus: slot.OrderStatus, SlotStatus: slot.SlotStatus}) {
				if spm.isMakerSafeExitPrice(exitTarget, lastPrice, slot.BookSide) {
					statusDesc += " [待挂平仓]"
				} else {
					statusDesc += " [等待PostOnly安全]"
				}
			}
			filledSlotCount++
		} else {
			statusDesc = "无持仓"
		}

		orderInfo := ""
		if spm.slotHasActiveOrder(&InventorySlot{OrderID: slot.OrderID, ClientOID: slot.ClientOID, OrderStatus: slot.OrderStatus, SlotStatus: slot.SlotStatus}) {
			orderPrice := slot.OrderPrice
			if orderPrice <= 0 {
				orderPrice = slot.Price
			}
			orderInfo = fmt.Sprintf(", 订单: %s/%s @%s (ID:%d)", slot.OrderSide, slot.OrderStatus, formatPrice(orderPrice, spm.priceDecimals), slot.OrderID)
			if slot.PostOnlyFails > 0 {
				orderInfo += fmt.Sprintf(", PostOnly失败:%d", slot.PostOnlyFails)
			}
			if spm.isEntryOrder(slot.OrderSide, slot.BookSide) {
				entryOrderCount++
			}
		}
		if slot.PositionStatus != PositionStatusFilled && orderInfo == "" && slot.SlotStatus == SlotStatusFree {
			emptySlotCount++
		}

		// 🔥 总是显示槽位状态,便于调试
		slotStatusInfo := ""
		if slot.SlotStatus != "" {
			slotStatusInfo = fmt.Sprintf(" [槽位:%s]", slot.SlotStatus)
		} else {
			slotStatusInfo = " [槽位:空]"
		}

		logger.Info("  %s %s: %s%s%s",
			statusIcon, priceStr, statusDesc, orderInfo, slotStatusInfo)
	}

	logger.Info("窗口统计: %d 个开仓单活跃, %d 个已持仓, %d 个空槽位",
		entryOrderCount, filledSlotCount, emptySlotCount)
	if len(exitWindowPriceMap) > 0 {
		exitPrices := make([]string, 0, len(exitWindowPriceMap))
		for price := range exitWindowPriceMap {
			exitPrices = append(exitPrices, price)
		}
		sort.Slice(exitPrices, func(i, j int) bool { return exitPrices[i] > exitPrices[j] })
		logger.Info("平仓窗口价格: %v", exitPrices)
	}
	logger.Info("==========================")
}

// 辅助函数
// roundPrice 价格四舍五入
func roundPrice(price float64, decimals int) float64 {
	multiplier := math.Pow(10, float64(decimals))
	return math.Round(price*multiplier) / multiplier
}

func roundQuantity(quantity float64, decimals int) float64 {
	if decimals < 0 {
		decimals = 0
	}
	multiplier := math.Pow(10, float64(decimals))
	return math.Round(quantity*multiplier) / multiplier
}

func floorQuantity(quantity float64, decimals int) float64 {
	if decimals < 0 {
		decimals = 0
	}
	multiplier := math.Pow(10, float64(decimals))
	return math.Floor(quantity*multiplier) / multiplier
}

func firstPositiveFloat(values ...float64) float64 {
	for _, value := range values {
		if value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0) {
			return value
		}
	}
	return 0
}

func gridRealizedPNL(entryPrice, tradePrice, qty float64, bookSide string, feeRate float64) float64 {
	if entryPrice <= 0 || tradePrice <= 0 || qty <= 0 {
		return 0
	}
	gross := (tradePrice - entryPrice) * qty
	if strings.ToUpper(bookSide) == BookSideShort {
		gross = (entryPrice - tradePrice) * qty
	}
	if feeRate < 0 {
		feeRate = 0
	}
	fees := (entryPrice + tradePrice) * qty * feeRate
	return gross - fees
}

func isActiveOrderStatus(status string) bool {
	switch status {
	case OrderStatusPlaced, OrderStatusConfirmed, OrderStatusPartiallyFilled, OrderStatusCancelRequested:
		return true
	default:
		return false
	}
}

func normalizeRemoteOrderStatus(status string) string {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "NEW", "OPEN", "LIVE":
		return OrderStatusConfirmed
	case "PARTIALLY_FILLED", "PARTIALLYFILLED":
		return OrderStatusPartiallyFilled
	case "PARTIALLY_FILLED_CANCELED", "PARTIAL_FILLED_CANCELED", "PARTIALLY_FILLED_CANCELLED", "PARTIAL_FILLED_CANCELLED":
		return OrderStatusCanceled
	case "FILLED":
		return OrderStatusFilled
	case "CANCELED", "CANCELLED", "EXPIRED", "REJECTED":
		return OrderStatusCanceled
	case OrderStatusPlaced, OrderStatusConfirmed, OrderStatusCancelRequested:
		return status
	default:
		return OrderStatusConfirmed
	}
}

func sameTradingSymbol(a, b string) bool {
	return normalizeComparableSymbol(a) == normalizeComparableSymbol(b)
}

func normalizeComparableSymbol(symbol string) string {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	replacer := strings.NewReplacer("/", "", "_", "", "-", "", " ", "")
	return replacer.Replace(symbol)
}

func (spm *SuperPositionManager) extractExchangeOrders(raw interface{}) []exchangeOrderSnapshot {
	values := reflectSlice(raw)
	orders := make([]exchangeOrderSnapshot, 0, len(values))
	for _, value := range values {
		order := exchangeOrderSnapshot{
			OrderID:       reflectInt64Field(value, "OrderID", "ID"),
			ClientOrderID: reflectStringField(value, "ClientOrderID", "ClientOID", "Text", "OrderLinkID", "ClOrdID"),
			Side:          reflectStringField(value, "Side", "OrderSide"),
			Status:        reflectStringField(value, "Status", "OrderStatus"),
			Price:         reflectFloatField(value, "Price", "OrderPrice"),
			Quantity:      reflectFloatField(value, "Quantity", "Qty", "Size"),
			ExecutedQty:   reflectFloatField(value, "ExecutedQty", "CumExecQty", "FillSize"),
			AvgPrice:      reflectFloatField(value, "AvgPrice", "FillPrice"),
			UpdateTime:    reflectInt64Field(value, "UpdateTime", "UpdatedTime"),
		}
		order.ClientOrderID = spm.normalizeClientOrderID(order.ClientOrderID)
		if order.ClientOrderID == "" && order.OrderID == 0 {
			continue
		}
		orders = append(orders, order)
	}
	return orders
}

func (spm *SuperPositionManager) extractExchangePositions(raw interface{}) []exchangePositionSnapshot {
	values := reflectSlice(raw)
	positions := make([]exchangePositionSnapshot, 0, len(values))
	for _, value := range values {
		pos := exchangePositionSnapshot{
			Symbol:     reflectStringField(value, "Symbol"),
			Size:       reflectFloatField(value, "Size", "PositionAmt"),
			EntryPrice: reflectFloatField(value, "EntryPrice", "AvgPrice"),
			MarkPrice:  reflectFloatField(value, "MarkPrice"),
		}
		if pos.Symbol == "" && pos.Size == 0 {
			continue
		}
		positions = append(positions, pos)
	}
	return positions
}

func reflectSlice(raw interface{}) []reflect.Value {
	if raw == nil {
		return nil
	}
	v := reflect.ValueOf(raw)
	for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return nil
	}
	values := make([]reflect.Value, 0, v.Len())
	for i := 0; i < v.Len(); i++ {
		item := v.Index(i)
		for item.Kind() == reflect.Pointer || item.Kind() == reflect.Interface {
			if item.IsNil() {
				goto next
			}
			item = item.Elem()
		}
		values = append(values, item)
	next:
	}
	return values
}

func reflectField(value reflect.Value, names ...string) reflect.Value {
	if value.Kind() != reflect.Struct {
		return reflect.Value{}
	}
	for _, name := range names {
		field := value.FieldByName(name)
		if field.IsValid() {
			return field
		}
	}
	return reflect.Value{}
}

func reflectStringField(value reflect.Value, names ...string) string {
	field := reflectField(value, names...)
	if !field.IsValid() {
		return ""
	}
	if field.Kind() == reflect.String {
		return field.String()
	}
	return fmt.Sprint(field.Interface())
}

func reflectFloatField(value reflect.Value, names ...string) float64 {
	field := reflectField(value, names...)
	if !field.IsValid() {
		return 0
	}
	switch field.Kind() {
	case reflect.Float32, reflect.Float64:
		return field.Float()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(field.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(field.Uint())
	case reflect.String:
		parsed, _ := strconv.ParseFloat(field.String(), 64)
		return parsed
	default:
		return 0
	}
}

func reflectInt64Field(value reflect.Value, names ...string) int64 {
	field := reflectField(value, names...)
	if !field.IsValid() {
		return 0
	}
	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return field.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(field.Uint())
	case reflect.Float32, reflect.Float64:
		return int64(field.Float())
	case reflect.String:
		parsed, _ := strconv.ParseInt(field.String(), 10, 64)
		return parsed
	default:
		return 0
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

// formatPrice 格式化价格字符串，使用指定的小数位数
func formatPrice(price float64, decimals int) string {
	return fmt.Sprintf("%.*f", decimals, price)
}
