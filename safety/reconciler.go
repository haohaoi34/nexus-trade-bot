package safety

import (
	"context"
	"fmt"
	"nexus-trade-bot/config"
	"nexus-trade-bot/logger"
	"nexus-trade-bot/realtime"
	"reflect"
	"strings"
	"time"
)

// IExchange 定义对账所需的交易所接口方法
type IExchange interface {
	GetPositions(ctx context.Context, symbol string) (interface{}, error)
	GetOpenOrders(ctx context.Context, symbol string) (interface{}, error)
	GetBaseAsset() string // 获取基础资产（交易币种）
}

// SlotInfo 槽位信息（避免直接依赖 position 包的内部结构）
type SlotInfo struct {
	Price          float64
	PositionStatus string
	PositionQty    float64
	BookSide       string
	OrderID        int64
	OrderSide      string
	OrderStatus    string
	OrderCreatedAt time.Time
}

// IPositionManager 定义对账所需的仓位管理器接口方法
@@ -65,81 +66,200 @@ type Reconciler struct {
	cfg               *config.Config
	exchange          IExchange
	pm                IPositionManager
	pauseChecker      func() bool
	markPriceProvider markPriceProvider
}

// NewReconciler 创建对账器
func NewReconciler(cfg *config.Config, exchange IExchange, pm IPositionManager) *Reconciler {
	return &Reconciler{
		cfg:      cfg,
		exchange: exchange,
		pm:       pm,
	}
}

// SetPauseChecker 设置暂停检查函数（用于风控暂停）
func (r *Reconciler) SetPauseChecker(checker func() bool) {
	r.pauseChecker = checker
}

func (r *Reconciler) SetMarkPriceProvider(provider func() float64) {
	r.markPriceProvider = provider
}
// Start 启动连续对账校验协程。
//
// 交易线程不在这里等待：循环在独立 goroutine 中运行，常态 30s 慢速校验，
// 一旦检测到漂移/不稳定状态则切换到 1-3s 快速校验。循环只检测并发布
// 漂移事件；风险等级和交易行为由 RiskEngine 决策。
func (r *Reconciler) Start(ctx context.Context) {
		interval := time.Duration(r.cfg.Trading.ReconcileInterval) * time.Second
	if interval <= 0 || interval > realtime.DefaultSlowInterval {
		interval = realtime.DefaultSlowInterval
	}
	loop := realtime.NewReconciliationLoop(&reconciliationStateAdapter{reconciler: r}, nil, realtime.LoopConfig{
		SlowInterval:    interval,
		FastInterval:    2 * time.Second,
		SmallThreshold:  0.00000001,
		MediumThreshold: 1,
		LargeThreshold:  3,
	})
	loop.Run(ctx)
	logger.Info("✅ 连续持仓对账已启动 (慢速: %s, 快速: %s)", interval, 2*time.Second)
}

type reconciliationStateAdapter struct {
	reconciler *Reconciler
}

func (a *reconciliationStateAdapter) LocalState(ctx context.Context) (realtime.StateSnapshot, error) {
	return a.reconciler.localStateSnapshot(), nil
}

func (a *reconciliationStateAdapter) ExchangeState(ctx context.Context) (realtime.StateSnapshot, error) {
	if a.reconciler.pauseChecker != nil && a.reconciler.pauseChecker() {
		return a.reconciler.localStateSnapshot(), nil
	}
	symbol := a.reconciler.pm.GetSymbol()
	apiCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	positionsRaw, err := a.reconciler.exchange.GetPositions(apiCtx, symbol)
	if err != nil {
		return realtime.StateSnapshot{}, fmt.Errorf("查询持仓失败: %w", err)
	}
	openOrdersRaw, err := a.reconciler.exchange.GetOpenOrders(apiCtx, symbol)
	if err != nil {
		return realtime.StateSnapshot{}, fmt.Errorf("查询挂单失败: %w", err)
	}
	return realtime.StateSnapshot{PositionQty: sumPositionQuantities(positionsRaw), OpenOrders: countItems(openOrdersRaw), UpdatedAt: time.Now()}, nil
}

func (r *Reconciler) localStateSnapshot() realtime.StateSnapshot {
	var localFilledPosition float64
	var activeOrders int
	const (
		OrderStatusPlaced          = "PLACED"
		OrderStatusConfirmed       = "CONFIRMED"
		OrderStatusPartiallyFilled = "PARTIALLY_FILLED"
		OrderStatusCancelRequested = "CANCEL_REQUESTED"
		PositionStatusFilled       = "FILLED"
	)
	r.pm.IterateSlots(func(price float64, slotRaw interface{}) bool {
		v := reflect.ValueOf(slotRaw)
		if v.Kind() != reflect.Struct {
			return true
		}
		getStringField := func(name string) string {
			field := v.FieldByName(name)
			if field.IsValid() && field.Kind() == reflect.String {
				return field.String()
			}
			return ""
		}
		getFloat64Field := func(name string) float64 {
			field := v.FieldByName(name)
			if field.IsValid() && field.CanFloat() {
				return field.Float()
			}
			return 0
		}
		getInt64Field := func(name string) int64 {
			field := v.FieldByName(name)
			if field.IsValid() && field.CanInt() {
				return field.Int()
			}
			return 0
		}
		if getStringField("PositionStatus") == PositionStatusFilled {
			localFilledPosition += getFloat64Field("PositionQty")
					}
				orderStatus := getStringField("OrderStatus")
		clientOID := getStringField("ClientOID")
		slotStatus := getStringField("SlotStatus")
		hasActiveOrder := (getInt64Field("OrderID") != 0 || clientOID != "") &&
			(orderStatus == OrderStatusPlaced || orderStatus == OrderStatusConfirmed ||
				orderStatus == OrderStatusPartiallyFilled || orderStatus == OrderStatusCancelRequested)
		if slotStatus == "PENDING" {
			hasActiveOrder = clientOID != ""
		}
		if hasActiveOrder {
			activeOrders++
		}
		return true
	})
	return realtime.StateSnapshot{PositionQty: localFilledPosition, OpenOrders: activeOrders, UpdatedAt: time.Now()}
}

func sumPositionQuantities(raw interface{}) float64 {
	v := reflect.ValueOf(raw)
	for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return 0
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return 0
	}
	total := 0.0
	for i := 0; i < v.Len(); i++ {
		item := v.Index(i)
		for item.Kind() == reflect.Pointer || item.Kind() == reflect.Interface {
			if item.IsNil() {
				item = reflect.Value{}
				break
			}
			item = item.Elem()
		}
		if !item.IsValid() || item.Kind() != reflect.Struct {
			continue
		}
		for _, fieldName := range []string{"Size", "PositionQty", "Quantity", "Qty"} {
			field := item.FieldByName(fieldName)
			if field.IsValid() && field.CanFloat() {
				total += field.Float()
				break
							}
		}
	}
	return total
}

func countItems(raw interface{}) int {
	v := reflect.ValueOf(raw)
	for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return 0
		}
		v = v.Elem()
	}
	if v.Kind() == reflect.Slice || v.Kind() == reflect.Array || v.Kind() == reflect.Map {
		return v.Len()
	}
	return 0
}

// Reconcile 执行对账（通用实现，支持所有交易所）
func (r *Reconciler) Reconcile() error {
	// 检查是否暂停（风控触发时不输出日志）
	if r.pauseChecker != nil && r.pauseChecker() {
		return nil
	}

	logger.Debugln("🔍 ===== 开始持仓对账 =====")

	symbol := r.pm.GetSymbol()
	apiCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	// 1. 查询交易所持仓信息（使用通用接口）
	positionsRaw, err := r.exchange.GetPositions(apiCtx, symbol)
	if err != nil {
		return fmt.Errorf("查询持仓失败: %w", err)
	}

	// 2. 查询所有挂单（使用通用接口）
	openOrdersRaw, err := r.exchange.GetOpenOrders(apiCtx, symbol)
	if err != nil {
		return fmt.Errorf("查询挂单失败: %w", err)
