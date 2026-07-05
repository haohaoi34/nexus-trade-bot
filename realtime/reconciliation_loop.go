package realtime

import (
	"context"
	"math"
	"time"
)

//
// ============================
// Drift Types
// ============================
//

type DriftSeverity int

const (
	DriftNone DriftSeverity = iota
	DriftSmall
	DriftMedium
	DriftLarge
)

type StateSnapshot struct {
	PositionQty float64
	OpenOrders  int
	UpdatedAt   time.Time
}

type DriftSample struct {
	PositionDelta float64
	OrderDelta    int
	At            time.Time
}

func (s DriftSample) Score() float64 {
	return math.Abs(s.PositionDelta) + float64(absInt(s.OrderDelta))
}

type DriftEvent struct {
	Severity DriftSeverity
	Sample   DriftSample
}

//
// ============================
// Stateless Classifier
// ============================
//

type DriftAccumulator struct {
	smallThreshold  float64
	mediumThreshold float64
	largeThreshold  float64
}

func NewDriftAccumulator(small, medium, large float64) *DriftAccumulator {
	if small <= 0 {
		small = 0.00000001
	}
	if medium <= small {
		medium = small * 3
	}
	if large <= medium {
		large = medium * 3
	}
	return &DriftAccumulator{
		smallThreshold:  small,
		mediumThreshold: medium,
		largeThreshold:  large,
	}
}

func (a *DriftAccumulator) Classify(s DriftSample) DriftSeverity {
	score := s.Score()

	switch {
	case score >= a.largeThreshold:
		return DriftLarge
	case score >= a.mediumThreshold:
		return DriftMedium
	case score >= a.smallThreshold:
		return DriftSmall
	default:
		return DriftNone
	}
}

func CompareSnapshots(local, exchange StateSnapshot) DriftSample {
	return DriftSample{
		PositionDelta: exchange.PositionQty - local.PositionQty,
		OrderDelta:    exchange.OpenOrders - local.OpenOrders,
		At:            time.Now(),
	}
}

//
// ============================
// Interfaces (PURE READ ONLY)
// ============================
//

type StateManager interface {
	LocalState(ctx context.Context) (StateSnapshot, error)
	ExchangeState(ctx context.Context) (StateSnapshot, error)
}

type RiskEngine interface {
	EmitDriftEvent(ctx context.Context, event DriftEvent) error
}

//
// ============================
// Reconciliation Loop (PURE OBSERVER)
// ============================
//

type LoopConfig struct {
	Interval       time.Duration
	SmallThreshold  float64
	MediumThreshold float64
	LargeThreshold  float64
}

type ReconciliationLoop struct {
	manager StateManager
	risk    RiskEngine
	classifier *DriftAccumulator
	cfg     LoopConfig
}

func NewReconciliationLoop(manager StateManager, risk RiskEngine, cfg LoopConfig) *ReconciliationLoop {
	if cfg.Interval <= 0 {
		cfg.Interval = 2 * time.Second
	}

	return &ReconciliationLoop{
		manager: manager,
		risk:    risk,
		classifier: NewDriftAccumulator(
			cfg.SmallThreshold,
			cfg.MediumThreshold,
			cfg.LargeThreshold,
		),
		cfg: cfg,
	}
}

func (l *ReconciliationLoop) Run(ctx context.Context) {
	ticker := time.NewTicker(l.cfg.Interval)
	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, _ = l.reconcileOnce(ctx)
			}
		}
	}()
}

func (l *ReconciliationLoop) ReconcileOnce(ctx context.Context) (DriftSeverity, error) {
	return l.reconcileOnce(ctx)
}

func (l *ReconciliationLoop) reconcileOnce(ctx context.Context) (DriftSeverity, error) {
	local, err := l.manager.LocalState(ctx)
	if err != nil {
		return DriftNone, err
	}

	ex, err := l.manager.ExchangeState(ctx)
	if err != nil {
		return DriftNone, err
	}

	sample := CompareSnapshots(local, ex)
	severity := l.classifier.Classify(sample)

	if severity != DriftNone && l.risk != nil {
		_ = l.risk.EmitDriftEvent(ctx, DriftEvent{
			Severity: severity,
			Sample:   sample,
		})
	}

	return severity, nil
}

func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}
