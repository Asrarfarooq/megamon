// Package events is responsible for managing historical event records.
// It acts as the \"State Manager\" for the Aggregator, fetching past state from GCS, reconciling it against current upness, and persisting the updated event log.
package events

import (
	"context"
	"fmt"
	"time"

	"example.com/megamon/internal/records"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var log = logf.Log.WithName("eventlog")

type EventLogImpl struct {
	Store                 EventStore
	ObservedStore         *CurrentObservedStore
	UnknownCountThreshold float64
}

func NewEventLogImpl(store EventStore, threshold float64) *EventLogImpl {
	return &EventLogImpl{
		Store:                 store,
		ObservedStore:         NewCurrentObservedStore(),
		UnknownCountThreshold: threshold,
	}
}

// AppendStateChange compares current upness values with historical GCS events. If changes are detected, it updates GCS.
func (r *EventLogImpl) AppendStateChange(ctx context.Context, now time.Time, key string, ups map[string]records.Upness) (map[string]records.EventRecords, error) {
	// Update in-memory observed store
	r.ObservedStore.Update(key, ups)

	recs, err := r.Store.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get %q: %w", key, err)
	}

	if changed := records.AppendStateChangeEvents(ctx, now, ups, recs, r.UnknownCountThreshold); changed {
		if errPut := r.Store.Put(ctx, key, recs); errPut != nil {
			return nil, fmt.Errorf("failed to put %q: %w", key, errPut)
		}
	}

	return recs, nil
}

func (r *EventLogImpl) GetLatestObservedState(key string) map[string]records.Upness {
	return r.ObservedStore.Get(key)
}
