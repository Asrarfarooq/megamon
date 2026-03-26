package aggregator

import (
	"context"
	"time"

	"example.com/megamon/internal/aggregator/events"
	"example.com/megamon/internal/records"
)

type ResourcePoller interface {
	PollResources(ctx context.Context) (map[string]records.Upness, error)
}

type EventLog interface {
	AppendStateChange(ctx context.Context, now time.Time, key string, ups map[string]records.Upness) (map[string]records.EventRecords, error)
	AppendStateChanges(ctx context.Context, now time.Time, changes map[string]map[string]records.Upness) error
	GetLatestObservedState(key string) map[string]records.Upness
	GetStore() events.EventStore
}

type SummaryProducer interface {
	GenerateSummaries(ctx context.Context, now time.Time, eventLog EventLog, sliceEnabled, lwsEnabled bool, report *records.Report) error
}

type Exporter interface {
	Export(ctx context.Context, report records.Report) error
}
