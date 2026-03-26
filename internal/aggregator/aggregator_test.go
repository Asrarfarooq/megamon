package aggregator

import (
	"context"
	"sync"
	"testing"
	"time"

	"example.com/megamon/internal/aggregator/events"
	"example.com/megamon/internal/metrics"
	"example.com/megamon/internal/records"
	"go.opentelemetry.io/otel/metric/noop"
)

type mockPoller struct {
	mtx       sync.Mutex
	pollCount int
}

func (m *mockPoller) PollResources(ctx context.Context) (map[string]records.Upness, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.pollCount++
	return map[string]records.Upness{"test-uid": {ReadyCount: 1, ExpectedCount: 1}}, nil
}

func (m *mockPoller) GetPollCount() int {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return m.pollCount
}

type mockSummaryProducer struct {
	mtx            sync.Mutex
	aggregateCount int
}

func (m *mockSummaryProducer) GenerateSummaries(ctx context.Context, now time.Time, eventLog EventLog, sliceEnabled, lwsEnabled bool, report *records.Report) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.aggregateCount++
	return nil
}

func (m *mockSummaryProducer) GetAggregateCount() int {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return m.aggregateCount
}

type mockEventLog struct {
	mu    sync.Mutex
	state map[string]map[string]records.Upness
}

func (m *mockEventLog) AppendStateChange(ctx context.Context, now time.Time, filename string, ups map[string]records.Upness) (map[string]records.EventRecords, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state == nil {
		m.state = make(map[string]map[string]records.Upness)
	}
	m.state[filename] = ups
	return nil, nil
}

func (m *mockEventLog) AppendStateChanges(ctx context.Context, now time.Time, changes map[string]map[string]records.Upness) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state == nil {
		m.state = make(map[string]map[string]records.Upness)
	}
	for filename, ups := range changes {
		m.state[filename] = ups
	}
	return nil
}

func (m *mockEventLog) GetLatestObservedState(filename string) map[string]records.Upness {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state[filename]
}

func (m *mockEventLog) GetStore() events.EventStore {
	return m
}

func (m *mockEventLog) Get(ctx context.Context, filename string) (map[string]records.EventRecords, error) {
	return nil, nil
}

func (m *mockEventLog) Put(ctx context.Context, filename string, recs map[string]records.EventRecords) error {
	return nil
}

func TestAggregator_SeparateIntervals(t *testing.T) {
	// Initialize metrics with no-op to avoid panic
	metrics.AggregationDuration, _ = noop.NewMeterProvider().Meter("test").Float64Histogram("test")

	poller := &mockPoller{}
	producer := &mockSummaryProducer{}
	mockEL := &mockEventLog{}

	a := &Aggregator{
		NodePoller:          poller,
		SummaryProducer:     producer,
		EventStore:          mockEL,
		EventLog:            mockEL,
		AggregationInterval: 100 * time.Millisecond,
		PollingInterval:     10 * time.Millisecond,
	}
	a.Init()

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- a.Start(ctx)
	}()

	// Wait for some cycles
	time.Sleep(200 * time.Millisecond)

	pollCount := poller.GetPollCount()
	aggCount := producer.GetAggregateCount()

	t.Logf("Poll count: %d, Aggregation count: %d", pollCount, aggCount)

	// Expect pollCount to be significantly higher than aggCount
	if pollCount <= aggCount {
		t.Errorf("Expected pollCount (%d) to be > aggCount (%d)", pollCount, aggCount)
	}

	if aggCount == 0 {
		t.Errorf("Expected at least one aggregation")
	}

	cancel()
	<-errCh
}
