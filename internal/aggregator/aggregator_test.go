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
	mu                  sync.Mutex
	state               map[string]map[string]records.Upness
	isPopulatedOverride *bool
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

func (m *mockEventLog) IsPopulated(keys []string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isPopulatedOverride != nil {
		return *m.isPopulatedOverride
	}

	if m.state == nil {
		return false
	}
	for _, key := range keys {
		if _, exists := m.state[key]; !exists {
			return false
		}
	}
	return true
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
	populated := true
	mockEL := &mockEventLog{isPopulatedOverride: &populated}

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

func TestAggregator_StartupDelay(t *testing.T) {
	metrics.AggregationDuration, _ = noop.NewMeterProvider().Meter("test").Float64Histogram("test")

	t.Run("waits for population", func(t *testing.T) {
		poller := &mockPoller{}
		producer := &mockSummaryProducer{}
		notPopulated := false
		mockEL := &mockEventLog{isPopulatedOverride: &notPopulated}

		a := &Aggregator{
			NodePoller:          poller,
			SummaryProducer:     producer,
			EventStore:          mockEL,
			EventLog:            mockEL,
			AggregationInterval: 50 * time.Millisecond,
			PollingInterval:     10 * time.Millisecond,
		}
		a.Init()

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond) // shorter than grace period
		defer cancel()

		go a.Start(ctx)

		time.Sleep(75 * time.Millisecond)

		// Aggregation should be skipped because it's not populated and grace period hasn't expired.
		if count := producer.GetAggregateCount(); count != 0 {
			t.Errorf("Expected 0 aggregations while waiting for population, got %d", count)
		}
	})

	t.Run("proceeds after grace period", func(t *testing.T) {
		poller := &mockPoller{}
		producer := &mockSummaryProducer{}
		notPopulated := false
		mockEL := &mockEventLog{isPopulatedOverride: &notPopulated}

		a := &Aggregator{
			NodePoller:          poller,
			SummaryProducer:     producer,
			EventStore:          mockEL,
			EventLog:            mockEL,
			AggregationInterval: 20 * time.Millisecond, // grace period is 3x = 60ms
			PollingInterval:     10 * time.Millisecond,
		}
		a.Init()

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		go a.Start(ctx)

		// Wait long enough for the grace period (60ms) + 1 aggregation tick to pass
		time.Sleep(100 * time.Millisecond)

		if count := producer.GetAggregateCount(); count == 0 {
			t.Errorf("Expected at least 1 aggregation after grace period expired, got 0")
		}
	})

	t.Run("proceeds immediately when populated", func(t *testing.T) {
		poller := &mockPoller{}
		producer := &mockSummaryProducer{}
		populated := true
		mockEL := &mockEventLog{isPopulatedOverride: &populated}

		a := &Aggregator{
			NodePoller:          poller,
			SummaryProducer:     producer,
			EventStore:          mockEL,
			EventLog:            mockEL,
			AggregationInterval: 50 * time.Millisecond, // grace period is 150ms
			PollingInterval:     10 * time.Millisecond,
		}
		a.Init()

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		go a.Start(ctx)

		// Wait for one interval, but much less than the grace period
		time.Sleep(75 * time.Millisecond)

		// Since IsPopulated is true, it should have aggregated at least once!
		if count := producer.GetAggregateCount(); count == 0 {
			t.Errorf("Expected aggregation to occur immediately because IsPopulated is true")
		}
	})
}
