package aggregator

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"time"

	"example.com/megamon/internal/aggregator/events"
	"example.com/megamon/internal/metrics"
	"example.com/megamon/internal/records"
	containerv1beta1 "google.golang.org/api/container/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var log = logf.Log.WithName("aggregator")

type GKEClient interface {
	ListNodePools(ctx context.Context) ([]*containerv1beta1.NodePool, error)
}

type Aggregator struct {
	client.Client

	NodePoller      ResourcePoller
	EventStore      events.EventStore
	EventLog        EventLog
	SummaryProducer SummaryProducer

	Exporters map[string]Exporter
	GKE       GKEClient

	EventsBucketName string
	EventsBucketPath string

	AggregationInterval    time.Duration
	PollingInterval        time.Duration
	UnknownCountThreshold  float64
	SliceEnabled           bool
	LeaderWorkerSetEnabled bool

	reportMtx   sync.RWMutex
	report      records.Report
	reportReady bool

	nodePoolSchedulingMtx sync.RWMutex
	// map[<nodepool-name>]<details-about-what-is-scheduled-on-it>
	nodePoolScheduling map[string]records.ScheduledJob
}

func (a *Aggregator) Start(ctx context.Context) error {
	if a.PollingInterval <= 0 {
		a.PollingInterval = a.AggregationInterval
		log.Info("polling interval not set, defaulting to aggregation interval", "pollingInterval", a.PollingInterval)
	}

	log.Info("starting aggregator", "aggregationInterval", a.AggregationInterval, "pollingInterval", a.PollingInterval)

	var wg sync.WaitGroup

	// Optional decoupled polling loop for reading resource states independently.
	wg.Go(func() {
		t := time.NewTicker(a.PollingInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				func() {
					log.V(3).Info("polling nodes")
					ups, err := a.NodePoller.PollResources(ctx)
					if err != nil {
						log.Error(err, "failed to poll nodes")
						return
					}

					if _, err := a.EventLog.AppendStateChange(ctx, time.Now(), records.EventKeyNodePools, ups); err != nil {
						log.Error(err, "failed to append nodepools state change")
					}
				}()
			}
		}
	})

	// Main aggregation loop that calculates and reports metrics.
	wg.Go(func() {
		t := time.NewTicker(a.AggregationInterval)
		defer t.Stop()

		// Determine required keys for initial readiness
		requiredKeys := []string{records.EventKeyJobSets, records.EventKeyNodePools}
		if a.SliceEnabled {
			requiredKeys = append(requiredKeys, records.EventKeySlices)
		} else {
			requiredKeys = append(requiredKeys, records.EventKeyJobSetNodes)
		}
		if a.LeaderWorkerSetEnabled {
			requiredKeys = append(requiredKeys, records.EventKeyLeaderWorkerSets)
		}

		isPopulated := false

		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				// Ensure the application passes its readiness probe immediately
				// even if observers haven't run yet, so K8s doesn't kill it.
				a.reportMtx.Lock()
				a.reportReady = true
				a.reportMtx.Unlock()

				if !isPopulated {
					if !a.EventLog.IsPopulated(requiredKeys) {
						log.Info("skipping aggregation, waiting for observers to populate initial state", "requiredKeys", requiredKeys)
						continue
					}
					log.Info("initial state populated, proceeding with aggregation")
					isPopulated = true
				}

				log.Info("aggregating")
				start := time.Now()
				if err := a.Aggregate(ctx); err != nil {
					log.Error(err, "failed to aggregate")
					continue
				}
				metrics.AggregationDuration.Record(ctx, time.Since(start).Seconds())

				for name, exporter := range a.Exporters {
					if err := exporter.Export(ctx, a.Report()); err != nil {
						log.Error(err, "failed to export", "exporter", name)
					}
				}
			}
		}
	})

	// Wait for all goroutines to finish gracefully
	wg.Wait()
	return ctx.Err()
}

func (a *Aggregator) Report() records.Report {
	a.reportMtx.RLock()
	defer a.reportMtx.RUnlock()
	return a.report
}

func (a *Aggregator) ReportReady() bool {
	a.reportMtx.RLock()
	defer a.reportMtx.RUnlock()
	return a.reportReady
}

func (a *Aggregator) Init() {
	a.nodePoolScheduling = make(map[string]records.ScheduledJob)
}

func (a *Aggregator) Aggregate(ctx context.Context) error {
	report := records.NewReport()

	now := time.Now()
	if err := a.SummaryProducer.GenerateSummaries(ctx, now, a.EventLog, a.SliceEnabled, a.LeaderWorkerSetEnabled, &report); err != nil {
		return fmt.Errorf("generating summaries: %w", err)
	}

	report.NodePoolScheduling = a.getNodePoolScheduling()

	a.reportMtx.Lock()
	a.report = report
	a.reportReady = true
	a.reportMtx.Unlock()

	return nil
}

func (a *Aggregator) UpdateNodePoolScheduling(nodepool string, js records.ScheduledJob) {
	a.nodePoolSchedulingMtx.Lock()
	defer a.nodePoolSchedulingMtx.Unlock()
	if a.nodePoolScheduling == nil {
		a.nodePoolScheduling = make(map[string]records.ScheduledJob)
	}
	a.nodePoolScheduling[nodepool] = js
}

func (a *Aggregator) DeleteNodePoolScheduling(nodepool string) {
	a.nodePoolSchedulingMtx.Lock()
	defer a.nodePoolSchedulingMtx.Unlock()
	if a.nodePoolScheduling != nil {
		delete(a.nodePoolScheduling, nodepool)
	}
}

func (a *Aggregator) getNodePoolScheduling() map[string]records.ScheduledJob {
	a.nodePoolSchedulingMtx.RLock()
	defer a.nodePoolSchedulingMtx.RUnlock()
	if a.nodePoolScheduling == nil {
		return nil
	}
	cp := make(map[string]records.ScheduledJob, len(a.nodePoolScheduling))
	maps.Copy(cp, a.nodePoolScheduling)
	return cp
}
