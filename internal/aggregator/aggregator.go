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

	// Optional decoupled polling loop for reading resource states independently.
	go func() {
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

					if _, err := a.EventLog.AppendStateChange(ctx, time.Now(), "node-pools.json", ups); err != nil {
						log.Error(err, "failed to append node pool state changes")
					}
				}()
			}
		}
	}()

	// Main aggregation loop that calculates and reports metrics.
	t := time.NewTicker(a.AggregationInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
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

	report.JobSetsUp = a.EventLog.GetLatestObservedState("jobsets.json")
	if !a.SliceEnabled {
		report.JobSetNodesUp = a.EventLog.GetLatestObservedState("jobset-nodes.json")
	}
	report.NodePoolsUp = a.EventLog.GetLatestObservedState("node-pools.json")
	if a.SliceEnabled {
		report.SlicesUp = a.EventLog.GetLatestObservedState("slices.json")
	}
	if a.LeaderWorkerSetEnabled {
		report.LeaderWorkerSetsUp = a.EventLog.GetLatestObservedState("leader-worker-sets.json")
	}

	now := time.Now()
	if err := a.SummaryProducer.GenerateSummaries(ctx, now, a.EventStore, a.SliceEnabled, a.LeaderWorkerSetEnabled, &report); err != nil {
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
