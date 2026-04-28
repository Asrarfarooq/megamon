// Package report is responsible for generating final time-based summaries.
// It acts as the "Transformer" for the Aggregator, taking reconciled historical events and raw upness attributes to calculate total uptime, downtime, and MTTR metrics.
package report

import (
	"context"
	"fmt"
	"time"

	"example.com/megamon/internal/aggregator"
	"example.com/megamon/internal/records"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var log = logf.Log.WithName("report-producer")

type Producer struct{}

func NewProducer() *Producer {
	return &Producer{}
}

func (p *Producer) GenerateSummaries(ctx context.Context, now time.Time, eventLog aggregator.EventLog, sliceEnabled, lwsEnabled bool, report *records.Report) error {
	jobsetContext := logf.IntoContext(ctx, log.WithValues("type", "jobsets"))
	jobsetNodesContext := logf.IntoContext(ctx, log.WithValues("type", "jobset-nodes"))
	nodePoolsContext := logf.IntoContext(ctx, log.WithValues("type", "nodepools"))
	slicesContext := logf.IntoContext(ctx, log.WithValues("type", "slices"))
	lwsContext := logf.IntoContext(ctx, log.WithValues("type", "leader-worker-sets"))

	store := eventLog.GetStore()

	// Populate latest upness from the observed store
	report.JobSetsUp = eventLog.GetLatestObservedState(records.EventKeyJobSets)
	report.NodePoolsUp = eventLog.GetLatestObservedState(records.EventKeyNodePools)

	if !sliceEnabled {
		report.JobSetNodesUp = eventLog.GetLatestObservedState(records.EventKeyJobSetNodes)
	}

	if sliceEnabled {
		report.SlicesUp = eventLog.GetLatestObservedState(records.EventKeySlices)
	}

	if lwsEnabled {
		report.LeaderWorkerSetsUp = eventLog.GetLatestObservedState(records.EventKeyLeaderWorkerSets)
	}

	// Fetch all events from GCS for summarizing (consistent source of truth)
	jsEvents, err := store.Get(jobsetContext, records.EventKeyJobSets)
	if err != nil {
		return fmt.Errorf("getting jobset events from GCS: %w", err)
	}

	var jsNodeEvents map[string]records.EventRecords
	if !sliceEnabled {
		jsNodeEvents, err = store.Get(jobsetNodesContext, records.EventKeyJobSetNodes)
		if err != nil {
			return fmt.Errorf("getting jobset node events from GCS: %w", err)
		}
	}

	nodePoolEvents, err := store.Get(nodePoolsContext, records.EventKeyNodePools)
	if err != nil {
		return fmt.Errorf("getting nodepool events from GCS: %w", err)
	}

	var sliceEvents map[string]records.EventRecords
	if sliceEnabled {
		sliceEvents, err = store.Get(slicesContext, records.EventKeySlices)
		if err != nil {
			return fmt.Errorf("getting slice events from GCS: %w", err)
		}
	}

	var lwsEvents map[string]records.EventRecords
	if lwsEnabled {
		lwsEvents, err = store.Get(lwsContext, records.EventKeyLeaderWorkerSets)
		if err != nil {
			return fmt.Errorf("getting lws events from GCS: %w", err)
		}
	}

	// Update summaries
	report.JobSetsUpSummaries = p.Summarize(jobsetContext, now, report.JobSetsUp, jsEvents)
	report.NodePoolsUpSummaries = p.Summarize(nodePoolsContext, now, report.NodePoolsUp, nodePoolEvents)
	if !sliceEnabled {
		report.JobSetNodesUpSummaries = p.Summarize(jobsetNodesContext, now, report.JobSetNodesUp, jsNodeEvents)
	}
	if sliceEnabled {
		report.SlicesUpSummaries = p.Summarize(slicesContext, now, report.SlicesUp, sliceEvents)
	}
	if lwsEnabled {
		report.LeaderWorkerSetsUpSummaries = p.Summarize(lwsContext, now, report.LeaderWorkerSetsUp, lwsEvents)
	}

	return nil
}

func (p *Producer) Summarize(ctx context.Context, now time.Time, ups map[string]records.Upness, events map[string]records.EventRecords) map[string]records.UpnessSummaryWithAttrs {
	summaries := make(map[string]records.UpnessSummaryWithAttrs, len(events))
	for key, evs := range events {
		eventSummary := evs.Summarize(ctx, now)

		var attrs records.Attrs
		if up, ok := ups[key]; ok {
			attrs = up.Attrs
		}

		summaries[key] = records.UpnessSummaryWithAttrs{
			Attrs:        attrs,
			EventSummary: eventSummary,
		}
	}
	return summaries
}
