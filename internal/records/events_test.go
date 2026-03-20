package records

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSummarize(t *testing.T) {
	t.Parallel()

	t0, err := time.Parse(time.RFC3339, "2021-01-01T00:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]struct {
		records         EventRecords
		now             time.Time
		expectedSummary EventSummary
	}{
		"empty": {
			records:         EventRecords{},
			expectedSummary: EventSummary{},
		},
		"missing down0": {
			records: EventRecords{
				UpEvents: []UpEvent{
					{Up: true, Timestamp: t0},
				},
			},
			expectedSummary: EventSummary{},
		},
		"not up yet": {
			records: EventRecords{
				UpEvents: []UpEvent{
					{Up: false, Timestamp: t0},
				},
			},
			now: t0.Add(time.Hour),
			expectedSummary: EventSummary{
				DownTime: time.Hour,
			},
		},
		"just up": {
			records: EventRecords{
				UpEvents: []UpEvent{
					{Up: false, Timestamp: t0},
					{Up: true, Timestamp: t0.Add(time.Hour)},
				},
			},
			now: t0.Add(time.Hour),
			expectedSummary: EventSummary{
				DownTime:        time.Hour,
				DownTimeInitial: time.Hour,
			},
		},
		"up for 3 hours": {
			records: EventRecords{
				UpEvents: []UpEvent{
					{Up: false, Timestamp: t0},
					{Up: true, Timestamp: t0.Add(time.Hour)},
				},
			},
			now: t0.Add(time.Hour + 3*time.Hour),
			expectedSummary: EventSummary{
				DownTimeInitial: time.Hour,
				DownTime:        time.Hour,
				UpTime:          3 * time.Hour,
			},
		},
		"single interruption": {
			records: EventRecords{
				UpEvents: []UpEvent{
					{Up: false, Timestamp: t0},
					{Up: true, Timestamp: t0.Add(time.Hour)},
					{Up: false, Timestamp: t0.Add(2 * time.Hour)},
				},
			},
			now: t0.Add(2 * time.Hour),
			expectedSummary: EventSummary{
				DownTimeInitial:                 time.Hour,
				UpTime:                          time.Hour,
				DownTime:                        time.Hour,
				InterruptionCount:               1,
				TotalUpTimeBetweenInterruption:  time.Hour,
				MeanUpTimeBetweenInterruption:   time.Hour,
				LatestUpTimeBetweenInterruption: time.Hour,
			},
		},
		"expected downtime interruption": {
			records: EventRecords{
				UpEvents: []UpEvent{
					{Up: false, Timestamp: t0},
					{Up: true, Timestamp: t0.Add(time.Hour)},
					{Up: false, Timestamp: t0.Add(2 * time.Hour), ExpectedDown: true},
				},
			},
			now: t0.Add(2 * time.Hour),
			expectedSummary: EventSummary{
				DownTimeInitial:                 time.Hour,
				UpTime:                          time.Hour,
				DownTime:                        time.Hour,
				InterruptionCount:               0,
				TotalUpTimeBetweenInterruption:  0,
				MeanUpTimeBetweenInterruption:   0,
				LatestUpTimeBetweenInterruption: time.Hour,
			},
		},
	}

	ctx := context.Background()
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			gotSum := tc.records.Summarize(ctx, tc.now)
			require.Equal(t, tc.expectedSummary.UpTime, gotSum.UpTime, "UpTime")
			require.Equal(t, tc.expectedSummary.DownTime, gotSum.DownTime, "DownTime")
			require.Equal(t, tc.expectedSummary.DownTimeInitial, gotSum.DownTimeInitial, "DownTimeInitial")
			require.Equal(t, tc.expectedSummary.InterruptionCount, gotSum.InterruptionCount, "InterruptionCount")
			require.Equal(t, tc.expectedSummary.RecoveryCount, gotSum.RecoveryCount, "RecoveryCount")
		})
	}
}

func TestAppendStateChangeEvents(t *testing.T) {
	t.Parallel()

	now := time.Now()
	cases := map[string]struct {
		inputUps         map[string]Upness
		inputEvents      map[string]EventRecords
		expEvents        map[string]EventRecords
		expChanged       bool
		unknownThreshold float64
	}{
		"first event up": {
			inputUps: map[string]Upness{
				"abc": {
					ExpectedCount: 1,
					ReadyCount:    1,
				},
			},
			inputEvents: map[string]EventRecords{},
			expEvents: map[string]EventRecords{
				"abc": {
					UpEvents: []UpEvent{
						{Up: false, Timestamp: now},
						{Up: true, Timestamp: now},
					},
				},
			},
			unknownThreshold: 1.0,
			expChanged:       true,
		},
		"still up": {
			inputUps: map[string]Upness{
				"abc": {
					ExpectedCount: 1,
					ReadyCount:    1,
				},
			},
			inputEvents: map[string]EventRecords{
				"abc": {
					UpEvents: []UpEvent{
						{Up: false, Timestamp: now.Add(-2 * time.Minute)},
						{Up: true, Timestamp: now.Add(-time.Minute)},
					},
				},
			},
			expEvents: map[string]EventRecords{
				"abc": {
					UpEvents: []UpEvent{
						{Up: false, Timestamp: now.Add(-2 * time.Minute)},
						{Up: true, Timestamp: now.Add(-time.Minute)},
					},
				},
			},
			unknownThreshold: 1.0,
			expChanged:       false,
		},
	}

	ctx := context.Background()
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			events := c.inputEvents
			gotChanged := AppendStateChangeEvents(ctx, now, c.inputUps, events, c.unknownThreshold)
			require.Equal(t, c.expEvents, events)
			require.Equal(t, c.expChanged, gotChanged)
		})
	}
}
