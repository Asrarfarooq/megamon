package report

import (
	"context"
	"testing"
	"time"

	"example.com/megamon/internal/records"
	"github.com/stretchr/testify/require"
)

func TestSummarize(t *testing.T) {
	producer := NewProducer()

	ups := map[string]records.Upness{
		"obj1": {
			Attrs: records.Attrs{
				JobSetName: "test-js",
			},
		},
	}

	events := map[string]records.EventRecords{
		"obj1": {
			UpEvents: []records.UpEvent{
				{
					Up:        true,
					Timestamp: time.Unix(0, 0),
				},
				{
					Up:        false,
					Timestamp: time.Unix(1, 0),
				},
			},
		},
	}

	now := time.Unix(2, 0)
	summaries := producer.Summarize(context.Background(), now, ups, events)

	require.Len(t, summaries, 1)
	summary, ok := summaries["obj1"]
	require.True(t, ok)
	require.Equal(t, "test-js", summary.Attrs.JobSetName)
}
