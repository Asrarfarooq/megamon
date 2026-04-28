package events

import (
	"context"
	"testing"
	"time"

	"example.com/megamon/internal/records"
	"github.com/stretchr/testify/require"
)

type fakeGCSClient struct {
	records map[string]map[string]records.EventRecords
	errGet  error
	errPut  error
}

func (f *fakeGCSClient) GetRecords(ctx context.Context, bucket, path string) (map[string]records.EventRecords, error) {
	if f.errGet != nil {
		return nil, f.errGet
	}
	if f.records == nil {
		f.records = make(map[string]map[string]records.EventRecords)
	}
	if recs, ok := f.records[path]; ok {
		return recs, nil
	}
	return make(map[string]records.EventRecords), nil
}

func (f *fakeGCSClient) PutRecords(ctx context.Context, bucket, path string, recs map[string]records.EventRecords) error {
	if f.errPut != nil {
		return f.errPut
	}
	if f.records == nil {
		f.records = make(map[string]map[string]records.EventRecords)
	}
	f.records[path] = recs
	return nil
}

func TestAppendStateChange(t *testing.T) {
	fakeGCS := &fakeGCSClient{}
	store := NewGCSEventStore(fakeGCS, "test-bucket", "test-path")
	reconciler := NewEventLogImpl(store, 0.5)

	now := time.Now()
	ups := map[string]records.Upness{
		"obj1": {
			ExpectedCount: 1,
			ReadyCount:    1,
			Attrs: records.Attrs{
				JobSetName: "test-js",
			},
		},
	}

	recs, err := reconciler.AppendStateChange(context.Background(), now, records.EventKeyJobSets, ups)
	require.NoError(t, err)
	require.NotNil(t, recs)

	// Verify it put the records into fake GCS
	saved, ok := fakeGCS.records["test-path/"+records.EventKeyJobSets]
	require.True(t, ok)
	require.Contains(t, saved, "obj1")
}
