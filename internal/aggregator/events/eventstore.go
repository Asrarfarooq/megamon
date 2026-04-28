package events

import (
	"context"
	"strings"
	"time"

	"example.com/megamon/internal/gcsclient"
	"example.com/megamon/internal/metrics"
	"example.com/megamon/internal/records"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type GCSEventStore struct {
	GCS              gcsclient.GCSClient
	EventsBucketName string
	EventsBucketPath string
}

func NewGCSEventStore(gcs gcsclient.GCSClient, bucketName, bucketPath string) *GCSEventStore {
	return &GCSEventStore{
		GCS:              gcs,
		EventsBucketName: bucketName,
		EventsBucketPath: bucketPath,
	}
}

// Get retrieves historical event records from GCS and records latency metrics.
func (s *GCSEventStore) Get(ctx context.Context, filename string) (map[string]records.EventRecords, error) {
	path := strings.TrimSuffix(s.EventsBucketPath, "/") + "/" + filename
	startGet := time.Now()
	recs, err := s.GCS.GetRecords(ctx, s.EventsBucketName, path)
	if metrics.GCSLatency != nil {
		metrics.GCSLatency.Record(ctx, time.Since(startGet).Seconds(), metric.WithAttributes(attribute.String("operation", "GetRecords")))
	}
	return recs, err
}

// Put saves historical event records to GCS and records latency metrics.
func (s *GCSEventStore) Put(ctx context.Context, filename string, recs map[string]records.EventRecords) error {
	path := strings.TrimSuffix(s.EventsBucketPath, "/") + "/" + filename
	startPut := time.Now()
	err := s.GCS.PutRecords(ctx, s.EventsBucketName, path, recs)
	if metrics.GCSLatency != nil {
		metrics.GCSLatency.Record(ctx, time.Since(startPut).Seconds(), metric.WithAttributes(attribute.String("operation", "PutRecords")))
	}
	return err
}
