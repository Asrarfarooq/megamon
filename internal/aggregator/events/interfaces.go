package events

import (
	"context"

	"example.com/megamon/internal/gcsclient"
	"example.com/megamon/internal/records"
)

type EventStore interface {
	Get(ctx context.Context, filename string) (map[string]records.EventRecords, error)
	Put(ctx context.Context, filename string, recs map[string]records.EventRecords) error
}

type GCSClient interface {
	gcsclient.GCSClient
}
