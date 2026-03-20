package events

import (
	"maps"
	"sync"

	"example.com/megamon/internal/records"
)

// CurrentObservedStore maintains an in-memory cache of the latest resource observations.
// This allows the Aggregator to be stateless regarding watchers/pollers, while keeping
// the GCS Event Store lean (no Attrs or current counts in historical logs).
type CurrentObservedStore struct {
	mu    sync.RWMutex
	state map[string]map[string]records.Upness // filename -> resource key -> upness
}

func NewCurrentObservedStore() *CurrentObservedStore {
	return &CurrentObservedStore{
		state: make(map[string]map[string]records.Upness),
	}
}

func (s *CurrentObservedStore) Update(key string, ups map[string]records.Upness) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Replace the entire set for this key with the latest observation
	cp := make(map[string]records.Upness, len(ups))
	maps.Copy(cp, ups)
	s.state[key] = cp
}

func (s *CurrentObservedStore) Get(key string) map[string]records.Upness {
	s.mu.RLock()
	defer s.mu.RUnlock()

	original, ok := s.state[key]
	if !ok {
		return make(map[string]records.Upness)
	}

	// Return a copy to prevent external mutation of the cache
	cp := make(map[string]records.Upness, len(original))
	maps.Copy(cp, original)
	return cp
}
