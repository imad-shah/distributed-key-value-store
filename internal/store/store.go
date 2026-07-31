package store

import (
	"sync"
)

// Store is a thread-safe, in-memory, key-value store
type Store struct {
	mu   sync.RWMutex
	data map[string]VersionedValue
}

func New() *Store {
	return &Store{
		data: make(map[string]VersionedValue),
	}
}

func (s *Store) Get(key string) (VersionedValue, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, ok := s.data[key]
	return val, ok
}

func (s *Store) Put(key string, incoming VersionedValue) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	cur, ok := s.data[key]

	if !ok || incoming.Version.After(cur.Version) {
		s.data[key] = incoming
		return true
	}

	if incoming.Version.Equal(cur.Version) {
		return incoming == cur
	}

	return false
}
