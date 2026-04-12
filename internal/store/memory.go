package store

import (
	"sync"
)

type memoryStore struct {
	mu   sync.RWMutex
	data map[string]string
}

func newMemoryStore() *memoryStore {
	return &memoryStore{data: map[string]string{}}
}

func (m *memoryStore) Set(key string, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[key] = value

	return nil
}

func (m *memoryStore) Get(key string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	value, ok := m.data[key]
	if !ok {
		return "", &NotFoundError{}
	}

	return value, nil
}

func (m *memoryStore) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.data, key)

	return nil
}

func (m *memoryStore) Clear() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	clear(m.data)

	return nil
}
