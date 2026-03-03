package upgrade

import (
	"sync"
)

type KeyedMutex struct {
	mutexes sync.Map // Zero value is empty and ready for use
}

type UnlockFunc = func()

func (m *KeyedMutex) Lock(key string) UnlockFunc {
	value, _ := m.mutexes.LoadOrStore(key, &sync.Mutex{})
	mtx, ok := value.(*sync.Mutex)
	if !ok {
		panic("object is not of type sync.Mutex which is what was expected")
	}
	mtx.Lock()
	return func() { mtx.Unlock() }
}

type StringSet struct {
	m  map[string]bool
	mu sync.RWMutex
}

func NewStringSet() *StringSet {
	return &StringSet{
		m:  make(map[string]bool),
		mu: sync.RWMutex{},
	}
}

func (s *StringSet) Add(item string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[item] = true
}

func (s *StringSet) Remove(item string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, item)
}

func (s *StringSet) Has(item string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.m[item]
	return ok
}

func (s *StringSet) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m = make(map[string]bool)
}
