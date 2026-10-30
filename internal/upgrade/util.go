/*
Copyright 2022 NVIDIA CORPORATION & AFFILIATES

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/*
Modifications Copyright 2026 Rebellions Inc.

This file is derived from github.com/NVIDIA/k8s-operator-libs
(pkg/upgrade/util.go, commit a0a0256b) and has been
modified by Rebellions Inc. See NOTICE for details.
*/

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
