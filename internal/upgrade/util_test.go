/*
Copyright 2026 Rebellions Inc.

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

package upgrade

import (
	"sync"
	"testing"
)

func TestStringSet(t *testing.T) {
	t.Run("add and has", func(t *testing.T) {
		s := NewStringSet()
		s.Add("a")
		if !s.Has("a") {
			t.Fatal("expected Has(a) = true")
		}
		if s.Has("b") {
			t.Fatal("expected Has(b) = false")
		}
	})

	t.Run("remove", func(t *testing.T) {
		s := NewStringSet()
		s.Add("a")
		s.Remove("a")
		if s.Has("a") {
			t.Fatal("expected Has(a) = false after Remove")
		}
	})

	t.Run("concurrent access", func(t *testing.T) {
		s := NewStringSet()
		var wg sync.WaitGroup
		for range 100 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				key := "key"
				s.Add(key)
				s.Has(key)
				s.Remove(key)
			}()
		}
		wg.Wait()
	})
}

func TestKeyedMutex(t *testing.T) {
	t.Run("lock and unlock same key", func(t *testing.T) {
		km := &KeyedMutex{}
		unlock := km.Lock("key1")
		unlock()
		// Should not deadlock when re-locking
		unlock2 := km.Lock("key1")
		unlock2()
	})

	t.Run("different keys do not block each other", func(t *testing.T) {
		km := &KeyedMutex{}
		unlock1 := km.Lock("key1")
		defer unlock1()

		// key2 should not be blocked by key1
		done := make(chan struct{})
		go func() {
			unlock2 := km.Lock("key2")
			unlock2()
			close(done)
		}()
		<-done
	})
}
