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

import "sync"

// KeyedMutex hands out one mutex per key so that work on different nodes can
// proceed concurrently while work on the same node is serialized. The zero
// value is ready to use; mutexes are created lazily on first Lock.
type KeyedMutex struct {
	guard sync.Mutex
	locks map[string]*sync.Mutex
}

// UnlockFunc releases a lock obtained from KeyedMutex.Lock.
type UnlockFunc = func()

// Lock blocks until the mutex for key is available and returns the function
// that releases it.
func (k *KeyedMutex) Lock(key string) UnlockFunc {
	k.guard.Lock()
	if k.locks == nil {
		k.locks = make(map[string]*sync.Mutex)
	}
	m, ok := k.locks[key]
	if !ok {
		m = &sync.Mutex{}
		k.locks[key] = m
	}
	k.guard.Unlock()

	m.Lock()
	return m.Unlock
}
