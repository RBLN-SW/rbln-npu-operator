package upgrade

import "testing"

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
