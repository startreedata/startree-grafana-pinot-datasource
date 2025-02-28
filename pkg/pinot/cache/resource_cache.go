package cache

import (
	"sync"
	"time"
)

type Loader[V any] func() (val V, err error)

type ResourceCache[V any] struct {
	entry *cacheEntry[V]
	ttl   time.Duration
}

func NewResourceCache[V any](ttl time.Duration) *ResourceCache[V] {
	return &ResourceCache[V]{
		entry: newCacheEntry[V](),
		ttl:   ttl,
	}
}

func (x *ResourceCache[V]) Get(loader Loader[V]) (V, error) {
	return x.entry.getVal(x.ttl, loader)
}

type MultiResourceCache[K comparable, V any] struct {
	entries map[K]*cacheEntry[V]
	ttl     time.Duration
	lock    sync.RWMutex
}

func NewMultiResourceCache[K comparable, V any](ttl time.Duration) *MultiResourceCache[K, V] {
	return &MultiResourceCache[K, V]{
		entries: make(map[K]*cacheEntry[V]),
		ttl:     ttl,
	}
}

func (x *MultiResourceCache[K, V]) Get(key K, loader Loader[V]) (V, error) {
	return x.getEntry(key).getVal(x.ttl, loader)
}

func (x *MultiResourceCache[K, V]) getEntry(key K) *cacheEntry[V] {
	x.lock.Lock()
	defer x.lock.Unlock()
	if entry := x.entries[key]; entry == nil {
		x.entries[key] = newCacheEntry[V]()
	}
	return x.entries[key]
}

type cacheEntry[V any] struct {
	value *V
	lock  sync.Mutex
}

func newCacheEntry[V any]() *cacheEntry[V] { return &cacheEntry[V]{} }

func (x *cacheEntry[V]) getVal(ttl time.Duration, loader Loader[V]) (V, error) {
	x.lock.Lock()
	defer x.lock.Unlock()

	if x.value != nil {
		return *x.value, nil
	}

	val, err := loader()
	if err != nil {
		return val, err
	}

	x.value = &val
	go x.expireAfter(ttl)
	return val, nil
}

func (x *cacheEntry[V]) expireAfter(ttl time.Duration) {
	time.Sleep(ttl)
	x.lock.Lock()
	defer x.lock.Unlock()
	x.value = nil
}
