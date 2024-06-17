package cache

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestResourceCache_Get(t *testing.T) {
	ctx := context.Background()

	t.Run("when fresh", func(t *testing.T) {
		cache := NewResourceCache[int](time.Hour)
		_, _ = cache.Get(ctx, func(ctx context.Context) (int, error) {
			return -1, nil
		})

		for i := 0; i < 10; i++ {
			go func(idx int) {
				_, _ = cache.Get(ctx, func(ctx context.Context) (int, error) {
					return idx, nil
				})
			}(i)
		}

		gotVal, gotErr := cache.Get(ctx, func(ctx context.Context) (int, error) {
			return 10, nil
		})

		assert.Equal(t, -1, gotVal)
		assert.Equal(t, nil, gotErr)
	})

	t.Run("when expired", func(t *testing.T) {
		cache := NewResourceCache[int](0)
		for i := 0; i < 10; i++ {
			go func(idx int) {
				_, _ = cache.Get(ctx, func(ctx context.Context) (int, error) {
					return idx, nil
				})
			}(i)
		}

		gotVal, gotErr := cache.Get(ctx, func(ctx context.Context) (int, error) {
			return 10, nil
		})

		assert.Equal(t, 10, gotVal)
		assert.Equal(t, nil, gotErr)
	})

	t.Run("when error", func(t *testing.T) {
		cache := NewResourceCache[int](time.Hour)
		for i := 0; i < 10; i++ {
			go func(idx int) {
				_, _ = cache.Get(ctx, func(ctx context.Context) (int, error) {
					return idx, errors.New("error")
				})
			}(i)
		}

		gotVal, gotErr := cache.Get(ctx, func(ctx context.Context) (int, error) {
			return 10, nil
		})
		assert.Equal(t, 10, gotVal)
		assert.Equal(t, nil, gotErr)
	})

}

func TestMultiResourceCache_Get(t *testing.T) {

}
