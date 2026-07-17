package parser

import (
	"context"
	"errors"
)

type reconciliationCache struct {
	index *discoveryDiskMap
}

type reconciliationCacheContextKey struct{}

// WithReconciliationCache attaches one disk-backed cache whose lifetime is the
// complete reconciliation, including all spool pages and parse workers.
func WithReconciliationCache(
	ctx context.Context,
) (context.Context, func() error, error) {
	index, err := newDiscoveryDiskMapForContext(ctx)
	if err != nil {
		return ctx, nil, err
	}
	cache := &reconciliationCache{index: index}
	return context.WithValue(ctx, reconciliationCacheContextKey{}, cache), index.close, nil
}

func reconciliationCachePut(ctx context.Context, key, value string) error {
	cache, _ := ctx.Value(reconciliationCacheContextKey{}).(*reconciliationCache)
	if cache == nil {
		return nil
	}
	return cache.index.put(ctx, key, value, true)
}

func reconciliationCacheAvailable(ctx context.Context) bool {
	cache, _ := ctx.Value(reconciliationCacheContextKey{}).(*reconciliationCache)
	return cache != nil
}

func reconciliationCacheGet(ctx context.Context, key string) (string, bool, error) {
	cache, _ := ctx.Value(reconciliationCacheContextKey{}).(*reconciliationCache)
	if cache == nil {
		return "", false, nil
	}
	return cache.index.get(ctx, key)
}

func reconciliationCacheAppend(ctx context.Context, key, value string) error {
	cache, _ := ctx.Value(reconciliationCacheContextKey{}).(*reconciliationCache)
	if cache == nil {
		return nil
	}
	return cache.index.append(ctx, key, value)
}

func reconciliationCacheAddInt(ctx context.Context, key string) (int, error) {
	cache, _ := ctx.Value(reconciliationCacheContextKey{}).(*reconciliationCache)
	if cache == nil {
		return 0, errors.New("reconciliation cache unavailable")
	}
	var next int
	err := cache.index.db.QueryRowContext(ctx, `
		INSERT INTO entries (key, ordinal, value) VALUES (?, 0, '1')
		ON CONFLICT(key, ordinal) DO UPDATE
		SET value = CAST(value AS INTEGER) + 1
		RETURNING CAST(value AS INTEGER)
	`, key).Scan(&next)
	return next - 1, err
}
