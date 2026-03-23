package limiter

import (
	"context"
	"sync"
	"time"
)

type TokenBucket struct {
	capacity   int
	tokens     float64
	refillRate float64
	lastRefill time.Time
	mu         sync.Mutex
}

func NewTokenBucket(capacity int, refillRate float64) *TokenBucket {
	return &TokenBucket{
		capacity:   capacity,
		tokens:     float64(capacity),
		refillRate: refillRate,
		lastRefill: time.Now(),
	}
}

func (tb *TokenBucket) refill() {
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()

	tb.tokens += (elapsed * tb.refillRate)

	if tb.tokens > float64(tb.capacity) {
		tb.tokens = float64(tb.capacity)
	}
	tb.lastRefill = now
}

func (tb *TokenBucket) Acquire(ctx context.Context) error {
	for {
		tb.mu.Lock()

		tb.refill()

		if tb.tokens >= 1 {
			tb.tokens -= 1
			tb.mu.Unlock()
			return nil
		}

		needed := 1 - tb.tokens
		waitTime := time.Duration((needed / tb.refillRate) * float64(time.Second))

		tb.mu.Unlock() // if not done the buckect will not be filled and no requests will be processed ever
		//select is the sleeping mechanism until one of its cases are satisfied
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitTime):
		}
	}

}
