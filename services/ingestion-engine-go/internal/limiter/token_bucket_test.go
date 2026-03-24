package limiter_test

import (
	"context"
	"fmt"
	"ingestion-engine-go/internal/limiter"
	"testing"
	"time"
)

func TestTokenBucketThroughput(t *testing.T) {
	fmt.Println("Starting Token Bucket Throughput Test...")

	// capacity = 10 tokens
	// refill rate = 2 tokens per second
	lim := limiter.NewTokenBucket(10, 2)

	ctx := context.Background()
	start := time.Now()

	for i := 0; i < 20; i++ {
		err := lim.Acquire(ctx)
		if err != nil {
			panic(err)
		}

		elapsed := time.Since(start).Seconds()
		fmt.Printf("Request %02d at %.2fs\n", i, elapsed)
	}
}
