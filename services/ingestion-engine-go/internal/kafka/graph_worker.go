package kafka

import (
	"context"
	"encoding/json"
	"ingestion-engine-go/internal/extractor"
	"ingestion-engine-go/internal/graph"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type GraphWorker struct {
	Reader      *kafka.Reader
	Logger      *log.Logger
	GraphWriter *graph.Writer
	WorkerCount int
}

func (w *GraphWorker) Start(ctx context.Context) {
	msgChan := make(chan kafka.Message, w.WorkerCount*2)
	var wg sync.WaitGroup

	for i := 0; i < w.WorkerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			w.worker(ctx, workerID, msgChan)
		}(i)
	}

	for {
		val, err := w.Reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break // Graceful shutdown
			}
			w.Logger.Printf("Graph Fetch error: %v", err)
			break
		}
		msgChan <- val
	}
	close(msgChan)
	wg.Wait()
}

func (w *GraphWorker) worker(ctx context.Context, workerID int, msgchan <-chan kafka.Message) {

	for msg := range msgchan {
		start := time.Now()

		if err := w.process(ctx, msg); err != nil {
			w.Logger.Printf(
				"worker=%d failed doc=%s err=%v",
				workerID,
				string(msg.Key),
				err,
			)
			continue
		}

		w.Logger.Printf(
			"worker=%d completed doc=%s duration=%s",
			workerID,
			string(msg.Key),
			time.Since(start),
		)
	}
}

func (w *GraphWorker) process(ctx context.Context, msg kafka.Message) error {

	//deserialize the message value into a graph message struct
	var pkg extractor.DocumentPackage

	if err := json.Unmarshal(msg.Value, &pkg); err != nil {
		return err
	}

	// write to Neo4j (atomic transaction)
	if err := w.GraphWriter.WriteDocumentPackage(ctx, &pkg); err != nil {
		if w.Logger != nil {
			w.Logger.Printf("Error writing document package doc=%s err=%v", pkg.DocID, err)
			return err
		}
	}

	//commit the message offset after successful processing
	return w.Reader.CommitMessages(ctx, msg)
}
