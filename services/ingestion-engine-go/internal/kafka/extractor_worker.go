package kafka

import (
	"context"
	"encoding/json"
	"ingestion-engine-go/internal/extractor"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

type ExtractorWorker struct {
	Reader         *kafka.Reader
	TripleProducer *kafka.Writer
	Transformer    *extractor.Transformer
	Logger         *log.Logger
	WorkerCount    int
}

func (w *ExtractorWorker) Start(ctx context.Context) {
	workerCount := w.WorkerCount // how many background workers to run for processing messages in parallel

	msgChan := make(chan kafka.Message, workerCount*2) // creates a conveyer belt for messages to be processed by workers

	for i := 0; i < workerCount; i++ {
		go w.worker(ctx, i, msgChan) // creates 5 background workers to process messages in parallel, each worker will read from the msgChan and process messages as they come in
	}

	for {
		msg, err := w.Reader.FetchMessage(ctx)
		if err != nil {
			w.Logger.Printf("Error reading message: %v", err)
			continue
		}

		msgChan <- msg
	}
}

func (w *ExtractorWorker) worker(ctx context.Context, workerID int, msgChan <-chan kafka.Message) {
	for msg := range msgChan {
		start := time.Now()

		if err := w.processMessage(ctx, msg); err != nil {
			w.Logger.Printf("Worker %d: Error processing message: %v", workerID, err)
			continue
		}

		w.Logger.Printf("Worker %d: Processed message in %v", workerID, time.Since(start))

	}
}

func (w *ExtractorWorker) processMessage(ctx context.Context, msg kafka.Message) error {
	docID := string(msg.Key)
	text := string(msg.Value)

	chunks := extractor.ChuckText(docID, text, 1000, 0.1)

	pkg := &extractor.DocumentPackage{
		DocID:   docID,
		Chunks:  chunks,
		TIngest: time.Now().UTC(),
	}

	// parallel extraction of triples from chunks

	var mu sync.Mutex
	g, ctx := errgroup.WithContext(ctx)
	// LIMIT chunk concurrency to avoid overwhelming the system
	g.SetLimit(1) // 1 because it needs to be sequential as the next chunk has 10% of previous chunk, if we do in parallel then it will create duplicate triples

	for _, chunk := range chunks {
		chunk := chunk // capture range variable

		g.Go(func() error {
			res, err := w.Transformer.ExtractChunkOptimized(ctx, chunk)
			if err != nil {
				return err
			}

			mu.Lock()
			pkg.Nodes = append(pkg.Nodes, res.Nodes...)
			pkg.Relationships = append(pkg.Relationships, res.Relationships...)
			mu.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	//place it in topic 2
	val, err := json.Marshal(pkg)
	if err != nil {
		return err
	}

	err = w.TripleProducer.WriteMessages(ctx, kafka.Message{
		Key:   msg.Key,
		Value: val,
	})

	if err != nil {
		return nil
	}

	// OFFSET COMMIT
	return w.Reader.CommitMessages(ctx, msg)
}
