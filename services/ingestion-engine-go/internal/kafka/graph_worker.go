package kafka

import (
	"context"
	"encoding/json"
	"ingestion-engine-go/internal/extractor"
	"ingestion-engine-go/internal/graph"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type GraphWorker struct {
	Reader      *kafka.Reader
	Writer      *kafka.Writer
	Logger      *log.Logger
	GraphWriter *graph.Writer
	workerCount int
}

func (w *GraphWorker) start(ctx context.Context) {

	msgchan := make(chan kafka.Message, w.workerCount*2)

	for i := 0; i < w.workerCount; i++ {
		go w.worker(ctx, i, msgchan)
	}

	for {
		val, err := w.Reader.FetchMessage(ctx)
		if err != nil {
			w.Logger.Printf("Error reading message: %v", err)
			continue
		}
		msgchan <- val
	}
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
	if err := w.GraphWriter.WriteDocumentPackage(ctx, pkg); err != nil {
		if w.Logger != nil {
			w.Logger.Printf("Error writing document package doc=%s err=%v", pkg.DocID, err)
		}
	}

	//commit the message offset after successful processing
	return w.Reader.CommitMessages(ctx, msg)
}
