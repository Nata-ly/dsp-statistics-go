package consumer

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"dsp-statistics-go/internal/core"
	"dsp-statistics-go/internal/db"
	"dsp-statistics-go/internal/service"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/joho/godotenv"
)

const (
	defaultBatchSize    = 200
	defaultBatchTimeout = 500
	defaultConcurrency  = 20
)

// StartConsumer запускает несколько независимых Kafka-consumer'ов в одном consumer group.
// Kafka сам распределит партиции между инстансами, что увеличит общую скорость обработки.
func StartConsumer() {
	// Загрузка переменных окружения из .env файла
	if err := godotenv.Load(); err != nil {
		log.Println("Warning: Error loading .env file, using system environment variables")
	}

	// Проверка критических переменных окружения
	if os.Getenv("KAFKA_BOOTSTRAP_SERVERS") == "" {
		log.Fatal("KAFKA_BOOTSTRAP_SERVERS environment variable is not set")
	}
	if os.Getenv("KAFKA_TOPIC") == "" {
		log.Fatal("KAFKA_TOPIC environment variable is not set")
	}

	// Инициализация базы данных (один пул на все goroutine)
	db.InitDB()
	defer db.CloseDB()

	// Количество воркеров можно задать через KAFKA_CONSUMER_WORKERS (по умолчанию 4 для лучшей пропускной способности)
	workerCount := 4
	if v := os.Getenv("KAFKA_CONSUMER_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			workerCount = n
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Обработка сигналов завершения
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup
	topic := os.Getenv("KAFKA_TOPIC")

	log.Printf("Starting %d Kafka consumer worker(s) for topic %s", workerCount, topic)

	// In-memory stats buffer flusher (periodic + final flush on shutdown)
	go service.RunStatsBufferFlusher(ctx)

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runConsumerWorker(ctx, id, topic)
		}(i + 1)
	}

	// Ждем сигнала завершения
	sig := <-sigchan
	log.Printf("Caught signal %v: shutting down consumers", sig)
	cancel()

	// Ждем корректного завершения всех воркеров
	wg.Wait()
	log.Println("All Kafka consumer workers stopped")
}

// runConsumerWorker создает отдельного Kafka-consumer, читает пачками, обрабатывает параллельно, коммитит по пачке.
func runConsumerWorker(ctx context.Context, workerID int, topic string) {
	batchSize := defaultBatchSize
	if v := os.Getenv("KAFKA_BATCH_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			batchSize = n
		}
	}
	batchTimeoutMs := defaultBatchTimeout
	if v := os.Getenv("KAFKA_BATCH_TIMEOUT_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			batchTimeoutMs = n
		}
	}
	concurrency := defaultConcurrency
	if v := os.Getenv("KAFKA_PROCESS_CONCURRENCY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			concurrency = n
		}
	}

	kafkaConfig := kafka.ConfigMap{
		"bootstrap.servers":          os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
		"group.id":                   os.Getenv("KAFKA_GROUP_ID"),
		"auto.offset.reset":          "earliest",
		"enable.auto.commit":         "false",
		"enable.auto.offset.store":   "false",
		"enable.partition.eof":       "true",
		"session.timeout.ms":         "30000",
		"socket.timeout.ms":          "10000",
		"fetch.wait.max.ms":          "1000",
		"queued.min.messages":        "100000",
		"queued.max.messages.kbytes": "1000000",
		"max.poll.interval.ms":       "300000",
	}

	consumer, err := kafka.NewConsumer(&kafkaConfig)
	if err != nil {
		log.Printf("[worker %d] Failed to create consumer: %s", workerID, err)
		return
	}
	defer consumer.Close()

	if err := consumer.SubscribeTopics([]string{topic}, nil); err != nil {
		log.Printf("[worker %d] Failed to subscribe to topic: %s", workerID, err)
		return
	}

	log.Printf("[worker %d] Consumer started (batch=%d, concurrency=%d)", workerID, batchSize, concurrency)

	sem := make(chan struct{}, concurrency)
	batchTimeout := time.Duration(batchTimeoutMs) * time.Millisecond
	pollMs := 50

	for {
		select {
		case <-ctx.Done():
			log.Printf("[worker %d] Context cancelled, stopping consumer loop", workerID)
			if _, err := consumer.Commit(); err != nil {
				log.Printf("[worker %d] Failed to commit offset on shutdown: %s", workerID, err)
			}
			return
		default:
			// Collect a batch of messages
			batch := collectBatch(consumer, batchSize, batchTimeout, pollMs)
			if len(batch) == 0 {
				continue
			}

			// Process batch in parallel with semaphore
			var wg sync.WaitGroup
			var errMu sync.Mutex
			var firstErr error
			for i := range batch {
				msg := batch[i]
				wg.Add(1)
				sem <- struct{}{}
				go func(m *kafka.Message) {
					defer wg.Done()
					defer func() { <-sem }()
					var payload core.Payload
					if err := json.Unmarshal(m.Value, &payload); err != nil {
						errMu.Lock()
						if firstErr == nil {
							firstErr = err
						}
						errMu.Unlock()
						log.Printf("[worker %d] Error unmarshalling message: %s", workerID, err)
						return
					}
					if err := service.ProcessPayload(payload); err != nil {
						errMu.Lock()
						if firstErr == nil {
							firstErr = err
						}
						errMu.Unlock()
						log.Printf("[worker %d] Error processing payload: %s", workerID, err)
						return
					}
				}(msg)
			}
			wg.Wait()

			if firstErr != nil {
				// Do not commit; messages will be redelivered
				continue
			}

			// Commit last offset per partition in the batch (next offset to fetch)
			toCommit := lastOffsetsPerPartition(batch, topic)
			if len(toCommit) > 0 {
				if _, err := consumer.Commit(toCommit); err != nil {
					log.Printf("[worker %d] Failed to commit batch offset: %s", workerID, err)
					time.Sleep(100 * time.Millisecond)
					if _, err := consumer.Commit(toCommit); err != nil {
						log.Printf("[worker %d] Second attempt to commit batch offset failed: %s", workerID, err)
					}
				}
			}
		}
	}
}

// collectBatch polls until batch is full or timeout elapsed.
func collectBatch(consumer *kafka.Consumer, maxSize int, timeout time.Duration, pollMs int) []*kafka.Message {
	var batch []*kafka.Message
	deadline := time.Now().Add(timeout)
	for len(batch) < maxSize && time.Now().Before(deadline) {
		ev := consumer.Poll(pollMs)
		if ev == nil {
			if len(batch) > 0 {
				break
			}
			continue
		}
		switch e := ev.(type) {
		case *kafka.Message:
			batch = append(batch, e)
		case kafka.Error:
			if e.Code() == kafka.ErrAllBrokersDown {
				log.Printf("Kafka connection error: %v", e)
				time.Sleep(5 * time.Second)
				continue
			}
		default:
			// ignore other events
		}
	}
	return batch
}

// lastOffsetsPerPartition returns one TopicPartition per partition with Offset set to the next offset to fetch.
func lastOffsetsPerPartition(messages []*kafka.Message, topic string) []kafka.TopicPartition {
	maxOffset := make(map[int32]int64)
	for _, m := range messages {
		p := m.TopicPartition.Partition
		o := m.TopicPartition.Offset.Int64()
		if o+1 > maxOffset[p] {
			maxOffset[p] = o + 1
		}
	}
	out := make([]kafka.TopicPartition, 0, len(maxOffset))
	for p, next := range maxOffset {
		out = append(out, kafka.TopicPartition{
			Topic:     &topic,
			Partition: p,
			Offset:    kafka.Offset(next),
		})
	}
	return out
}
