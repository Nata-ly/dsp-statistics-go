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

    // Количество воркеров можно задать через KAFKA_CONSUMER_WORKERS (по умолчанию 1)
    workerCount := 1
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

// runConsumerWorker создает отдельного Kafka-consumer и крутит его poll-цикл до остановки контекста.
func runConsumerWorker(ctx context.Context, workerID int, topic string) {
    kafkaConfig := kafka.ConfigMap{
        "bootstrap.servers":          os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
        "group.id":                   os.Getenv("KAFKA_GROUP_ID"),
        "auto.offset.reset":          "earliest",
        "enable.auto.commit":         "false", // ВАЖНО: отключаем автоматический коммит
        "enable.partition.eof":       "true",
        "session.timeout.ms":         "30000",
        "socket.timeout.ms":          "10000",
        "fetch.wait.max.ms":          "1000",
        "queued.min.messages":        "100000",
        "queued.max.messages.kbytes": "1000000",
        "max.poll.interval.ms":       "300000", // Увеличиваем интервал для обработки сообщений
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

    log.Printf("[worker %d] Consumer started. Listening to topic: %s", workerID, topic)

    for {
        select {
        case <-ctx.Done():
            log.Printf("[worker %d] Context cancelled, stopping consumer loop", workerID)
            // При завершении работы фиксируем текущее смещение
            if _, err := consumer.Commit(); err != nil {
                log.Printf("[worker %d] Failed to commit offset on shutdown: %s", workerID, err)
            }
            return
        default:
            ev := consumer.Poll(100)
            if ev == nil {
                continue
            }

            switch e := ev.(type) {
            case *kafka.Message:
                log.Printf("[worker %d] Received message from partition %d | offset %d",
                    workerID, e.TopicPartition.Partition, e.TopicPartition.Offset)

                var payload core.Payload
                if err := json.Unmarshal(e.Value, &payload); err != nil {
                    log.Printf("[worker %d] Error unmarshalling message: %s", workerID, err)
                    // Не фиксируем смещение при ошибке десериализации
                    continue
                }

                if err := service.ProcessPayload(payload); err != nil {
                    log.Printf("[worker %d] Error processing payload: %s", workerID, err)
                    // Не фиксируем смещение при ошибке обработки, чтобы повторить попытку
                    continue
                }

                // Фиксируем смещение ТОЛЬКО после успешной обработки
                if _, err := consumer.Commit(); err != nil {
                    log.Printf("[worker %d] Failed to commit offset: %s", workerID, err)
                    // Повторная попытка коммита
                    time.Sleep(100 * time.Millisecond)
                    if _, err := consumer.Commit(); err != nil {
                        log.Printf("[worker %d] Second attempt to commit offset failed: %s", workerID, err)
                    }
                }

            case kafka.Error:
                if e.Code() == kafka.ErrAllBrokersDown {
                    log.Printf("[worker %d] Kafka connection error: %v", workerID, e)
                    // Попробуем переподключиться через некоторое время
                    time.Sleep(5 * time.Second)
                    continue
                }
                log.Printf("[worker %d] Kafka error: %v", workerID, e)

            default:
                // Игнорируем другие события
            }
        }
    }
}
