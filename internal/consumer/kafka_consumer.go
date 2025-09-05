package consumer

import (
    "encoding/json"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"

    "dsp-statistics-go/internal/core"
    "dsp-statistics-go/internal/db"
    "dsp-statistics-go/internal/service"

    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
    "github.com/joho/godotenv"
)

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

    // Инициализация базы данных
    db.InitDB()
    defer db.CloseDB()

    // Kafka конфигурация
    kafkaConfig := kafka.ConfigMap{
        "bootstrap.servers":        os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
        "group.id":                 os.Getenv("KAFKA_GROUP_ID"),
        "auto.offset.reset":        "earliest",
        "enable.auto.commit":       "false",  // ВАЖНО: отключаем автоматический коммит
        "enable.partition.eof":     "true",
        "session.timeout.ms":       "30000",
        "socket.timeout.ms":        "10000",
        "fetch.wait.max.ms":        "1000",
        "queued.min.messages":      "100000",
        "queued.max.messages.kbytes": "1000000",
        "max.poll.interval.ms":     "300000", // Увеличиваем интервал для обработки сообщений
    }

    // Создание потребителя
    consumer, err := kafka.NewConsumer(&kafkaConfig)
    if err != nil {
        log.Fatalf("Failed to create consumer: %s", err)
    }
    defer consumer.Close()

    // Подписка на топик
    topic := os.Getenv("KAFKA_TOPIC")
    err = consumer.SubscribeTopics([]string{topic}, nil)
    if err != nil {
        log.Fatalf("Failed to subscribe to topic: %s", err)
    }

    // Обработка сигналов завершения
    sigchan := make(chan os.Signal, 1)
    signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

    log.Printf("Consumer started. Listening to topic: %s", topic)

    run := true
    for run {
        select {
        case sig := <-sigchan:
            log.Printf("Caught signal %v: terminating", sig)
            run = false
        default:
            ev := consumer.Poll(100)
            if ev == nil {
                continue
            }

            switch e := ev.(type) {
            case *kafka.Message:
                log.Printf("Received message from partition %d | offset %d",
                    e.TopicPartition.Partition, e.TopicPartition.Offset)

                var payload core.Payload
                if err := json.Unmarshal(e.Value, &payload); err != nil {
                    log.Printf("Error unmarshalling message: %s", err)
                    // Не фиксируем смещение при ошибке десериализации
                    continue
                }

                if err := service.ProcessPayload(payload); err != nil {
                    log.Printf("Error processing payload: %s", err)
                    // Не фиксируем смещение при ошибке обработки, чтобы повторить попытку
                    continue
                } else {
                    // Фиксируем смещение ТОЛЬКО после успешной обработки
                    if _, err := consumer.Commit(); err != nil {
                        log.Printf("Failed to commit offset: %s", err)
                        // Повторная попытка коммита
                        time.Sleep(100 * time.Millisecond)
                        if _, err := consumer.Commit(); err != nil {
                            log.Printf("Second attempt to commit offset failed: %s", err)
                        }
                    }
                }

            case kafka.Error:
                if e.Code() == kafka.ErrAllBrokersDown {
                    log.Printf("Kafka connection error: %v", e)
                    // Попробуем переподключиться через некоторое время
                    time.Sleep(5 * time.Second)
                    continue
                }
                log.Printf("Kafka error: %v", e)

            default:
                // Игнорируем другие события
            }
        }
    }

    // При завершении работы фиксируем текущее смещение
    if _, err := consumer.Commit(); err != nil {
        log.Printf("Failed to commit offset on shutdown: %s", err)
    }
}
