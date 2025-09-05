package consumer

import (
    "encoding/json"
    "log"
    "os"
    "os/signal"
    "syscall"

    "dsp-statistics-go/internal/core"
    "dsp-statistics-go/internal/service"
    "dsp-statistics-go/internal/db"  // Важно: добавлен импорт пакета db

    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func StartConsumer() {
    // Инициализация базы данных
    db.InitDB()
    defer db.CloseDB()

    // Kafka конфигурация
    kafkaConfig := kafka.ConfigMap{
        "bootstrap.servers": os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
        "group.id":          os.Getenv("KAFKA_GROUP_ID"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "true",
        "auto.commit.interval.ms": "5000",
        "session.timeout.ms": "6000",
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
                    continue
                }

                if err := service.ProcessPayload(payload); err != nil {
                    log.Printf("Error processing payload: %s", err)
                } else {
                    if _, err := consumer.Commit(); err != nil {
                        log.Printf("Failed to commit offset: %s", err)
                    }
                }

            case kafka.Error:
                log.Printf("Kafka error: %v", e)
            default:
                // Игнорируем другие события
            }
        }
    }
}
