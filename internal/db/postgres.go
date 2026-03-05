package db

import (
    "context"
    "log"
    "os"
    "strconv"

    "github.com/jackc/pgx/v4/pgxpool"
)

var pool *pgxpool.Pool

func InitDB() {
    connectionString := os.Getenv("DATABASE_URL")
    if connectionString == "" {
        log.Fatal("DATABASE_URL environment variable is not set")
    }
    config, err := pgxpool.ParseConfig(connectionString)
    if err != nil {
        log.Fatalf("Unable to parse database config: %v\n", err)
    }
    if v := os.Getenv("DB_MAX_CONNS"); v != "" {
        if n, err := strconv.Atoi(v); err == nil && n > 0 {
            config.MaxConns = int32(n)
        }
    }
    if v := os.Getenv("DB_MIN_CONNS"); v != "" {
        if n, err := strconv.Atoi(v); err == nil && n >= 0 {
            config.MinConns = int32(n)
        }
    }
    pool, err = pgxpool.ConnectConfig(context.Background(), config)
    if err != nil {
        log.Fatalf("Unable to connect to database: %v\n", err)
    }
}

func GetDB() *pgxpool.Pool {
    return pool
}

func CloseDB() {
    if pool != nil {
        pool.Close()
    }
}
