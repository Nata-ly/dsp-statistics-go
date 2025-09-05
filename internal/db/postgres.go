package db

import (
    "context"
    "log"
    "os"

    "github.com/jackc/pgx/v4/pgxpool"
)

var pool *pgxpool.Pool

func InitDB() {
    connectionString := os.Getenv("DATABASE_URL")
    var err error
    pool, err = pgxpool.Connect(context.Background(), connectionString)
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
