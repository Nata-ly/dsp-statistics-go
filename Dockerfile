# Stage 1: Сборка
FROM golang:1.21 AS builder

WORKDIR /app

# Копируем зависимости
COPY go.mod go.sum ./
RUN go mod download

# Копируем и компилируем код
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o dsp-statistics ./cmd/server

# Stage 2: Минимальный образ
FROM alpine:latest

WORKDIR /root/

# Устанавливаем необходимые зависимости
RUN apk --no-cache add ca-certificates tzdata

# Копируем бинарник
COPY --from=builder /app/dsp-statistics .

# Запускаем приложение
CMD ["./dsp-statistics"]
