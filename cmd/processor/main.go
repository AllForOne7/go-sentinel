package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"sentinel/internal/logger"
	"sentinel/internal/models"
	"sentinel/internal/storage"

	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"
)

func main() {
	godotenv.Load()

	// 1. Инициализация логов
	logCloser := initProcessorLogs()
	defer logCloser.Close()

	// 2. Инициализация БД
	db := initDatabase()
	defer db.Close()

	// 3. Подключение к NATS
	nc := connectNats()
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		slog.Error("ошибка JetStream", "err", err)
		return
	}

	setupStreams(js)

	// 4. Запуск подписок
	subMetrics := subscribeToMetrics(js, db)
	subSpeedtest := subscribeToSpeedtest(js, db)

	slog.Info("processor запущен и слушает события...")

	// 5. Ожидание завершения
	handleGracefulShutdown(subMetrics, subSpeedtest)
}

// --- Вспомогательные функции ---

func initProcessorLogs() (ioCloser interface{ Close() error }) {
	logDir := os.Getenv("LOG_DIR")
	if logDir == "" {
		logDir = "logs"
	}
	os.MkdirAll(logDir, 0755)

	logFile, err := logger.Init(filepath.Join(logDir, "processor.log"))
	if err != nil {
		slog.Error("ошибка инициализации логов", "err", err)
		os.Exit(1)
	}
	return logFile
}

func initDatabase() storage.Storage {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("DB_PATH")
		if dbURL == "" {
			dbURL = "metrics.db"
		}
	}

	db, err := storage.NewStorage(context.Background(), dbURL)
	if err != nil {
		slog.Error("ошибка БД", "err", err)
		os.Exit(1)
	}

	if err := db.InitSpeedtest(context.Background()); err != nil {
		slog.Error("ошибка инициализации speedtest", "err", err)
	}
	return db
}

func connectNats() *nats.Conn {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	nc, err := nats.Connect(url,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(5*time.Second),
	)
	if err != nil {
		slog.Error("ошибка подключения к NATS", "url", url, "err", err)
		os.Exit(1)
	}
	return nc
}

func setupStreams(js nats.JetStreamContext) {
	js.AddStream(&nats.StreamConfig{
		Name:     "METRICS",
		Subjects: []string{"metrics.>"},
		MaxAge:   24 * time.Hour,
	})
	js.AddStream(&nats.StreamConfig{
		Name:     "SPEEDTEST",
		Subjects: []string{"speedtest.>"},
		MaxAge:   7 * 24 * time.Hour,
	})
}

func subscribeToMetrics(js nats.JetStreamContext, db storage.Storage) *nats.Subscription {
	sub, err := js.Subscribe("metrics.>", func(msg *nats.Msg) {
		var e models.MetricEvent
		if err := json.Unmarshal(msg.Data, &e); err != nil {
			slog.Error("ошибка парсинга метрики", "err", err)
			msg.Ack()
			return
		}

		if err := db.Save(context.Background(), e); err != nil {
			slog.Error("ошибка сохранения метрики", "host", e.Host, "err", err)
		} else {
			slog.Info("метрика сохранена", "host", e.Host, "cpu", math.Round(e.CPU*10)/10)
		}
		msg.Ack()
	}, nats.Durable("processor"), nats.DeliverNew())

	if err != nil {
		slog.Error("ошибка подписки на метрики", "err", err)
		os.Exit(1)
	}
	return sub
}

func subscribeToSpeedtest(js nats.JetStreamContext, db storage.Storage) *nats.Subscription {
	sub, err := js.Subscribe("speedtest.>", func(msg *nats.Msg) {
		var r models.SpeedtestResult
		if err := json.Unmarshal(msg.Data, &r); err != nil {
			slog.Error("ошибка парсинга speedtest", "err", err)
			msg.Ack()
			return
		}

		if err := db.SaveSpeedtest(context.Background(), r); err != nil {
			slog.Error("ошибка сохранения speedtest", "host", r.Host, "err", err)
		} else {
			slog.Info("speedtest сохранён", "host", r.Host, "down", r.DownloadMbps)
		}
		msg.Ack()
	}, nats.Durable("processor-speedtest"), nats.DeliverNew())

	if err != nil {
		slog.Error("ошибка подписки на speedtest", "err", err)
		os.Exit(1)
	}
	return sub
}

func handleGracefulShutdown(subs ...*nats.Subscription) {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("processor останавливается...")
	for _, sub := range subs {
		sub.Unsubscribe()
	}
	slog.Info("processor остановлен")
}
