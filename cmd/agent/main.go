package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"sentinel/internal/collector"
	"sentinel/internal/logger"
	"sentinel/internal/models"

	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"
)

func main() {
	godotenv.Load()

	logDir := os.Getenv("LOG_DIR")
	if logDir == "" {
		logDir = "logs"
	}
	os.MkdirAll(logDir, 0755)

	logFile, err := logger.Init(filepath.Join(logDir, "agent.log"))
	if err != nil {
		slog.Error("ошибка инициализации логов", "err", err)
	} else {
		defer logFile.Close()
	}

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	hostName := os.Getenv("HOST_NAME")
	if hostName == "" {
		hostName = "my-pc"
	}

	speedtestInterval := 30 * time.Minute
	if v := os.Getenv("SPEEDTEST_INTERVAL_MIN"); v != "" {
		if mins, err := time.ParseDuration(v + "m"); err == nil {
			speedtestInterval = mins
		}
	}

	nc, err := nats.Connect(natsURL,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(5*time.Second),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			slog.Warn("NATS отключился", "err", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			slog.Info("NATS переподключился", "url", nc.ConnectedUrl())
		}),
	)
	if err != nil {
		slog.Error("не могу подключиться к NATS", "url", natsURL, "err", err)
		return
	}
	defer nc.Close()
	slog.Info("подключился к NATS", "url", natsURL)

	js, err := nc.JetStream()
	if err != nil {
		slog.Error("ошибка JetStream", "err", err)
		return
	}

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

	// Парсим порты для мониторинга из .env
	portConfigs := collector.ParsePortsConfig(os.Getenv("MONITOR_PORTS"))
	if len(portConfigs) > 0 {
		slog.Info("мониторинг портов включён", "ports", len(portConfigs))
	}

	// Собираем системную информацию при старте
	sysInfo := collector.CollectSystemInfo()
	slog.Info("системная информация",
		"os", sysInfo.OS,
		"cpu", sysInfo.CPUModel,
		"cores", sysInfo.CPUCores,
		"ram_gb", sysInfo.TotalRAM,
	)

	slog.Info("агент запущен", "host", hostName, "speedtest_interval", speedtestInterval)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Счётчики для редких операций
	var tickCount int
	var lastPorts []models.PortStatus

	// Speedtest горутина
	go func() {
		time.Sleep(1 * time.Minute)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			slog.Info("запускаю speedtest")
			result, err := collector.RunSpeedtest(hostName)
			if err != nil {
				slog.Error("ошибка speedtest", "err", err)
			} else {
				data, _ := json.Marshal(result)
				if _, err := js.Publish("speedtest."+hostName, data); err != nil {
					slog.Error("ошибка публикации speedtest", "err", err)
				} else {
					slog.Info("speedtest готов",
						"host", hostName,
						"download_mbps", result.DownloadMbps,
						"upload_mbps", result.UploadMbps,
						"ping_ms", result.PingMs,
						"server", result.Server,
					)
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(speedtestInterval):
			}
		}
	}()

	// Основной цикл сбора метрик
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			event := collector.Collect(hostName)

			// SysInfo: при старте и каждые 60 тиков (~5 мин)
			if tickCount == 0 || tickCount%60 == 0 {
				collector.UpdateUptime(sysInfo)
				event.SysInfo = sysInfo
			}

			// Порты: каждые 6 тиков (~30 сек)
			if len(portConfigs) > 0 && tickCount%6 == 0 {
				lastPorts = collector.CheckPorts(portConfigs)
			}
			event.Ports = lastPorts

			tickCount++

			data, _ := json.Marshal(event)
			if _, err := js.Publish("metrics."+hostName, data); err != nil {
				slog.Error("ошибка публикации метрик", "err", err)
			} else {
				slog.Info("метрики отправлены",
					"host", hostName,
					"cpu", event.CPU,
					"ram", event.RAMUsed,
					"net_recv", event.NetRecvMBps,
					"net_sent", event.NetSentMBps,
					"procs", len(event.TopProcesses),
				)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
		}
	}()

	// Ожидаем сигнал завершения
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	slog.Info("агент останавливается...")
	cancel()
	time.Sleep(500 * time.Millisecond) // даём горутинам завершиться
	slog.Info("агент остановлен")
}
