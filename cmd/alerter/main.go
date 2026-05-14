package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"sentinel/internal/logger"
	"sentinel/internal/models"
	"sentinel/internal/storage"

	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"
)

type alertState struct {
	cpuHighCount   int
	cpuAlerted     bool
	cpuIncidentID  int64
	ramAlerted     bool
	ramIncidentID  int64
	diskAlerted    bool
	diskIncidentID int64
}

var (
	states   = map[string]*alertState{}
	statesMu sync.Mutex
)

func sendTelegram(token, chatID, text string) {
	apiURL := "https://api.telegram.org/bot" + token + "/sendMessage"
	resp, err := http.PostForm(apiURL, url.Values{
		"chat_id":    {chatID},
		"text":       {text},
		"parse_mode": {"HTML"},
	})
	if err != nil {
		slog.Error("ошибка отправки telegram", "err", err)
		return
	}
	defer resp.Body.Close()
	slog.Info("telegram отправлено")
}

func getRuleThreshold(rules []models.Rule, metric string) (threshold float64, count int, enabled bool) {
	for _, r := range rules {
		if r.Metric == metric {
			return r.Threshold, r.Count, r.Enabled
		}
	}
	switch metric {
	case "cpu":
		return 60, 3, true
	case "ram":
		return 80, 1, true
	case "disk":
		return 85, 1, true
	}
	return 100, 1, false
}

func fmtFloat(f float64) string {
	return fmt.Sprintf("%.1f", f)
}

func check(e models.MetricEvent, token, chatID string, rules []models.Rule, db storage.Storage) {
	statesMu.Lock()
	defer statesMu.Unlock()

	if states[e.Host] == nil {
		states[e.Host] = &alertState{}
	}
	s := states[e.Host]

	// CPU
	cpuThreshold, cpuCount, cpuEnabled := getRuleThreshold(rules, "cpu")
	if cpuEnabled {
		if e.CPU > cpuThreshold {
			s.cpuHighCount++
			slog.Warn("cpu высокий",
				"host", e.Host,
				"cpu", e.CPU,
				"threshold", cpuThreshold,
				"count", s.cpuHighCount,
				"need", cpuCount,
			)
			if s.cpuHighCount >= cpuCount && !s.cpuAlerted {
				sendTelegram(token, chatID, "🔴 <b>CPU перегрев!</b>\nХост: <code>"+e.Host+"</code>\n"+
					"CPU: <b>"+fmtFloat(e.CPU)+"%</b>  RAM: "+fmtFloat(e.RAMUsed)+"%\n"+
					"Время: "+e.Time.Format("15:04:05"))
				s.cpuAlerted = true
				id, err := db.OpenIncident(context.Background(), e.Host, "cpu", e.CPU, cpuThreshold, e.Time)
				if err != nil {
					slog.Error("ошибка создания инцидента cpu", "err", err)
				} else {
					s.cpuIncidentID = id
					slog.Info("инцидент открыт", "id", id, "host", e.Host, "metric", "cpu")
				}
			}
		} else {
			if s.cpuAlerted {
				sendTelegram(token, chatID, "✅ <b>CPU в норме</b>\nХост: <code>"+e.Host+"</code>\n"+
					"CPU: <b>"+fmtFloat(e.CPU)+"%</b>")
				if err := db.CloseIncident(context.Background(), e.Host, "cpu", time.Now()); err != nil {
					slog.Error("ошибка закрытия инцидента cpu", "err", err)
				} else {
					slog.Info("инцидент закрыт", "id", s.cpuIncidentID, "host", e.Host, "metric", "cpu")
				}
			}
			s.cpuHighCount = 0
			s.cpuAlerted = false
			s.cpuIncidentID = 0
		}
	}

	// RAM
	ramThreshold, _, ramEnabled := getRuleThreshold(rules, "ram")
	if ramEnabled {
		if e.RAMUsed > ramThreshold {
			if !s.ramAlerted {
				sendTelegram(token, chatID, "🟡 <b>Мало памяти!</b>\nХост: <code>"+e.Host+"</code>\n"+
					"RAM: <b>"+fmtFloat(e.RAMUsed)+"%</b> (порог "+fmtFloat(ramThreshold)+"%)\n"+
					"Свободно: "+fmtFloat(e.RAMFreeGB)+" GB\n"+
					"Время: "+e.Time.Format("15:04:05"))
				s.ramAlerted = true
				id, err := db.OpenIncident(context.Background(), e.Host, "ram", e.RAMUsed, ramThreshold, e.Time)
				if err != nil {
					slog.Error("ошибка создания инцидента ram", "err", err)
				} else {
					s.ramIncidentID = id
					slog.Info("инцидент открыт", "id", id, "host", e.Host, "metric", "ram")
				}
			}
		} else {
			if s.ramAlerted {
				sendTelegram(token, chatID, "✅ <b>RAM в норме</b>\nХост: <code>"+e.Host+"</code>\n"+
					"RAM: <b>"+fmtFloat(e.RAMUsed)+"%</b>")
				if err := db.CloseIncident(context.Background(), e.Host, "ram", time.Now()); err != nil {
					slog.Error("ошибка закрытия инцидента ram", "err", err)
				} else {
					slog.Info("инцидент закрыт", "id", s.ramIncidentID, "host", e.Host, "metric", "ram")
				}
			}
			s.ramAlerted = false
			s.ramIncidentID = 0
		}
	}

	// Диск
	diskThreshold, _, diskEnabled := getRuleThreshold(rules, "disk")
	if diskEnabled {
		if e.DiskUsed > diskThreshold {
			if !s.diskAlerted {
				sendTelegram(token, chatID, "🟠 <b>Мало места на диске!</b>\nХост: <code>"+e.Host+"</code>\n"+
					"Диск: <b>"+fmtFloat(e.DiskUsed)+"%</b> (порог "+fmtFloat(diskThreshold)+"%)\n"+
					"Свободно: "+fmtFloat(e.DiskFreeGB)+" GB\n"+
					"Время: "+e.Time.Format("15:04:05"))
				s.diskAlerted = true
				id, err := db.OpenIncident(context.Background(), e.Host, "disk", e.DiskUsed, diskThreshold, e.Time)
				if err != nil {
					slog.Error("ошибка создания инцидента disk", "err", err)
				} else {
					s.diskIncidentID = id
					slog.Info("инцидент открыт", "id", id, "host", e.Host, "metric", "disk")
				}
			}
		} else {
			if s.diskAlerted {
				sendTelegram(token, chatID, "✅ <b>Диск в норме</b>\nХост: <code>"+e.Host+"</code>\n"+
					"Диск: <b>"+fmtFloat(e.DiskUsed)+"%</b>")
				if err := db.CloseIncident(context.Background(), e.Host, "disk", time.Now()); err != nil {
					slog.Error("ошибка закрытия инцидента disk", "err", err)
				} else {
					slog.Info("инцидент закрыт", "id", s.diskIncidentID, "host", e.Host, "metric", "disk")
				}
			}
			s.diskAlerted = false
			s.diskIncidentID = 0
		}
	}
}

func main() {
	godotenv.Load()

	logDir := os.Getenv("LOG_DIR")
	if logDir == "" {
		logDir = "logs"
	}
	os.MkdirAll(logDir, 0755)

	logFile, err := logger.Init(filepath.Join(logDir, "alerter.log"))
	if err != nil {
		slog.Error("ошибка инициализации логов", "err", err)
		return
	} else {
		defer logFile.Close()
	}

	token := os.Getenv("TELEGRAM_TOKEN")
	if token == "" {
		slog.Error("TELEGRAM_TOKEN не задан в .env")
		return
	}

	chatID := os.Getenv("TELEGRAM_CHAT_ID")
	if chatID == "" {
		slog.Error("TELEGRAM_CHAT_ID не задан в .env")
		return
	}

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "metrics.db"
	}
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = dbPath
	}
	db, err := storage.NewStorage(context.Background(), dbURL)
	if err != nil {
		slog.Error("ошибка БД", "err", err)
		return
	}
	defer db.Close()

	if err := db.InitRules(context.Background()); err != nil {
		slog.Error("ошибка инициализации правил", "err", err)
		return
	}

	if err := db.InitIncidents(context.Background()); err != nil {
		slog.Error("ошибка инициализации инцидентов", "err", err)
		return
	}

	// Восстанавливаем состояние открытых инцидентов после перезапуска
	if openIncidents, err := db.GetOpenIncidents(context.Background()); err == nil {
		statesMu.Lock()
		for _, inc := range openIncidents {
			if states[inc.Host] == nil {
				states[inc.Host] = &alertState{}
			}
			s := states[inc.Host]
			switch inc.Metric {
			case "cpu":
				s.cpuAlerted = true
				s.cpuIncidentID = inc.ID
			case "ram":
				s.ramAlerted = true
				s.ramIncidentID = inc.ID
			case "disk":
				s.diskAlerted = true
				s.diskIncidentID = inc.ID
			}
		}
		statesMu.Unlock()
		slog.Info("восстановлено открытых инцидентов", "count", len(openIncidents))
	}

	var (
		rules   []models.Rule
		rulesMu sync.RWMutex
	)

	loadRules := func() {
		r, err := db.GetRules(context.Background())
		if err != nil {
			slog.Error("ошибка загрузки правил", "err", err)
			return
		}
		rulesMu.Lock()
		rules = r
		rulesMu.Unlock()
		slog.Info("правила загружены", "count", len(r))
		for _, rule := range r {
			status := "включено"
			if !rule.Enabled {
				status = "выключено"
			}
			slog.Info("правило",
				"metric", rule.Metric,
				"threshold", rule.Threshold,
				"count", rule.Count,
				"status", status,
			)
		}
	}
	loadRules()

	go func() {
		for range time.Tick(10 * time.Second) {
			loadRules()
		}
	}()

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
		slog.Error("ошибка подключения к NATS", "url", natsURL, "err", err)
		return
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		slog.Error("ошибка JetStream", "err", err)
		return
	}

	// Создаём стрим если не существует
	js.AddStream(&nats.StreamConfig{
		Name:     "METRICS",
		Subjects: []string{"metrics.>"},
		MaxAge:   24 * time.Hour,
	})

	slog.Info("alerter запущен",
		"nats", natsURL,
		"db", dbURL,
		"rules_refresh", "10s",
	)

	sub, err := js.Subscribe("metrics.>", func(msg *nats.Msg) {
		var e models.MetricEvent
		if err := json.Unmarshal(msg.Data, &e); err != nil {
			slog.Error("ошибка парсинга метрики", "err", err)
			msg.Ack()
			return
		}
		slog.Info("метрика получена",
			"host", e.Host,
			"cpu", math.Round(e.CPU*10)/10,
			"ram", math.Round(e.RAMUsed*10)/10,
			"disk", math.Round(e.DiskUsed*10)/10,
		)
		rulesMu.RLock()
		r := make([]models.Rule, len(rules))
		copy(r, rules)
		rulesMu.RUnlock()
		check(e, token, chatID, r, db)
		msg.Ack()
	}, nats.Durable("alerter"), nats.DeliverNew())
	if err != nil {
		// Если consumer уже существует с другим конфигом — удаляем и создаём заново
		if strings.Contains(err.Error(), "consumer name already in use") || strings.Contains(err.Error(), "already exists") {
			slog.Warn("consumer 'alerter' уже существует, удаляем и пересоздаём...")
			_ = js.DeleteConsumer("METRICS", "alerter")
			sub, err = js.Subscribe("metrics.>", func(msg *nats.Msg) {
				var e models.MetricEvent
				if err := json.Unmarshal(msg.Data, &e); err != nil {
					slog.Error("ошибка парсинга метрики", "err", err)
					msg.Ack()
					return
				}
				slog.Info("метрика получена",
					"host", e.Host,
					"cpu", math.Round(e.CPU*10)/10,
					"ram", math.Round(e.RAMUsed*10)/10,
					"disk", math.Round(e.DiskUsed*10)/10,
				)
				rulesMu.RLock()
				r := make([]models.Rule, len(rules))
				copy(r, rules)
				rulesMu.RUnlock()
				check(e, token, chatID, r, db)
				msg.Ack()
			}, nats.Durable("alerter"), nats.DeliverNew())
			if err != nil {
				slog.Error("не удалось создать подписку после пересоздания", "err", err)
				os.Exit(1)
			}
			slog.Info("подписка alerter пересоздана")
		} else {
			slog.Error("ошибка подписки на metrics", "err", err)
			os.Exit(1)
		}
	}
	slog.Info("подписка на метрики создана")

	// Ожидаем сигнал завершения
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	slog.Info("alerter останавливается...")
	sub.Unsubscribe()
	slog.Info("alerter остановлен")
}