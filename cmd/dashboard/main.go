package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"html/template"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"sentinel/internal/hostcache"
	"sentinel/internal/logger"
	"sentinel/internal/models"
	"sentinel/internal/storage"

	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

//go:embed templates/index.html templates/rules.html templates/login.html templates/incidents.html
var templateFS embed.FS

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type hub struct {
	mu      sync.Mutex
	clients map[*websocket.Conn]bool
}

func (h *hub) add(c *websocket.Conn) {
	h.mu.Lock()
	h.clients[c] = true
	h.mu.Unlock()
}

func (h *hub) remove(c *websocket.Conn) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
}

func (h *hub) broadcast(data []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.clients {
		c.WriteMessage(websocket.TextMessage, data)
	}
}

var clients = &hub{clients: make(map[*websocket.Conn]bool)}

var store = hostcache.New()

// Prometheus метрики
var (
	promCPU = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sentinel_cpu_percent",
		Help: "CPU usage percent per host",
	}, []string{"host"})

	promRAM = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sentinel_ram_percent",
		Help: "RAM usage percent per host",
	}, []string{"host"})

	promDisk = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sentinel_disk_percent",
		Help: "Disk usage percent per host and mount point",
	}, []string{"host", "mount"})

	promDiskFreeGB = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sentinel_disk_free_gb",
		Help: "Disk free space GB per host and mount point",
	}, []string{"host", "mount"})

	promNetRecv = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sentinel_net_recv_mbps",
		Help: "Network receive speed MB/s per host",
	}, []string{"host"})

	promNetSent = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sentinel_net_sent_mbps",
		Help: "Network send speed MB/s per host",
	}, []string{"host"})

	promHostOnline = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sentinel_host_online",
		Help: "1 if host is online (last seen < 30s), 0 otherwise",
	}, []string{"host"})
)

// updatePrometheusMetrics обновляет Prometheus метрики из кэша хостов.
func updatePrometheusMetrics() {
	for _, e := range store.GetAll() {
		promCPU.WithLabelValues(e.Host).Set(e.CPU)
		promRAM.WithLabelValues(e.Host).Set(e.RAMUsed)
		promNetRecv.WithLabelValues(e.Host).Set(e.NetRecvMBps)
		promNetSent.WithLabelValues(e.Host).Set(e.NetSentMBps)
		online := 0.0
		if store.IsOnline(e.Host) {
			online = 1.0
		}
		promHostOnline.WithLabelValues(e.Host).Set(online)

		// Метрики по каждому диску отдельно
		if len(e.Disks) > 0 {
			for _, d := range e.Disks {
				promDisk.WithLabelValues(e.Host, d.Mount).Set(d.UsedPct)
				promDiskFreeGB.WithLabelValues(e.Host, d.Mount).Set(d.FreeGB)
			}
		} else {
			// Fallback для старых данных
			promDisk.WithLabelValues(e.Host, "total").Set(e.DiskUsed)
			promDiskFreeGB.WithLabelValues(e.Host, "total").Set(e.DiskFreeGB)
		}
	}
}

type HostStatus struct {
	models.MetricEvent
	Online bool `json:"online"`
}

type HistoryPoint struct {
	Time      string  `json:"time"`
	CPU       float64 `json:"cpu"`
	RAM       float64 `json:"ram"`
	NetSentMBps float64 `json:"net_sent_mbps"`
	NetRecvMBps float64 `json:"net_recv_mbps"`
	DiskRead  float64 `json:"disk_read"`
	DiskWrite float64 `json:"disk_write"`
}

func makeSessionToken(username, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(username))
	return username + ":" + hex.EncodeToString(mac.Sum(nil))
}

func verifySessionToken(token, secret string) bool {
	for i, c := range token {
		if c == ':' && i > 0 {
			username := token[:i]
			expected := makeSessionToken(username, secret)
			return hmac.Equal([]byte(token), []byte(expected))
		}
	}
	return false
}

func auth(secret string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie("sentinel_session")
		if err != nil || !verifySessionToken(cookie.Value, secret) {
			http.Redirect(w, r, "/login", http.StatusFound)
			return
		}
		next(w, r)
	}
}

func mustReadFile(path string) string {
	data, err := templateFS.ReadFile(path)
	if err != nil {
		panic(err)
	}
	return string(data)
}

func main() {
	godotenv.Load()

	// Логи
	logDir := os.Getenv("LOG_DIR")
	if logDir == "" {
		logDir = "logs"
	}
	os.MkdirAll(logDir, 0755)

	logFile, err := logger.Init(filepath.Join(logDir, "dashboard.log"))
	if err != nil {
		slog.Error("ошибка инициализации логов", "err", err)
		return
	} else {
		defer logFile.Close()
	}

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	dashPort := os.Getenv("DASHBOARD_PORT")
	if dashPort == "" {
		dashPort = "8080"
	}

	dashUser := os.Getenv("DASHBOARD_USER")
	dashPass := os.Getenv("DASHBOARD_PASS")
	sessionSecret := os.Getenv("SESSION_SECRET")

	if dashUser == "" {
		dashUser = "admin"
	}
	if dashPass == "" {
		dashPass = "admin"
		slog.Warn("DASHBOARD_PASS не задан, используется 'admin' — смени в .env!")
	}
	if sessionSecret == "" {
		sessionSecret = "default-secret-change-me"
		slog.Warn("SESSION_SECRET не задан — смени в .env!")
	}

	// БД
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
	if err := db.InitSpeedtest(context.Background()); err != nil {
		slog.Error("ошибка инициализации speedtest", "err", err)
		return
	}
	if err := db.InitIncidents(context.Background()); err != nil {
		slog.Error("ошибка инициализации инцидентов", "err", err)
		return
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
		slog.Error("ошибка подключения к NATS", "url", natsURL, "err", err)
		return
	}
	defer nc.Close()
	slog.Info("подключился к NATS", "url", natsURL)

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

		_, err = js.Subscribe("metrics.>", func(msg *nats.Msg) {
			var e models.MetricEvent
			if err := json.Unmarshal(msg.Data, &e); err != nil {
				slog.Error("ошибка парсинга метрики", "err", err)
				msg.Ack()
				return
			}
			store.SetWithMerge(e)
			updatePrometheusMetrics()
			data, _ := json.Marshal(e)
			clients.broadcast(data)
			msg.Ack()
		}, nats.Durable("dashboard"), nats.DeliverNew())
	if err != nil {
		// Если consumer уже существует с другим конфигом — удаляем и создаём заново
		if strings.Contains(err.Error(), "consumer name already in use") || strings.Contains(err.Error(), "already exists") {
			slog.Warn("consumer 'dashboard' уже существует, удаляем и пересоздаём...")
			_ = js.DeleteConsumer("METRICS", "dashboard")
			_, err = js.Subscribe("metrics.>", func(msg *nats.Msg) {
				var e models.MetricEvent
				if err := json.Unmarshal(msg.Data, &e); err != nil {
					slog.Error("ошибка парсинга метрики", "err", err)
					msg.Ack()
					return
				}
				store.SetWithMerge(e)
				updatePrometheusMetrics()
				data, _ := json.Marshal(e)
				clients.broadcast(data)
				msg.Ack()
			}, nats.Durable("dashboard"), nats.DeliverNew())
			if err != nil {
				slog.Error("не удалось создать подписку после пересоздания", "err", err)
				os.Exit(1)
			}
			slog.Info("подписка dashboard пересоздана")
		} else {
			slog.Error("ошибка подписки на метрики", "err", err)
			os.Exit(1)
		}
	}
	slog.Info("подписка на метрики создана")

	// Prometheus /metrics endpoint (без авторизации — для Prometheus scraper)
	http.Handle("/metrics", promhttp.Handler())

	loginTmpl, _ := template.New("login").Parse(mustReadFile("templates/login.html"))

	// Логин
	http.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			loginTmpl.Execute(w, map[string]string{"Error": ""})
			return
		}
		username := r.FormValue("username")
		password := r.FormValue("password")
		if username != dashUser || password != dashPass {
			slog.Warn("неудачная попытка входа", "username", username, "ip", r.RemoteAddr)
			w.WriteHeader(http.StatusUnauthorized)
			loginTmpl.Execute(w, map[string]string{"Error": "Невірний логін або пароль"})
			return
		}
		token := makeSessionToken(username, sessionSecret)
		http.SetCookie(w, &http.Cookie{
			Name:     "sentinel_session",
			Value:    token,
			Path:     "/",
			HttpOnly: true,
			MaxAge:   86400 * 7,
		})
		slog.Info("успешный вход", "username", username, "ip", r.RemoteAddr)
		http.Redirect(w, r, "/", http.StatusFound)
	})

	http.HandleFunc("/logout", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{
			Name:   "sentinel_session",
			Value:  "",
			Path:   "/",
			MaxAge: -1,
		})
		slog.Info("выход из системы", "ip", r.RemoteAddr)
		http.Redirect(w, r, "/login", http.StatusFound)
	})

	// API
	http.HandleFunc("/api/hosts", auth(sessionSecret, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		all := store.GetAll()
		result := make([]HostStatus, 0, len(all))
		for _, e := range all {
			result = append(result, HostStatus{
				MetricEvent: e,
				Online:      store.IsOnline(e.Host),
			})
		}
		json.NewEncoder(w).Encode(result)
	}))

	http.HandleFunc("/api/history", auth(sessionSecret, func(w http.ResponseWriter, r *http.Request) {
		hours := 1
		if r.URL.Query().Get("hours") == "24" {
			hours = 24
		}
		host := r.URL.Query().Get("host")
		if host == "" {
			host = "my-pc"
		}
		points, err := db.GetMetricsHistory(r.Context(), host, hours)
		if err != nil {
			slog.Error("ошибка загрузки истории", "err", err)
			http.Error(w, err.Error(), 500)
			return
		}
		// Convert []models.MetricEvent to []HistoryPoint
		hp := make([]HistoryPoint, 0, len(points))
		for _, ev := range points {
			hp = append(hp, HistoryPoint{
				Time:      ev.Time.Format("15:04:05"),
				CPU:       ev.CPU,
				RAM:       ev.RAMUsed,
				NetSentMBps: ev.NetSentMBps,
				NetRecvMBps: ev.NetRecvMBps,
				DiskRead:  ev.DiskReadMBps,
				DiskWrite: ev.DiskWriteMBps,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(hp)
	}))

	http.HandleFunc("/api/speedtest", auth(sessionSecret, func(w http.ResponseWriter, r *http.Request) {
		host := r.URL.Query().Get("host")
		if host == "" {
			host = "my-pc"
		}
		hours := 24
		if r.URL.Query().Get("hours") == "168" {
			hours = 168
		}
		results, err := db.GetSpeedtestHistory(r.Context(), host, hours)
		if err != nil {
			slog.Error("ошибка загрузки speedtest", "err", err)
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(results)
	}))

	http.HandleFunc("/api/incidents", auth(sessionSecret, func(w http.ResponseWriter, r *http.Request) {
		hours := 24
		if r.URL.Query().Get("hours") == "168" {
			hours = 168
		}
		incidents, err := db.GetIncidents(r.Context(), hours)
		if err != nil {
			slog.Error("ошибка загрузки инцидентов", "err", err)
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(incidents)
	}))

	// Kill process API
	type KillRequest struct {
		Host string `json:"host"`
		PID  int    `json:"pid"`
	}

	http.HandleFunc("/api/kill", auth(sessionSecret, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "метод не підтримується", 405)
			return
		}
		var req KillRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		cmd := map[string]interface{}{
			"action": "KILL",
			"pid":    req.PID,
		}
		data, _ := json.Marshal(cmd)
		topic := "commands." + req.Host
		if err := nc.Publish(topic, data); err != nil {
			slog.Error("ошибка отправки команды KILL", "err", err)
			http.Error(w, "failed to send command", 500)
			return
		}
		slog.Info("команда KILL отправлена", "host", req.Host, "pid", req.PID)
		w.WriteHeader(http.StatusOK)
	}))

	http.HandleFunc("/api/rules", auth(sessionSecret, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			rules, err := db.GetRules(r.Context())
			if err != nil {
				slog.Error("ошибка загрузки правил", "err", err)
				http.Error(w, err.Error(), 500)
				return
			}
			json.NewEncoder(w).Encode(rules)
			return
		}
		if r.Method == http.MethodPost {
			var rule models.Rule
			if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			id, err := db.AddRule(r.Context(), rule)
			if err != nil {
				slog.Error("ошибка добавления правила", "err", err)
				http.Error(w, err.Error(), 500)
				return
			}
			rule.ID = id
			slog.Info("правило добавлено", "metric", rule.Metric, "threshold", rule.Threshold)
			json.NewEncoder(w).Encode(rule)
			return
		}
		http.Error(w, "метод не підтримується", 405)
	}))

	http.HandleFunc("/api/rules/", auth(sessionSecret, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPut {
			var rule models.Rule
			if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			if err := db.UpdateRule(r.Context(), rule); err != nil {
				slog.Error("ошибка обновления правила", "err", err)
				http.Error(w, err.Error(), 500)
				return
			}
			slog.Info("правило обновлено", "id", rule.ID, "metric", rule.Metric)
			json.NewEncoder(w).Encode(rule)
			return
		}
		if r.Method == http.MethodDelete {
			var rule models.Rule
			if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			if err := db.DeleteRule(r.Context(), rule.ID); err != nil {
				slog.Error("ошибка удаления правила", "err", err)
				http.Error(w, err.Error(), 500)
				return
			}
			slog.Info("правило удалено", "id", rule.ID)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.Error(w, "метод не підтримується", 405)
	}))

	// WebSocket
	http.HandleFunc("/ws", auth(sessionSecret, func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			slog.Error("ошибка WebSocket upgrade", "err", err)
			return
		}
		clients.add(conn)
		slog.Info("браузер подключился", "ip", r.RemoteAddr)
		defer func() {
			clients.remove(conn)
			conn.Close()
			slog.Info("браузер отключился", "ip", r.RemoteAddr)
		}()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				break
			}
		}
	}))

	// Страницы
	http.HandleFunc("/incidents", auth(sessionSecret, func(w http.ResponseWriter, r *http.Request) {
		data, err := templateFS.ReadFile("templates/incidents.html")
		if err != nil {
			slog.Error("ошибка чтения шаблона incidents", "err", err)
			http.Error(w, "не можу прочитати шаблон", 500)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(data)
	}))

	http.HandleFunc("/rules", auth(sessionSecret, func(w http.ResponseWriter, r *http.Request) {
		data, err := templateFS.ReadFile("templates/rules.html")
		if err != nil {
			slog.Error("ошибка чтения шаблона rules", "err", err)
			http.Error(w, "не можу прочитати шаблон", 500)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(data)
	}))

	http.HandleFunc("/", auth(sessionSecret, func(w http.ResponseWriter, r *http.Request) {
		data, err := templateFS.ReadFile("templates/index.html")
		if err != nil {
			slog.Error("ошибка чтения шаблона index", "err", err)
			http.Error(w, "не можу прочитати шаблон", 500)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(data)
	}))

	addr := ":" + dashPort
	slog.Info("дашборд запущен",
		"addr", "http://localhost:"+dashPort,
		"user", dashUser,
		"db", dbURL,
	)

	srv := &http.Server{Addr: addr}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("ошибка HTTP-сервера", "err", err)
			return
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	slog.Info("дашборд останавливается...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("ошибка остановки HTTP-сервера", "err", err)
		return
	}
	slog.Info("дашборд остановлен")
}