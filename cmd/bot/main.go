package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"

	"sentinel/internal/hostcache"
	"sentinel/internal/logger"
	"sentinel/internal/models"
	"sentinel/internal/storage"
)

const (
	cmdStatus = "status:"
	cmdMute   = "mute:"
	cmdUnmute = "unmute:"
	cmdAlerts = "alerts"
	cmdTop    = "top:"
	cmdKill   = "kill:"
	modeHTML  = "HTML"
)

var store = hostcache.New()

type muteStore struct {
	mu    sync.RWMutex
	cache map[string]time.Time
	db    storage.Storage
}

func newMuteStore(db storage.Storage) *muteStore {
	m := &muteStore{
		cache: make(map[string]time.Time),
		db:    db,
	}
	if saved, err := db.GetMutes(context.Background()); err == nil {
		m.cache = saved
		slog.Info("завантажено заглушень з БД", "count", len(saved))
	}
	return m
}

func (m *muteStore) mute(host string, duration time.Duration) {
	until := time.Now().Add(duration)
	m.mu.Lock()
	m.cache[host] = until
	m.mu.Unlock()
	if err := m.db.SetMute(context.Background(), host, until); err != nil {
		slog.Error("помилка збереження mute в БД", "host", host, "err", err)
	}
}

func (m *muteStore) unmute(host string) {
	m.mu.Lock()
	delete(m.cache, host)
	m.mu.Unlock()
	if err := m.db.DeleteMute(context.Background(), host); err != nil {
		slog.Error("помилка видалення mute з БД", "host", host, "err", err)
	}
}

func (m *muteStore) isMuted(host string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	until, ok := m.cache[host]
	if !ok {
		return false
	}
	return time.Now().Before(until)
}

var mutes *muteStore

func colorEmoji(value, warn, danger float64) string {
	if value >= danger {
		return "🔴"
	}
	if value >= warn {
		return "🟡"
	}
	return "🟢"
}

func formatDuration(d time.Duration) string {
	if d.Hours() >= 1 {
		return fmt.Sprintf("%.0f год %.0f хв", d.Hours(), d.Minutes()-d.Hours()*60)
	}
	if d.Minutes() >= 1 {
		return fmt.Sprintf("%.0f хв", d.Minutes())
	}
	return fmt.Sprintf("%.0f с", d.Seconds())
}

func hostStatusText(e models.MetricEvent, online bool) string {
	if !online {
		return fmt.Sprintf("📊 <b>%s</b> · офлайн\n\nОстанні дані: %s",
			e.Host, e.Time.Format("15:04:05 02.01.2006"))
	}
	ago := time.Since(e.Time)

	diskLines := ""
	if len(e.Disks) > 0 {
		for _, d := range e.Disks {
			diskLines += fmt.Sprintf("%s %s: <b>%.1f%%</b> · вільн. %.1f GB\n",
				colorEmoji(d.UsedPct, 85, 95), d.Mount, d.UsedPct, d.FreeGB)
		}
	} else {
		diskLines = fmt.Sprintf("%s Диск: <b>%.1f%%</b> · вільн. %.1f GB\n",
			colorEmoji(e.DiskUsed, 85, 95), e.DiskUsed, e.DiskFreeGB)
	}

	return fmt.Sprintf(
		"📊 <b>%s</b> · %s тому\n\n"+
			"%s CPU:  <b>%.1f%%</b>\n"+
			"%s RAM:  <b>%.1f%%</b> · вільн. %.1f GB\n"+
			"%s"+
			"🌐 Мережа: ↓%.2f MB/s  ↑%.2f MB/s",
		e.Host, formatDuration(ago),
		colorEmoji(e.CPU, 50, 80), e.CPU,
		colorEmoji(e.RAMUsed, 70, 90), e.RAMUsed, e.RAMFreeGB,
		diskLines, e.NetRecvMBps, e.NetSentMBps,
	)
}

func hostStatusButtons(host string, isMuted bool) tgbotapi.InlineKeyboardMarkup {
	muteLabel := "🔕 Заглушити 30хв"
	muteData := fmt.Sprintf("%s%s:30", cmdMute, host)
	if isMuted {
		muteLabel = "🔔 Увімкнути алерти"
		muteData = cmdUnmute + host
	}
	return tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData(muteLabel, muteData),
			tgbotapi.NewInlineKeyboardButtonData("📋 Алерти", cmdAlerts),
		),
	)
}

func setupLogs() io.Closer {
	logDir := os.Getenv("LOG_DIR")
	if logDir == "" {
		logDir = "logs"
	}
	os.MkdirAll(logDir, 0755)
	logFile, err := logger.Init(filepath.Join(logDir, "bot.log"))
	if err != nil {
		slog.Error("помилка ініціалізації логів", "err", err)
		os.Exit(1)
	}
	return logFile
}

func setupDatabase() storage.Storage {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("DB_PATH")
		if dbURL == "" {
			dbURL = "metrics.db"
		}
	}
	db, err := storage.NewStorage(context.Background(), dbURL)
	if err != nil {
		slog.Error("помилка БД", "err", err)
		os.Exit(1)
	}
	db.InitRules(context.Background())
	db.InitIncidents(context.Background())
	db.InitMutes(context.Background())
	return db
}

func setupNats() *nats.Conn {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}
	slog.Info("підключення до NATS", "url", natsURL)
	nc, err := nats.Connect(natsURL,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(5*time.Second),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			slog.Warn("NATS відключился", "err", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			slog.Info("NATS перепідключився", "url", nc.ConnectedUrl())
		}),
	)
	if err != nil {
		slog.Error("помилка підключення до NATS", "url", natsURL, "err", err)
		os.Exit(1)
	}
	slog.Info("підключився до NATS", "url", natsURL)

	js, err := nc.JetStream()
	if err != nil {
		slog.Error("помилка JetStream", "err", err)
		os.Exit(1)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "METRICS",
		Subjects: []string{"metrics.>"},
		MaxAge:   24 * time.Hour,
	})
	if err != nil && !strings.Contains(err.Error(), "stream already exists") {
		slog.Error("помилка створення стріму METRICS", "err", err)
		os.Exit(1)
	}
	slog.Info("стрім METRICS готовий")

	_, err = js.Subscribe("metrics.>", func(msg *nats.Msg) {
		var e models.MetricEvent
		if err := json.Unmarshal(msg.Data, &e); err != nil {
			slog.Error("помилка парсингу метрики", "err", err)
			msg.Ack()
			return
		}
		store.Set(e)
		slog.Debug("метрику збережено в кеш", "host", e.Host, "cpu", e.CPU)
		msg.Ack()
	}, nats.Durable("bot"), nats.DeliverNew())
	if err != nil {
		if strings.Contains(err.Error(), "consumer name already in use") || strings.Contains(err.Error(), "already exists") {
			slog.Warn("consumer 'bot' вже існує, видаляємо та перестворюємо...")
			_ = js.DeleteConsumer("METRICS", "bot")
			_, err2 := js.Subscribe("metrics.>", func(msg *nats.Msg) {
				var e models.MetricEvent
				if err := json.Unmarshal(msg.Data, &e); err != nil {
					slog.Error("помилка парсингу метрики", "err", err)
					msg.Ack()
					return
				}
				store.Set(e)
				msg.Ack()
			}, nats.Durable("bot"), nats.DeliverNew())
			if err2 != nil {
				slog.Error("не вдалося створити підписку після перестворення", "err", err2)
				os.Exit(1)
			}
			slog.Info("підписку на metrics перестворено")
		} else {
			slog.Error("помилка підписки на metrics", "err", err)
			os.Exit(1)
		}
	}
	slog.Info("підписку на metrics створено")
	return nc
}

func main() {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("PANIC в main", "err", r)
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			slog.Error("stack: " + string(buf[:n]))
		}
	}()

	godotenv.Load()
	slog.Info("завантаження .env завершено")

	logFile := setupLogs()
	defer logFile.Close()
	slog.Info("логи ініціалізовано")

	db := setupDatabase()
	defer db.Close()
	slog.Info("БД підключена")

	mutes = newMuteStore(db)
	slog.Info("muteStore ініціалізовано")

	store = hostcache.New()
	slog.Info("store ініціалізовано", "store_ptr", fmt.Sprintf("%p", store))

	nc := setupNats()
	defer nc.Close()
	slog.Info("NATS підключено")

	bot, err := tgbotapi.NewBotAPI(os.Getenv("TELEGRAM_TOKEN"))
	if err != nil {
		slog.Error("помилка створення бота", "err", err)
		return
	}
	slog.Info("бота створено", "username", bot.Self.UserName)

	allowedIDs := parseAllowedIDs()

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60
	updates := bot.GetUpdatesChan(u)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case update := <-updates:
			if update.CallbackQuery != nil {
				handleCallback(bot, update.CallbackQuery, db, nc, allowedIDs)
				continue
			}
			if update.Message != nil {
				handleMessage(bot, update, db, nc, allowedIDs)
			}
		case <-quit:
			slog.Info("бот зупиняється...")
			bot.StopReceivingUpdates()
			return
		}
	}
}

func parseAllowedIDs() map[int64]bool {
	allowed := make(map[int64]bool)
	if raw := os.Getenv("ALLOWED_CHAT_IDS"); raw != "" {
		for _, part := range strings.Split(raw, ",") {
			if id, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64); err == nil {
				allowed[id] = true
			}
		}
	}
	return allowed
}

func handleMessage(bot *tgbotapi.BotAPI, update tgbotapi.Update, db storage.Storage, nc *nats.Conn, allowedIDs map[int64]bool) {
	chatID := update.Message.Chat.ID
	if len(allowedIDs) > 0 && !allowedIDs[chatID] {
		bot.Send(tgbotapi.NewMessage(chatID, "⛔ Доступ заборонений"))
		return
	}

	text := update.Message.Text
	slog.Info("команду отримано", "text", text, "user", update.Message.From.UserName)

	switch {
	case text == "/start" || text == "/help":
		handleHelp(bot, chatID)
	case text == "/hosts":
		handleHosts(bot, chatID)
	case text == "/alerts":
		handleAlerts(bot, chatID, db)
	case strings.HasPrefix(text, "/status"):
		parts := strings.Fields(text)
		if len(parts) >= 2 {
			handleStatus(bot, chatID, parts[1])
		} else {
			bot.Send(tgbotapi.NewMessage(chatID, "⚠️ Вкажіть ім'я хоста.\nПриклад: `/status my-pc`"))
		}
	case strings.HasPrefix(text, "/speedtest"):
		parts := strings.Fields(text)
		if len(parts) >= 2 {
			handleSpeedtest(bot, chatID, parts[1], db)
		} else {
			bot.Send(tgbotapi.NewMessage(chatID, "⚠️ Вкажіть ім'я хоста.\nПриклад: `/speedtest my-pc`"))
		}
	case strings.HasPrefix(text, "/mute"):
		parts := strings.Fields(text)
		if len(parts) >= 3 {
			handleMute(bot, chatID, parts[1], parts[2])
		} else {
			bot.Send(tgbotapi.NewMessage(chatID, "⚠️ Вкажіть хост та час у хвилинах.\nПриклад: `/mute my-pc 30`"))
		}
	case strings.HasPrefix(text, "/top"):
		parts := strings.Fields(text)
		if len(parts) >= 2 {
			handleTop(bot, chatID, parts[1], nc)
		} else {
			bot.Send(tgbotapi.NewMessage(chatID, "⚠️ Вкажіть ім'я хоста.\nПриклад: `/top my-pc`"))
		}
	}
}

func handleHelp(bot *tgbotapi.BotAPI, chatID int64) {
	text := `🛡 <b>Sentinel Monitor Bot</b>

<b>Команди:</b>
/hosts — список усіх хостів
/status &lt;хост&gt; — метрики хоста
/top &lt;хост&gt; — список топ процесів
/alerts — активні інциденти
/speedtest &lt;хост&gt; — останній speedtest
/mute &lt;хост&gt; &lt;хвилини&gt; — заглушити алерти
/help — ця довідка`
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = modeHTML
	bot.Send(msg)
}

func handleHosts(bot *tgbotapi.BotAPI, chatID int64) {
	hosts := store.GetAll()
	if len(hosts) == 0 {
		bot.Send(tgbotapi.NewMessage(chatID, "Немає підключених хостів"))
		return
	}

	text := "🖥 <b>Хости</b>\n\n"
	var buttons [][]tgbotapi.InlineKeyboardButton

	for _, h := range hosts {
		online := store.IsOnline(h.Host)
		status, muteIcon := "⚫", ""
		if online {
			status = "🟢"
		}
		if mutes.isMuted(h.Host) {
			muteIcon = " 🔕"
		}

		text += fmt.Sprintf("%s <b>%s</b>%s · CPU %.1f%% · RAM %.1f%%\n",
			status, h.Host, muteIcon, h.CPU, h.RAMUsed)

		if online {
			buttons = append(buttons, tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonData("📊 "+h.Host, cmdStatus+h.Host),
			))
		}
	}

	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = modeHTML
	if len(buttons) > 0 {
		msg.ReplyMarkup = tgbotapi.NewInlineKeyboardMarkup(buttons...)
	}
	bot.Send(msg)
}

func handleStatus(bot *tgbotapi.BotAPI, chatID int64, host string) {
	e, ok := store.Get(host)
	if !ok {
		bot.Send(tgbotapi.NewMessage(chatID, "Хост <b>"+host+"</b> не знайдено"))
		return
	}
	online := store.IsOnline(host)
	msg := tgbotapi.NewMessage(chatID, hostStatusText(e, online))
	msg.ParseMode = modeHTML
	if online {
		msg.ReplyMarkup = hostStatusButtons(host, mutes.isMuted(host))
	}
	bot.Send(msg)
}

func handleAlerts(bot *tgbotapi.BotAPI, chatID int64, db storage.Storage) {
	incidents, err := db.GetIncidents(context.Background(), 24)
	if err != nil {
		bot.Send(tgbotapi.NewMessage(chatID, "Помилка отримання інцидентів"))
		return
	}

	active := incidents[:0]
	for _, inc := range incidents {
		if inc.ResolvedAt == nil {
			active = append(active, inc)
		}
	}

	if len(active) == 0 {
		msg := tgbotapi.NewMessage(chatID, "✅ <b>Активних інцидентів немає</b>")
		msg.ParseMode = modeHTML
		bot.Send(msg)
		return
	}

	text := fmt.Sprintf("🔔 <b>Активні інциденти (%d)</b>\n\n", len(active))
	for _, inc := range active {
		duration := time.Since(inc.StartedAt)
		text += fmt.Sprintf("⚠️ <b>%s</b> · %s %.1f%% (поріг %.0f%%)\n ⏱ %s\n\n",
			inc.Host, strings.ToUpper(inc.Metric), inc.Value, inc.Threshold, formatDuration(duration))
	}

	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = modeHTML
	bot.Send(msg)
}

func handleSpeedtest(bot *tgbotapi.BotAPI, chatID int64, host string, db storage.Storage) {
	results, err := db.GetSpeedtestHistory(context.Background(), host, 24)
	if err != nil || len(results) == 0 {
		bot.Send(tgbotapi.NewMessage(chatID, "Немає даних speedtest для "+host))
		return
	}

	last := results[len(results)-1]
	text := fmt.Sprintf("⚡ <b>Speedtest · %s</b>\n\n↓ <b>%.1f</b> ↑ <b>%.1f</b> Mbps\nPing: %.0f ms",
		host, last.DownloadMbps, last.UploadMbps, last.PingMs)

	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = modeHTML
	bot.Send(msg)
}

func handleMute(bot *tgbotapi.BotAPI, chatID int64, host, minStr string) {
	mins, err := strconv.Atoi(minStr)
	if err != nil || mins <= 0 {
		bot.Send(tgbotapi.NewMessage(chatID, "Вкажіть хвилини (число > 0)"))
		return
	}

	duration := time.Duration(mins) * time.Minute
	mutes.mute(host, duration)

	text := fmt.Sprintf("🔕 Алерти для <b>%s</b> заглушено на %d хв.", host, mins)
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = modeHTML
	bot.Send(msg)
}

func handleTop(bot *tgbotapi.BotAPI, chatID int64, host string, nc *nats.Conn) {
	e, ok := store.Get(host)
	if !ok {
		bot.Send(tgbotapi.NewMessage(chatID, "Хост <b>"+host+"</b> не знайдено"))
		return
	}

	if len(e.TopProcesses) == 0 {
		bot.Send(tgbotapi.NewMessage(chatID, "Немає даних про процеси для "+host))
		return
	}

	text := fmt.Sprintf("📋 <b>Топ процеси на %s</b>\n\n", host)
	var rows [][]tgbotapi.InlineKeyboardButton

	for _, p := range e.TopProcesses {
		text += fmt.Sprintf("<code>%5d</code> | %-20s | CPU: %5.1f%% | RAM: %5.1fMB\n",
			p.PID, p.Name, p.CPUPct, p.MemMB)

		killData := fmt.Sprintf("%s%s:%d", cmdKill, host, p.PID)
		rows = append(rows, tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf("❌ Kill %d", p.PID), killData),
		))
	}

	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = modeHTML
	if len(rows) > 0 {
		msg.ReplyMarkup = tgbotapi.NewInlineKeyboardMarkup(rows...)
	}
	bot.Send(msg)
}

func handleKill(bot *tgbotapi.BotAPI, cb *tgbotapi.CallbackQuery, host string, pidStr string, nc *nats.Conn) {
	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		bot.Request(tgbotapi.NewCallback(cb.ID, "Невірний PID"))
		return
	}

	cmd := map[string]interface{}{
		"action": "KILL",
		"pid":    pid,
	}
	data, _ := json.Marshal(cmd)
	topic := fmt.Sprintf("commands.%s", host)

	if err := nc.Publish(topic, data); err != nil {
		slog.Error("помилка відправки команди KILL", "err", err)
		bot.Request(tgbotapi.NewCallback(cb.ID, "Помилка відправлення команди"))
		return
	}

	bot.Request(tgbotapi.NewCallback(cb.ID, fmt.Sprintf("Команду на завершення процесу [%d] відправлено на %s", pid, host)))
}

func handleCallback(bot *tgbotapi.BotAPI, cb *tgbotapi.CallbackQuery, db storage.Storage, nc *nats.Conn, allowedIDs map[int64]bool) {
	chatID := cb.Message.Chat.ID
	if len(allowedIDs) > 0 && !allowedIDs[chatID] {
		return
	}

	data := cb.Data

	switch {
	case strings.HasPrefix(data, cmdStatus):
		bot.Send(tgbotapi.NewCallback(cb.ID, ""))
		handleStatus(bot, chatID, strings.TrimPrefix(data, cmdStatus))
	case strings.HasPrefix(data, cmdMute):
		bot.Send(tgbotapi.NewCallback(cb.ID, ""))
		parts := strings.Split(data, ":")
		if len(parts) == 3 {
			handleMute(bot, chatID, parts[1], parts[2])
		}
	case strings.HasPrefix(data, cmdUnmute):
		bot.Send(tgbotapi.NewCallback(cb.ID, ""))
		host := strings.TrimPrefix(data, cmdUnmute)
		mutes.unmute(host)
		bot.Send(tgbotapi.NewMessage(chatID, "🔔 Алерти для <b>"+host+"</b> увімкнено"))
	case data == cmdAlerts:
		bot.Send(tgbotapi.NewCallback(cb.ID, ""))
		handleAlerts(bot, chatID, db)
	case strings.HasPrefix(data, cmdKill):
		parts := strings.Split(data, ":")
		if len(parts) == 3 {
			handleKill(bot, cb, parts[1], parts[2], nc)
		}
	}
}