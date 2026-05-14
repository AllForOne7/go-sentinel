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
		slog.Info("загружено заглушений из БД", "count", len(saved))
	}
	return m
}

func (m *muteStore) mute(host string, duration time.Duration) {
	until := time.Now().Add(duration)
	m.mu.Lock()
	m.cache[host] = until
	m.mu.Unlock()
	if err := m.db.SetMute(context.Background(), host, until); err != nil {
		slog.Error("ошибка сохранения mute в БД", "host", host, "err", err)
	}
}

func (m *muteStore) unmute(host string) {
	m.mu.Lock()
	delete(m.cache, host)
	m.mu.Unlock()
	if err := m.db.DeleteMute(context.Background(), host); err != nil {
		slog.Error("ошибка удаления mute из БД", "host", host, "err", err)
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
		return fmt.Sprintf("%.0f ч %.0f мин", d.Hours(), d.Minutes()-d.Hours()*60)
	}
	if d.Minutes() >= 1 {
		return fmt.Sprintf("%.0f мин", d.Minutes())
	}
	return fmt.Sprintf("%.0f сек", d.Seconds())
}

func hostStatusText(e models.MetricEvent, online bool) string {
	if !online {
		return fmt.Sprintf("📊 <b>%s</b> · офлайн\n\nПоследние данные: %s",
			e.Host, e.Time.Format("15:04:05 02.01.2006"))
	}
	ago := time.Since(e.Time)

	diskLines := ""
	if len(e.Disks) > 0 {
		for _, d := range e.Disks {
			diskLines += fmt.Sprintf("%s %s: <b>%.1f%%</b> · св. %.1f GB\n",
				colorEmoji(d.UsedPct, 85, 95), d.Mount, d.UsedPct, d.FreeGB)
		}
	} else {
		diskLines = fmt.Sprintf("%s Диск: <b>%.1f%%</b> · св. %.1f GB\n",
			colorEmoji(e.DiskUsed, 85, 95), e.DiskUsed, e.DiskFreeGB)
	}

	return fmt.Sprintf(
		"📊 <b>%s</b> · %s назад\n\n"+
			"%s CPU:  <b>%.1f%%</b>\n"+
			"%s RAM:  <b>%.1f%%</b> · св. %.1f GB\n"+
			"%s"+
			"🌐 Сеть: ↓%.2f MB/s  ↑%.2f MB/s",
		e.Host, formatDuration(ago),
		colorEmoji(e.CPU, 50, 80), e.CPU,
		colorEmoji(e.RAMUsed, 70, 90), e.RAMUsed, e.RAMFreeGB,
		diskLines, e.NetRecvMBps, e.NetSentMBps,
	)
}

func hostStatusButtons(host string, isMuted bool) tgbotapi.InlineKeyboardMarkup {
	muteLabel := "🔕 Заглушить 30м"
	muteData := fmt.Sprintf("%s%s:30", cmdMute, host)
	if isMuted {
		muteLabel = "🔔 Включить алерты"
		muteData = cmdUnmute + host
	}
	return tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData(muteLabel, muteData),
			tgbotapi.NewInlineKeyboardButtonData("📋 Алерты", cmdAlerts),
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
		slog.Error("ошибка инициализации логов", "err", err)
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
		slog.Error("ошибка БД", "err", err)
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
	slog.Info("подключение к NATS", "url", natsURL)
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
		os.Exit(1)
	}
	slog.Info("подключился к NATS", "url", natsURL)

	js, err := nc.JetStream()
	if err != nil {
		slog.Error("ошибка JetStream", "err", err)
		os.Exit(1)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "METRICS",
		Subjects: []string{"metrics.>"},
		MaxAge:   24 * time.Hour,
	})
	if err != nil && !strings.Contains(err.Error(), "stream already exists") {
		slog.Error("ошибка создания стрима METRICS", "err", err)
		os.Exit(1)
	}
	slog.Info("стрим METRICS готов")

	_, err = js.Subscribe("metrics.>", func(msg *nats.Msg) {
		var e models.MetricEvent
		if err := json.Unmarshal(msg.Data, &e); err != nil {
			slog.Error("ошибка парсинга метрики", "err", err)
			msg.Ack()
			return
		}
		store.Set(e)
		slog.Debug("метрика сохранена в кэш", "host", e.Host, "cpu", e.CPU)
		msg.Ack()
	}, nats.Durable("bot"), nats.DeliverNew())
	if err != nil {
		if strings.Contains(err.Error(), "consumer name already in use") || strings.Contains(err.Error(), "already exists") {
			slog.Warn("consumer 'bot' уже существует, удаляем и пересоздаём...")
			_ = js.DeleteConsumer("METRICS", "bot")
			_, err2 := js.Subscribe("metrics.>", func(msg *nats.Msg) {
				var e models.MetricEvent
				if err := json.Unmarshal(msg.Data, &e); err != nil {
					slog.Error("ошибка парсинга метрики", "err", err)
					msg.Ack()
					return
				}
				store.Set(e)
				msg.Ack()
			}, nats.Durable("bot"), nats.DeliverNew())
			if err2 != nil {
				slog.Error("не удалось создать подписку после пересоздания", "err", err2)
				os.Exit(1)
			}
			slog.Info("подписка на metrics пересоздана")
		} else {
			slog.Error("ошибка подписки на metrics", "err", err)
			os.Exit(1)
		}
	}
	slog.Info("подписка на metrics создана")
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
	slog.Info("загрузка .env завершена")

	logFile := setupLogs()
	defer logFile.Close()
	slog.Info("логи инициализированы")

	db := setupDatabase()
	defer db.Close()
	slog.Info("БД подключена")

	mutes = newMuteStore(db)
	slog.Info("muteStore инициализирован")

	store = hostcache.New()
	slog.Info("store инициализирован", "store_ptr", fmt.Sprintf("%p", store))

	nc := setupNats()
	defer nc.Close()
	slog.Info("NATS подключен")

	bot, err := tgbotapi.NewBotAPI(os.Getenv("TELEGRAM_TOKEN"))
	if err != nil {
		slog.Error("ошибка создания бота", "err", err)
		return
	}
	slog.Info("бот создан", "username", bot.Self.UserName)

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
			slog.Info("бот останавливается...")
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
		bot.Send(tgbotapi.NewMessage(chatID, "⛔ Доступ запрещён"))
		return
	}

	text := update.Message.Text
	slog.Info("команда получена", "text", text, "user", update.Message.From.UserName)

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

<b>Команды:</b>
/hosts — список всех хостов
/status &lt;хост&gt; — метрики хоста
/top &lt;хост&gt; — список топ процессов
/alerts — активные инциденты
/speedtest &lt;хост&gt; — последний speedtest
/mute &lt;хост&gt; &lt;минуты&gt; — заглушить алерты
/help — эта справка`
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = modeHTML
	bot.Send(msg)
}

func handleHosts(bot *tgbotapi.BotAPI, chatID int64) {
	hosts := store.GetAll()
	if len(hosts) == 0 {
		bot.Send(tgbotapi.NewMessage(chatID, "Нет подключённых хостов"))
		return
	}

	text := "🖥 <b>Хосты</b>\n\n"
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
		bot.Send(tgbotapi.NewMessage(chatID, "Хост <b>"+host+"</b> не найден"))
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
		bot.Send(tgbotapi.NewMessage(chatID, "Ошибка получения инцидентов"))
		return
	}

	active := incidents[:0]
	for _, inc := range incidents {
		if inc.ResolvedAt == nil {
			active = append(active, inc)
		}
	}

	if len(active) == 0 {
		msg := tgbotapi.NewMessage(chatID, "✅ <b>Активных инцидентов нет</b>")
		msg.ParseMode = modeHTML
		bot.Send(msg)
		return
	}

	text := fmt.Sprintf("🔔 <b>Активные инциденты (%d)</b>\n\n", len(active))
	for _, inc := range active {
		duration := time.Since(inc.StartedAt)
		text += fmt.Sprintf("⚠️ <b>%s</b> · %s %.1f%% (порог %.0f%%)\n ⏱ %s\n\n",
			inc.Host, strings.ToUpper(inc.Metric), inc.Value, inc.Threshold, formatDuration(duration))
	}

	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = modeHTML
	bot.Send(msg)
}

func handleSpeedtest(bot *tgbotapi.BotAPI, chatID int64, host string, db storage.Storage) {
	results, err := db.GetSpeedtestHistory(context.Background(), host, 24)
	if err != nil || len(results) == 0 {
		bot.Send(tgbotapi.NewMessage(chatID, "Нет данных speedtest для "+host))
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
		bot.Send(tgbotapi.NewMessage(chatID, "Укажите минуты (число > 0)"))
		return
	}

	duration := time.Duration(mins) * time.Minute
	mutes.mute(host, duration)

	text := fmt.Sprintf("🔕 Алерты для <b>%s</b> заглушены на %d мин.", host, mins)
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = modeHTML
	bot.Send(msg)
}

func handleTop(bot *tgbotapi.BotAPI, chatID int64, host string, nc *nats.Conn) {
	e, ok := store.Get(host)
	if !ok {
		bot.Send(tgbotapi.NewMessage(chatID, "Хост <b>"+host+"</b> не найден"))
		return
	}

	if len(e.TopProcesses) == 0 {
		bot.Send(tgbotapi.NewMessage(chatID, "Нет данных о процессах для "+host))
		return
	}

	text := fmt.Sprintf("📋 <b>Топ процессы на %s</b>\n\n", host)
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
		bot.Request(tgbotapi.NewCallback(cb.ID, "Неверный PID"))
		return
	}

	cmd := map[string]interface{}{
		"action": "KILL",
		"pid":    pid,
	}
	data, _ := json.Marshal(cmd)
	topic := fmt.Sprintf("commands.%s", host)

	if err := nc.Publish(topic, data); err != nil {
		slog.Error("ошибка отправки команды KILL", "err", err)
		bot.Request(tgbotapi.NewCallback(cb.ID, "Ошибка отправки команды"))
		return
	}

	bot.Request(tgbotapi.NewCallback(cb.ID, fmt.Sprintf("Команду на завершение процесу [%d] відправлено на %s", pid, host)))
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
		bot.Send(tgbotapi.NewMessage(chatID, "🔔 Алерты для <b>"+host+"</b> включены"))
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