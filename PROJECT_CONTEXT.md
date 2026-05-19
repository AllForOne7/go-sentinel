# Sentinel Project Context

## Project Overview

**Sentinel** — система моніторингу та SOAR (Security Orchestration, Automation, and Response) для дистрибутивного збору метрик серверів/ПК, обробки алертів та віддаленого управління процесами. Система побудова на мікросервісній архітектурі з використанням NATS як шині повідомлень.

---

## Tech Stack

- **Language**: Go 1.25.0
- **Message Bus**: NATS (JetStream) — асинхронна шина повідомлень
- **Database**: PostgreSQL 15 (основна БД) або SQLite (fallback)
- **Containerization**: Docker, Docker Compose
- **Frontend**: HTML шаблони з WebSocket для реального часу
- **Telegram Bot API**: для сповіщень та управління

**Key Go Dependencies**:

- `github.com/nats-io/nats.go` — NATS клієнт
- `github.com/jackc/pgx/v5` — PostgreSQL драйвер
- `github.com/shirou/gopsutil/v3` — збір системних метрик
- `github.com/go-telegram-bot-api/telegram-bot-api/v5` — Telegram бот
- `github.com/gorilla/websocket` — WebSocket підтримка
- `github.com/prometheus/client_golang` — Prometheus метрики

---

## Project Structure

```
sentinel/
├── cmd/                          # Точки входу мікросервісів
│   ├── agent/                    # Агент збору метрик
│   │   └── main.go
│   ├── processor/                # Обробка даних, збереження в БД
│   │   └── main.go
│   ├── alerter/                  # Логіка алертингу, відправка сповіщень
│   │   └── main.go
│   ├── bot/                      # Telegram бот (управління через команди)
│   │   └── main.go
│   ├── dashboard/                # Веб-дашборд з візуалізацією
│   │   └── main.go
│   │   └── templates/            # Вбудовані HTML шаблони
│   └── migrate/                  # Міграції БД
│       └── main.go
│
├── internal/
│   ├── collector/                # Збір системних метрик (CPU, RAM, диск, процеси)
│   │   ├── collector.go
│   │   ├── sysinfo.go            # Системна інфа (OS, CPU модель, RAM)
│   │   ├── ports.go              # Моніторинг TCP портів
│   │   └── speedtest.go          # Тест швидкості інтернету
│   │
│   ├── models/                   # Структури даних
│   │   ├── metric.go             # MetricEvent, SpeedtestResult, DiskInfo, ProcessInfo
│   │   ├── rule.go               # Rule (правила алертингу)
│   │   └── incident.go           # Incident (інциденти)
│   │
│   ├── storage/                  # Шарінг даних (Storage інтерфейс)
│   │   ├── storage.go            # Storage інтерфейс + SQLite реалізація
│   │   └── postgres.go           # PostgreSQL реалізація Storage
│   │
│   ├── hostcache/                # Кеш останніх метрик хостів (in-memory)
│   │   └── hostcache.go
│   │
│   └── logger/                   # Логування з ротацією
│       └── logger.go
│
├── migrations/                   # SQL міграції PostgreSQL
│   └── 001_create_tables.up.sql
│
├── docker-compose.yml            # Оркестрація сервісів
├── go.mod                        # Залежності Go
└── README.md
```

---

## Microservices Logic

### Agent (`cmd/agent/main.go`)

- **Роль**: Агент на моніторимому хості
- **Збір метрик**: CPU, RAM, мережа (MB/s), диск, топ-5 процесів, температура, системна інфа
- **Speedtest**: Тест швидкості інтернету кожні 30 хвилин (конфігуровано)
- **Моніторинг портів**: Перевірка доступності TCP портів з `.env`
- **Команди**: Підписка на `commands.{host}` для віддаленого kill процесів
- **Відправка**: Публікує в NATS топіки `metrics.{host}` та `speedtest.{host}`

### Processor (`cmd/processor/main.go`)

- **Роль**: Обробка метрик, збереження в БД
- **Підписка**: `metrics.>` та `speedtest.>` через JetStream
- **Функції**: Зберігає метрики та speedtest результати в PostgreSQL/SQLite
- **Не має алертингу** — тільки зберігає дані

### Alerter (`cmd/alerter/main.go`)

- **Роль**: Логіка алертингу та сповіщення
- **Динамічні пороги**: Читає правила з БД (можливість зміни on-the-fly)
- **Логіка тривоги**:
  - CPU: потрібно `count` підряд перевищення порогу
  - RAM/Disk: одне перевищення — тривога
- **Telegram сповіщення**: 🔴 CPU перегрів, 🟡 Мало пам'яті, 🟠 Мало місця на диску
- **Інциденти**: Створює/закриває інциденти в БД
- **Відновлення стану**: Після рестарту бере відкриті інциденти з БД

### Bot (`cmd/bot/main.go`)

- **Роль**: Telegram інтерфейс для моніторингу та управління
- **Команди**:
  - `/hosts` — список хостів зі статусом
  - `/status {host}` — детальні метрики
  - `/top {host}` — топ процесів з можливістю kill
  - `/alerts` — активні інциденти
  - `/speedtest {host}` — останні дані speedtest
  - `/mute {host} {minutes}` — заглушити алерти на час
- **Kill процесів**: Через кнопку "❌ Kill PID" — відправляє команду в NATS `commands.{host}`

### Dashboard (`cmd/dashboard/main.go`)

- **Роль**: Веб-інтерфейс з візуалізацією
- **Функції**:
  - WebSocket трансляція метрик в реальному часі
  - Графіки історії (1h/24h)
  - CRUD правил алертингу
  - Список інцидентів
  - Kill процесів через API
- **Prometheus метрики**: `/metrics` endpoint для scraper
- **Авторизація**: Session-based з HMAC токеном

---

## Data Flow (NATS Topics)

### Message Topics

| Topic              | Publisher      | Subscriber                         | Format                           |
| ------------------ | -------------- | ---------------------------------- | -------------------------------- |
| `metrics.{host}`   | agent          | processor, alerter, bot, dashboard | `MetricEvent` JSON               |
| `speedtest.{host}` | agent          | processor, dashboard               | `SpeedtestResult` JSON           |
| `commands.{host}`  | bot, dashboard | agent                              | `{"action": "KILL", "pid": int}` |

### JSON Message Format

**MetricEvent** (`metrics.{host}`):

```json
{
  "host": "my-pc",
  "time": "2026-05-15T00:00:00Z",
  "cpu_pct": 25.5,
  "ram_used_pct": 45.2,
  "ram_free_gb": 7.8,
  "net_sent_mbps": 1.2,
  "net_recv_mbps": 0.8,
  "disk_used_pct": 65.0,
  "disk_free_gb": 200.5,
  "disk_read_mbps": 5.1,
  "disk_write_mbps": 2.3,
  "disks": [{ "mount": "C:", "used_pct": 70.0, "free_gb": 150.0 }],
  "top_processes": [
    { "pid": 1234, "name": "chrome", "cpu_pct": 15.5, "mem_mb": 512.0 }
  ],
  "temperatures": [{ "label": "CPU Package", "current": 55.0 }],
  "sys_info": { "os": "Windows 11", "cpu_model": "i7-12700K", "cpu_cores": 12 }
}
```

**SpeedtestResult** (`speedtest.{host}`):

```json
{
  "host": "my-pc",
  "time": "2026-05-15T00:00:00Z",
  "download_mbps": 150.5,
  "upload_mbps": 50.2,
  "ping_ms": 15.0,
  "server": "Kyiv"
}
```

**Kill Command** (`commands.{host}`):

```json
{
  "action": "KILL",
  "pid": 12345
}
```

---

## Database Schema (PostgreSQL)

### Tables

**metrics** — історія метрик

```sql
id BIGSERIAL PRIMARY KEY,
host TEXT NOT NULL,
ts TIMESTAMPTZ NOT NULL,
cpu_pct REAL NOT NULL,
ram_pct REAL NOT NULL,
ram_free REAL NOT NULL,
net_sent_mbps REAL NOT NULL DEFAULT 0,
net_recv_mbps REAL NOT NULL DEFAULT 0,
disk_pct REAL NOT NULL,
disk_free REAL NOT NULL,
disk_read_mbps REAL NOT NULL DEFAULT 0,
disk_write_mbps REAL NOT NULL DEFAULT 0
```

**rules** — правила алертингу

```sql
id BIGSERIAL PRIMARY KEY,
host TEXT NOT NULL DEFAULT 'my-pc',
metric TEXT NOT NULL,  -- cpu, ram, disk
threshold REAL NOT NULL,
count INTEGER NOT NULL DEFAULT 3,
enabled BOOLEAN NOT NULL DEFAULT TRUE
```

**incidents** — інциденти

```sql
id BIGSERIAL PRIMARY KEY,
host TEXT NOT NULL,
metric TEXT NOT NULL,
value REAL NOT NULL,
threshold REAL NOT NULL,
started_at TIMESTAMPTZ NOT NULL,
resolved_at TIMESTAMPTZ,
notified BOOLEAN DEFAULT TRUE
```

**speedtest** — результати тестів швидкості

```sql
id BIGSERIAL PRIMARY KEY,
host TEXT NOT NULL,
ts TIMESTAMPTZ NOT NULL,
download_mbps REAL NOT NULL,
upload_mbps REAL NOT NULL,
ping_ms REAL NOT NULL,
server TEXT NOT NULL
```

**mutes** — заглушення алертів

```sql
host TEXT PRIMARY KEY,
muted_until TIMESTAMPTZ NOT NULL
```

---

## Key Code Patterns

### Storage Pattern (internal/storage)

Storage — це інтерфейс, який абстрагує роботу з БД:

```go
type Storage interface {
    Save(ctx context.Context, e models.MetricEvent) error
    Close() error
    InitRules/GetRules/AddRule/UpdateRule/DeleteRule
    InitSpeedtest/SaveSpeedtest/GetSpeedtestHistory
    GetMetricsHistory
    InitMutes/SetMute/DeleteMute/GetMutes
    InitIncidents/OpenIncident/CloseIncident/GetIncidents
}
```

**Factory Pattern**: `NewStorage()` автоматично вибирає SQLite або PostgreSQL за connection string:

- `postgres://` або `postgresql://` → PostgreSQL
- Інше → SQLite

### Metrics Collection (internal/collector)

Функція `Collect(host)` повертає `MetricEvent`:

1. CPU: `gopsutil.cpu.Percent()` — 1 секунда замірювання
2. RAM: `gopsutil.mem.VirtualMemory()` — використання та вільна пам'ять
3. Диск: `gopsutil.disk.Usage()` — Windows (C:,D:) або Linux (mountpoints)
4. Мережа: різниця байтів між викликами → MB/s
5. Дискова активність: `gopsutil.disk.IOCounters()` → MB/s
6. Топ-процеси: відбір за CPU > 0 або RAM > 50MB, сортування, топ-5

---

## Current Features

### Working Features ✅

- [x] Дистрибутивний збір метрик (CPU, RAM, диск, мережа)
- [x] Топ-5 процесів з PID, назвою, CPU%, RAM MB
- [x] Speedtest (download/upload/ping) кожні 30 хвилин
- [x] Моніторинг TCP портів (конфігурується в .env)
- [x] Динамічні пороги алертингу (можна змінювати в дашборді)
- [x] Підрахунок `count` для CPU (напр. 3 підряд = тривога)
- [x] Telegram сповіщення про тривоги та подолання
- [x] Інциденти в БД з тривалістю та часом
- [x] Віддалене вбивання процесів (через бот або дашборд)
- [x] Mute алерти на вказаний час
- [x] WebSocket трансляція метрик в дашборді
- [x] Prometheus метрики для зовнішнього моніторингу
- [x] Авторизація в дашборді (логін/пароль + HMAC сесія)
- [x] Підтримка PostgreSQL та SQLite

---

## Code Summary

### go.mod

```go
module sentinel

go 1.25.0

require (
    github.com/go-telegram-bot-api/telegram-bot-api/v5 v5.5.1
    github.com/gorilla/websocket v1.5.3
    github.com/jackc/pgx/v5 v5.9.2
    github.com/joho/godotenv v1.5.1
    github.com/nats-io/nats.go v1.49.0
    github.com/prometheus/client_golang v1.23.2
    github.com/shirou/gopsutil/v3 v3.24.5
    github.com/showwin/speedtest-go v1.7.10
    gopkg.in/lumberjack.v2 v2.0.0
    modernc.org/sqlite v1.46.1
)
```

### Models (internal/models)

**metric.go**:

```go
type MetricEvent struct {
    Host          string     `json:"host"`
    Time          time.Time  `json:"time"`
    CPU           float64    `json:"cpu_pct"`
    RAMUsed       float64    `json:"ram_used_pct"`
    RAMFreeGB     float64    `json:"ram_free_gb"`
    NetSentMBps   float64    `json:"net_sent_mbps"`
    NetRecvMBps   float64    `json:"net_recv_mbps"`
    DiskUsed      float64    `json:"disk_used_pct"`
    DiskFreeGB    float64    `json:"disk_free_gb"`
    DiskReadMBps  float64    `json:"disk_read_mbps"`
    DiskWriteMBps float64    `json:"disk_write_mbps"`
    Disks         []DiskInfo `json:"disks,omitempty"`
    TopProcesses  []ProcessInfo `json:"top_processes,omitempty"`
    Temperatures  []TemperatureInfo `json:"temperatures,omitempty"`
    SysInfo       *SystemInfo `json:"sys_info,omitempty"`
    Ports         []PortStatus `json:"ports,omitempty"`
}

type DiskInfo struct {
    Mount   string  `json:"mount"`
    UsedPct float64 `json:"used_pct"`
    FreeGB  float64 `json:"free_gb"`
    TotalGB float64 `json:"total_gb"`
}

type ProcessInfo struct {
    PID    int32   `json:"pid"`
    Name   string  `json:"name"`
    CPUPct float64 `json:"cpu_pct"`
    MemMB  float64 `json:"mem_mb"`
    MemPct float64 `json:"mem_pct"`
}

type SpeedtestResult struct {
    Host         string    `json:"host"`
    Time         time.Time `json:"time"`
    DownloadMbps float64   `json:"download_mbps"`
    UploadMbps   float64   `json:"upload_mbps"`
    PingMs       float64   `json:"ping_ms"`
    Server       string    `json:"server"`
}
```

**rule.go**:

```go
type Rule struct {
    ID        int64   `json:"id"`
    Host      string  `json:"host"`
    Metric    string  `json:"metric"`    // cpu, ram, disk
    Threshold float64 `json:"threshold"` // поріг %
    Count     int     `json:"count"`     // замірів підряд
    Enabled   bool    `json:"enabled"`
}
```

**incident.go**:

```go
type Incident struct {
    ID         int64      `json:"id"`
    Host       string     `json:"host"`
    Metric     string     `json:"metric"`
    Value      float64    `json:"value"`
    Threshold  float64    `json:"threshold"`
    StartedAt  time.Time  `json:"started_at"`
    ResolvedAt *time.Time `json:"resolved_at"`
    Duration   string     `json:"duration"`
}
```

### Processor — Main Handler (cmd/processor/main.go)

```go
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
    // ...
}
```

### Alerter — Alert Logic (cmd/alerter/main.go)

```go
func check(e models.MetricEvent, token, chatID string, rules []models.Rule, db storage.Storage) {
    statesMu.Lock()
    defer statesMu.Unlock()

    if states[e.Host] == nil {
        states[e.Host] = &alertState{}
    }
    s := states[e.Host]

    // CPU з підрахунком count
    cpuThreshold, cpuCount, cpuEnabled := getRuleThreshold(rules, "cpu")
    if cpuEnabled && e.CPU > cpuThreshold {
        s.cpuHighCount++
        if s.cpuHighCount >= cpuCount && !s.cpuAlerted {
            sendTelegram(token, chatID, "🔴 CPU перегрев!")
            db.OpenIncident(...)
            s.cpuAlerted = true
        }
    } else if s.cpuAlerted {
        sendTelegram(token, chatID, "✅ CPU в норме")
        db.CloseIncident(...)
        s.cpuAlerted = false
        s.cpuHighCount = 0
    }
    // ... RAM, Disk аналогічно
}
```

### Bot — Kill Command (cmd/bot/main.go)

```go
func handleTop(bot *tgbotapi.BotAPI, chatID int64, host string, nc *nats.Conn) {
    e, ok := store.Get(host)
    if !ok || len(e.TopProcesses) == 0 {
        bot.Send(tgbotapi.NewMessage(chatID, "Нет данных о процессах"))
        return
    }

    text := fmt.Sprintf("📋 Топ процессы на %s\n\n", host)
    var rows [][]tgbotapi.InlineKeyboardButton

    for _, p := range e.TopProcesses {
        text += fmt.Sprintf("%5d | %-20s | CPU: %5.1f%% | RAM: %5.1fMB\n",
            p.PID, p.Name, p.CPUPct, p.MemMB)

        killData := fmt.Sprintf("%s%s:%d", cmdKill, host, p.PID)
        rows = append(rows, tgbotapi.NewInlineKeyboardRow(
            tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf("❌ Kill %d", p.PID), killData),
        ))
    }
    // ...
}
```

### HostCache (internal/hostcache/hostcache.go)

```go
type Cache struct {
    mu       sync.RWMutex
    hosts    map[string]models.MetricEvent
    lastSeen map[string]time.Time
}

func (c *Cache) Set(e models.MetricEvent) {
    c.mu.Lock()
    c.hosts[e.Host] = e
    c.lastSeen[e.Host] = time.Now()
    c.mu.Unlock()
}

func (c *Cache) IsOnline(host string) bool {
    c.mu.RLock()
    defer c.mu.RUnlock()
    t, ok := c.lastSeen[host]
    if !ok {
        return false
    }
    return time.Since(t) < 30*time.Second
}
```
