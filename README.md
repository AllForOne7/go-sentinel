# 🛡️ Sentinel – розподілена SOAR‑система для моніторингу серверів у реальному часі  

![Go Version](https://img.shields.io/github/go-mod/go-version/AllForOne7/go-sentinel)  
![License](https://img.shields.io/badge/license-MIT-blue.svg)  

---

## 🏗️ Архітектура  

```
+----------------+      metrics.*      +-----------------+
|   Agent (host) |  -----------------> |   NATS JetStream|
| (per server)   |  < speedtest.* ---- |                 |
+----------------+                      +--------+--------+
                                   ｜
                 +-----------------+-----------------+
                 |                 |                 |
          +------v------+   +-----v------+   +------v------+
          | Processor   |   | Alerter    |   | Dashboard   |
          | (SQLite/PG) |   | (Telegram) |   | (WebSocket) |
          +------+------+   +------+-----+   +------+------+
                 |                 |                 |
          +------v------+   +-----v------+   +------v------+
          |   Bot       |   |   Logs     |   |   Prometheus|
          | (Telegram)  |   +------------+   +-------------+
          +-------------+
```

**Пояснення потоків даних**  
- **Agent** збирає локальні метрики (CPU, RAM, Disk, мережа, speedtest) та публікує їх у теми `metrics.*` та `speedtest.*` у NATS JetStream.  
- **NATS JetStream** – надійний broker з можливістю зберігання повідомлень (durable streams), що гарантує не втрату даних при простою сервісів.  
- **Processor** читає повідомлення, зберігає їх у вибрану СУБД (PostgreSQL або SQLite) через абстрактний інтерфейс `storage.Storage`.  
- **Alerter** оцінює правила, створює/закриває инциденти та надсилає сповіщення через Telegram‑бота.  
- **Dashboard** – веб‑інтерфейс з реальним оновленням через WebSocket, відображає графіки, інциденти та дозволяє керування правилами через UI; також експортує метрики у Prometheus (`/metrics`).  
- **Bot** – Telegram‑бот для швидкого перегляду стану хостів, инцидентів та керування мьютами (тайм‑аути сповіщень).  

---

## ⭐ Ключові особливості  

- 🗄️ **Гібридне сховище** – прозора підтримка PostgreSQL та SQLite через інтерфейс `Storage`; перемикання залежить лише від рядка `DATABASE_URL` у `.env`.  
- ⚡ **Реал‑тайм дашборд** – оновлення метрик у браузері без перезавантаження завдяки WebSocket.  
- 📱 **Телеграм‑алертинг** – автоматичні сповіщення при пороговому перетинадлю та керування через бота (`/hosts`, `/status`, `/alerts`, `/speedtest`, `/mute`). Тепер повністю українською мовою.  
- 📊 **Eкспорт у Prometheus** – стандартні метрики (`sentinel_cpu_percent`, `sentinel_ram_percent`, тощо) доступні за `/metrics`.  
- 🐳 **Docker‑готовність** – готовий `docker‑compose.yml` для швидкого розгортання NATS, PostgreSQL та всіх сервісів.  
- 🚨 **Стандартні правила алертів** – автоматична ініціалізація правил для CPU, RAM та Disk при першому запуску.  
- 📈 **Незалежні часові діапазони графіків** – кожен графік (CPU, RAM, Network) має власний вибір часового діапазону (1г/24г/живий), що дозволяє одночасно моніторити різкі проміжки часу.  
- 📋 **Згортаємий список процесів** – у дашборді додана кнопка розгортання/згортання списку запущених процесів.  

---

## 🛠️ Стек технологій  

- **Go (Golang)** ≥ 1.22  
- **NATS JetStream** – брокер повідомлень  
- **PostgreSQL** (≥ 13) або **SQLite** (файл‑база)  
- **Docker** & **docker‑compose** (опційно)  
- **HTML/CSS/JS** (embedded templates, веб‑сокет)  
- **Telegram Bot API**  
- **Prometheus** (метрики)  

---

## 🚀 Швидкий старт  

### Крок 1 – Клонуйте репозиторій  
```bash
git clone https://github.com/AllForOne7/go-sentinel.git
cd go-sentinel
```

### Крок 2 – Налаштуйте змінні оточення  
Скопіюйте приклад конфігурації та заповніть реальними значеннями:  
```bash
cp .env.example .env
# Відредагуйте .env:
#   DATABASE_URL=postgres://user:pass@localhost:5432/sentinel?sslmode=disable   # залиште порожнім → SQLite
#   TELEGRAM_TOKEN=ваш_токен_бота
#   TELEGRAM_CHAT_ID=ваш_chat_id
#   NATS_URL=nats://localhost:4222
#   DASHBOARD_PORT=8080
#   DASHBOARD_USER=admin
#   DASHBOARD_PASS=змініть_в_.env
#   SESSION_SECRET=секретний_рядок
#   LOG_DIR=logs
```

### Крок 3 – Запустіть інфраструктуру (Docker)  
```bash
docker-compose up -d   # підніме NATS та (за потреби) PostgreSQL
```

### Крок 4 – Запустіть сервіси  

**Вариант A – через скрипт (Windows)**  
```bash
start.bat   # запустить processor, alerter, dashboard, agent, bot
```

**Вариант B – вручну (Linux/macOS)**  
```bash
go run ./cmd/processor &
go run ./cmd/alerter &
go run ./cmd/dashboard &
go run ./cmd/agent &
go run ./cmd/bot &
```

### Крок 5 – Перевірте роботу  
- Відкрийте браузер: `http://localhost:8080` → сторінка логіну (або дашборд, якщо вже авторизовані).  
- У Telegram напишіть боту `/hosts` – повинно повернути список доступних хостів.  
- При першому запуску автоматично створюються стандартні правила алертів для CPU, RAM та Disk (пороги: CPU > 80%, RAM > 85%, Disk > 90%). Ви можете переглядати та редагувати їх у дашборді або через бота.  

---

##📦 Таблиця компонентів  

| Компонент | Опис | Основні файли |
|-----------|------|---------------|
| **Agent** | Збирає CPU, RAM, Disk, сеть, speedtest та надсилає в NATS | `cmd/agent/main.go`, `internal/collector/` |
| **Processor** | Зберігає метрики та speedtest у БД | `cmd/processor/main.go` |
| **Alerter** | Оцінює правила, створює інциденти, надсилає сповіщення | `cmd/alerter/main.go` |
| **Dashboard** | Веб‑UI, API, WebSocket, Prometheus endpoint | `cmd/dashboard/main.go`, `templates/` |
| **Bot** | Telegram‑бот для керування та отримання даних | `cmd/bot/main.go` |
| **NATS JetStream** | Брокер повідомлень з durability | `docker-compose.yml` (служба `nats`) |
| **Сховище** | PostgreSQL **або** SQLite через інтерфейс `Storage` | `internal/storage/storage.go`, `internal/storage/postgres.go` |

---

## 🤖 Інструкція для Telegram‑бота  

1. **Додайте бота** у Telegram за допомогою BotFather, отримайте `TELEGRAM_TOKEN`.  
2. **Вкажіть дозволені чати** у змінній оточення `ALLOWED_CHAT_IDS` (через coma, наприклад `-1001234567890,-1009876543210`). Якщо змінна не встановлена – бот відповідає на будь‑який chat (не рекомендується для продакшн).  
3. **Доступні команди**  
   Бот тепер повністю українською мовою, включаючи всі повідомлення та інтерфейс.  

| Команда | Опис |
|---------|------|
| `/hosts` | Список всіх хостів з їхнім статусом (online/offline) та базовими метриками. |
| `/status <hostname>` | Детальна інформація про конкретний хост (CPU, RAM, Disk, сеть, час з моменту останнього оновлення). |
| `/alerts` | Список активних инцидентів (тип, значення, порог, тривалість). |
| `/speedtest <hostname>` | Останній результат speedtest‑тесту для вказаного хосту. |
| `/mute <hostname> <minutes>` | Тимчасово вимкнути сповіщення для хосту (за замовчуванням 30 хвилин). |

Бот також реагує на inline‑кнопки у повідомленнях про статус хосту (увімкнути/вимкнути алерти).  

---

## 📈 Метрики Prometheus  

Дашборд експонує наступні показчики (тип – `Gauge` або `GaugeVec` з відповідними мітками):  

| Метрика | Опис | Мітки |
|---------|------|-------|
| `sentinel_cpu_percent` | Відсоток використання CPU на хості | `host` |
| `sentinel_ram_percent` | Відсоток використання RAM на хості | `host` |
| `sentinel_disk_percent` | Відсоток використання диска (за замовчуванням – загальний) | `host`, `mount` |
| `sentinel_disk_free_gb` | Вільне місце на диску (GB) | `host`, `mount` |
| `sentinel_net_recv_mbps` | Швидкість отримання даних (МБ/с) | `host` |
| `sentinel_net_sent_mbps` | Швидкість передачі даних (МБ/с) | `host` |
| `sentinel_host_online` | `1` – хост онлайн (останнє повідомлення < 30 с), `0` – офлайн | `host` |
| `sentinel_active_incidents` | Кількість активних инцидентів на хост | `host`, `metric` |
| `sentinel_rules_total` | Загальна кількість активних правил | `metric` |

Метрики оновлюються кожного разу, коли агент надсилає нову порцію даних (за замовчуванням кожні 5 секунд).  

---

## 🔧 Технічний акцент  

Серверна частина Сентинелю **повністю абстрагована від конкретної СУБД**. Всі взаємодії з базою даних відбуваються через інтерфейс  

```go
type Storage interface {
    Save(ctx context.Context, e MetricEvent) error
    GetMetricsHistory(ctx context.Context, host string, hours int) ([]MetricEvent, error)
    // ... інші методи ...
}
```

Реалізації:  

- `*DB` – SQLite (файл `metrics.db`).  
- `*postgresDB` – PostgreSQL (використовується `pgxpool`).  

Фабричная функция  

```go
func NewStorage(ctx context.Context, connString string) (Storage, error)
```  

автоматично вибирає потрібний провайдер: якщо `connString` починається з `postgres://` чи `postgresql://` – створюється PostgreSQL‑провайдер, інакше – SQLite. Таким чином, перемикання між СУБД відбувається **лише через зміну змінної оточки `DATABASE_URL`**, без модифікації коду.  

---  