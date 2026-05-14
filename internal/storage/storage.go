package storage

import (
	"context"
	"database/sql"
	"sentinel/internal/models"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// Storage — единый интерфейс для работы с БД (SQLite или PostgreSQL)
type Storage interface {
	Save(ctx context.Context, e models.MetricEvent) error
	Close() error

	InitRules(ctx context.Context) error
	GetRules(ctx context.Context) ([]models.Rule, error)
	AddRule(ctx context.Context, r models.Rule) (int64, error)
	UpdateRule(ctx context.Context, r models.Rule) error
	DeleteRule(ctx context.Context, id int64) error

	InitSpeedtest(ctx context.Context) error
	SaveSpeedtest(ctx context.Context, r models.SpeedtestResult) error
	GetSpeedtestHistory(ctx context.Context, host string, hours int) ([]models.SpeedtestResult, error)

	GetMetricsHistory(ctx context.Context, host string, hours int) ([]models.MetricEvent, error)

	InitMutes(ctx context.Context) error
	SetMute(ctx context.Context, host string, until time.Time) error
	DeleteMute(ctx context.Context, host string) error
	GetMutes(ctx context.Context) (map[string]time.Time, error)

	InitIncidents(ctx context.Context) error
	OpenIncident(ctx context.Context, host, metric string, value, threshold float64, startedAt time.Time) (int64, error)
	CloseIncident(ctx context.Context, host, metric string, resolvedAt time.Time) error
	GetOpenIncidents(ctx context.Context) ([]models.Incident, error)        // Исправлено на models.Incident
	GetIncidents(ctx context.Context, hours int) ([]models.Incident, error) // Исправлено на models.Incident
}

// DB реализует интерфейс Storage для SQLite
type DB struct {
	conn *sql.DB
}

// New создает экземпляр SQLite
func New(ctx context.Context, path string) (Storage, error) {
	dsn := path + "?_journal_mode=WAL&_busy_timeout=5000&_synchronous=NORMAL"
	conn, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	conn.SetMaxOpenConns(1)

	// Базовая инициализация таблиц
	_, err = conn.Exec(`
		CREATE TABLE IF NOT EXISTS metrics (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			host TEXT,
			ts DATETIME,
			cpu_pct REAL,
			ram_pct REAL,
			ram_free REAL,
			net_sent REAL,
			net_recv REAL,
			disk_pct REAL,
			disk_free REAL,
			net_sent_mbps REAL DEFAULT 0,
			net_recv_mbps REAL DEFAULT 0,
			disk_read_mbps REAL DEFAULT 0,
			disk_write_mbps REAL DEFAULT 0
		)`)
	return &DB{conn: conn}, err
}

// Фабрика для выбора БД
func NewStorage(ctx context.Context, connString string) (Storage, error) {
	if strings.HasPrefix(connString, "postgres://") || strings.HasPrefix(connString, "postgresql://") {
		return NewPostgresStorage(ctx, connString)
	}
	return New(ctx, connString)
}

func (db *DB) Close() error {
	return db.conn.Close()
}

// --- Реализация методов с добавлением Context ---

func (db *DB) Save(ctx context.Context, e models.MetricEvent) error {
	_, err := db.conn.ExecContext(ctx, `
		INSERT INTO metrics (host, ts, cpu_pct, ram_pct, ram_free, net_sent, net_recv, disk_pct, disk_free, net_sent_mbps, net_recv_mbps, disk_read_mbps, disk_write_mbps)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.Host, e.Time, e.CPU, e.RAMUsed, e.RAMFreeGB, e.NetSentMBps, e.NetRecvMBps, e.DiskUsed, e.DiskFreeGB, e.NetSentMBps, e.NetRecvMBps, e.DiskReadMBps, e.DiskWriteMBps)
	return err
}

func (db *DB) InitRules(ctx context.Context) error {
	_, err := db.conn.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS rules (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			host TEXT,
			metric TEXT,
			threshold REAL,
			count INTEGER,
			enabled BOOLEAN DEFAULT 1
		)`)
	return err
}

func (db *DB) GetRules(ctx context.Context) ([]models.Rule, error) {
	rows, err := db.conn.QueryContext(ctx, "SELECT id, host, metric, threshold, count, enabled FROM rules")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var rules []models.Rule
	for rows.Next() {
		var r models.Rule
		rows.Scan(&r.ID, &r.Host, &r.Metric, &r.Threshold, &r.Count, &r.Enabled)
		rules = append(rules, r)
	}
	return rules, nil
}

func (db *DB) AddRule(ctx context.Context, r models.Rule) (int64, error) {
	res, err := db.conn.ExecContext(ctx, "INSERT INTO rules (host, metric, threshold, count, enabled) VALUES (?, ?, ?, ?, ?)",
		r.Host, r.Metric, r.Threshold, r.Count, r.Enabled)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (db *DB) UpdateRule(ctx context.Context, r models.Rule) error {
	_, err := db.conn.ExecContext(ctx, "UPDATE rules SET host=?, metric=?, threshold=?, count=?, enabled=? WHERE id=?",
		r.Host, r.Metric, r.Threshold, r.Count, r.Enabled, r.ID)
	return err
}

func (db *DB) DeleteRule(ctx context.Context, id int64) error {
	_, err := db.conn.ExecContext(ctx, "DELETE FROM rules WHERE id=?", id)
	return err
}

func (db *DB) GetMetricsHistory(ctx context.Context, host string, hours int) ([]models.MetricEvent, error) {
	since := time.Now().Add(-time.Duration(hours) * time.Hour)
	rows, err := db.conn.QueryContext(ctx, "SELECT ts, cpu_pct, ram_pct, disk_pct FROM metrics WHERE host=? AND ts > ? ORDER BY ts ASC", host, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var history []models.MetricEvent
	for rows.Next() {
		var e models.MetricEvent
		rows.Scan(&e.Time, &e.CPU, &e.RAMUsed, &e.DiskUsed)
		e.Host = host
		history = append(history, e)
	}
	return history, nil
}

// --- Speedtest ---

func (db *DB) InitSpeedtest(ctx context.Context) error {
	_, err := db.conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS speedtest (id INTEGER PRIMARY KEY AUTOINCREMENT, host TEXT, ts DATETIME, download_mbps REAL, upload_mbps REAL, ping_ms REAL, server TEXT)`)
	return err
}

func (db *DB) SaveSpeedtest(ctx context.Context, r models.SpeedtestResult) error {
	_, err := db.conn.ExecContext(ctx, "INSERT INTO speedtest (host, ts, download_mbps, upload_mbps, ping_ms, server) VALUES (?, ?, ?, ?, ?, ?)",
		r.Host, r.Time, r.DownloadMbps, r.UploadMbps, r.PingMs, r.Server)
	return err
}

func (db *DB) GetSpeedtestHistory(ctx context.Context, host string, hours int) ([]models.SpeedtestResult, error) {
	since := time.Now().Add(-time.Duration(hours) * time.Hour)
	rows, err := db.conn.QueryContext(ctx, "SELECT ts, download_mbps, upload_mbps, ping_ms, server FROM speedtest WHERE host=? AND ts > ? ORDER BY ts ASC", host, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var results []models.SpeedtestResult
	for rows.Next() {
		var r models.SpeedtestResult
		rows.Scan(&r.Time, &r.DownloadMbps, &r.UploadMbps, &r.PingMs, &r.Server)
		r.Host = host
		results = append(results, r)
	}
	return results, nil
}

// --- Mutes ---

func (db *DB) InitMutes(ctx context.Context) error {
	_, err := db.conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS mutes (host TEXT PRIMARY KEY, muted_until DATETIME)`)
	return err
}

func (db *DB) SetMute(ctx context.Context, host string, until time.Time) error {
	_, err := db.conn.ExecContext(ctx, "INSERT INTO mutes (host, muted_until) VALUES (?, ?) ON CONFLICT(host) DO UPDATE SET muted_until=excluded.muted_until", host, until)
	return err
}

func (db *DB) DeleteMute(ctx context.Context, host string) error {
	_, err := db.conn.ExecContext(ctx, "DELETE FROM mutes WHERE host=?", host)
	return err
}

func (db *DB) GetMutes(ctx context.Context) (map[string]time.Time, error) {
	rows, err := db.conn.QueryContext(ctx, "SELECT host, muted_until FROM mutes")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res := make(map[string]time.Time)
	for rows.Next() {
		var h string
		var t time.Time
		rows.Scan(&h, &t)
		res[h] = t
	}
	return res, nil
}

// --- Incidents ---

func (db *DB) InitIncidents(ctx context.Context) error {
	_, err := db.conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS incidents (id INTEGER PRIMARY KEY AUTOINCREMENT, host TEXT, metric TEXT, value REAL, threshold REAL, started_at DATETIME, resolved_at DATETIME)`)
	return err
}

func (db *DB) OpenIncident(ctx context.Context, host, metric string, value, threshold float64, startedAt time.Time) (int64, error) {
	res, err := db.conn.ExecContext(ctx, "INSERT INTO incidents (host, metric, value, threshold, started_at) VALUES (?, ?, ?, ?, ?)", host, metric, value, threshold, startedAt)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (db *DB) CloseIncident(ctx context.Context, host, metric string, resolvedAt time.Time) error {
	_, err := db.conn.ExecContext(ctx, "UPDATE incidents SET resolved_at=? WHERE host=? AND metric=? AND resolved_at IS NULL", resolvedAt, host, metric)
	return err
}

func (db *DB) GetOpenIncidents(ctx context.Context) ([]models.Incident, error) {
	rows, err := db.conn.QueryContext(ctx, "SELECT id, host, metric, value, threshold, started_at FROM incidents WHERE resolved_at IS NULL")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []models.Incident
	for rows.Next() {
		var i models.Incident
		rows.Scan(&i.ID, &i.Host, &i.Metric, &i.Value, &i.Threshold, &i.StartedAt)
		res = append(res, i)
	}
	return res, nil
}

func (db *DB) GetIncidents(ctx context.Context, hours int) ([]models.Incident, error) {
	since := time.Now().Add(-time.Duration(hours) * time.Hour)
	rows, err := db.conn.QueryContext(ctx, "SELECT id, host, metric, value, threshold, started_at, resolved_at FROM incidents WHERE started_at > ?", since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []models.Incident
	for rows.Next() {
		var i models.Incident
		rows.Scan(&i.ID, &i.Host, &i.Metric, &i.Value, &i.Threshold, &i.StartedAt, &i.ResolvedAt)
		res = append(res, i)
	}
	return res, nil
}

// Проверка реализации интерфейса
var _ Storage = (*DB)(nil)
