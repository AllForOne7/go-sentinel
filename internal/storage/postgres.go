package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"sentinel/internal/models"
)

// postgresDB implements Storage for PostgreSQL.
type postgresDB struct {
	pool *pgxpool.Pool
}

// NewPostgresStorage creates a PostgreSQL-backed Storage.
func NewPostgresStorage(ctx context.Context, connString string) (Storage, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("parse postgres connection string: %w", err)
	}
	// Basic pool configuration
	config.MaxConns = 10
	config.MinConns = 2
	config.MaxConnLifetime = time.Hour
	config.HealthCheckPeriod = time.Minute
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}
	// Optionally, we could run migrations here, but we assume they are applied externally.
	return &postgresDB{pool: pool}, nil
}

// Close releases the pool.
func (db *postgresDB) Close() error {
	db.pool.Close()
	return nil
}

// Save inserts a metric event.
func (db *postgresDB) Save(ctx context.Context, e models.MetricEvent) error {
	_, err := db.pool.Exec(ctx, `
		INSERT INTO metrics
			(host, ts, cpu_pct, ram_pct, ram_free,
			 net_sent_mbps, net_recv_mbps,
			 disk_read_mbps, disk_write_mbps)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`,
		e.Host, e.Time, e.CPU, e.RAMUsed, e.RAMFreeGB,
		e.NetSentMBps, e.NetRecvMBps,
		e.DiskReadMBps, e.DiskWriteMBps,
	)
	return err
}

// GetMetricsHistory returns historical metrics for a host.
func (db *postgresDB) GetMetricsHistory(ctx context.Context, host string, hours int) ([]models.MetricEvent, error) {
	since := time.Now().Add(-time.Duration(hours) * time.Hour)
	rows, err := db.pool.Query(ctx, `
		SELECT ts, cpu_pct, ram_pct, ram_free,
		       net_sent_mbps, net_recv_mbps,
		       disk_read_mbps, disk_write_mbps
		FROM metrics
		WHERE host = $1 AND ts > $2
		ORDER BY ts ASC
		LIMIT 500
	`, host, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []models.MetricEvent
	for rows.Next() {
		var e models.MetricEvent
		var ts time.Time
		if err := rows.Scan(&ts, &e.CPU, &e.RAMUsed, &e.RAMFreeGB,
			&e.NetSentMBps, &e.NetRecvMBps,
			&e.DiskReadMBps, &e.DiskWriteMBps); err != nil {
			return nil, err
		}
		e.Host = host
		e.Time = ts
		events = append(events, e)
	}
	return events, nil
}

// InitRules ensures the rules table exists (no-op if migrations applied).
func (db *postgresDB) InitRules(ctx context.Context) error {
	// In a real app, you might run migrations here. We'll just ensure default rows.
	var count int
	err := db.pool.QueryRow(ctx, `SELECT COUNT(*) FROM rules`).Scan(&count)
	if err != nil {
		return err
	}
	if count == 0 {
		_, err = db.pool.Exec(ctx, `INSERT INTO rules (host, metric, threshold, count, enabled) VALUES ('my-pc', 'cpu', 60, 3, true)`)
		if err != nil {
			return err
		}
		_, err = db.pool.Exec(ctx, `INSERT INTO rules (host, metric, threshold, count, enabled) VALUES ('my-pc', 'ram', 80, 1, true)`)
		if err != nil {
			return err
		}
		_, err = db.pool.Exec(ctx, `INSERT INTO rules (host, metric, threshold, count, enabled) VALUES ('my-pc', 'disk', 85, 1, true)`)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetRules returns all rules.
func (db *postgresDB) GetRules(ctx context.Context) ([]models.Rule, error) {
	rows, err := db.pool.Query(ctx, `SELECT id, host, metric, threshold, count, enabled FROM rules ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rules []models.Rule
	for rows.Next() {
		var r models.Rule
		var enabled bool
		if err := rows.Scan(&r.ID, &r.Host, &r.Metric, &r.Threshold, &r.Count, &enabled); err != nil {
			return nil, err
		}
		r.Enabled = enabled
		rules = append(rules, r)
	}
	return rules, nil
}

// AddRule inserts a rule and returns its ID.
func (db *postgresDB) AddRule(ctx context.Context, r models.Rule) (int64, error) {
	var id int64
	err := db.pool.QueryRow(ctx,
		`INSERT INTO rules (host, metric, threshold, count, enabled) VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		r.Host, r.Metric, r.Threshold, r.Count, r.Enabled,
	).Scan(&id)
	return id, err
}

// UpdateRule updates a rule.
func (db *postgresDB) UpdateRule(ctx context.Context, r models.Rule) error {
	_, err := db.pool.Exec(ctx,
		`UPDATE rules SET host=$1, metric=$2, threshold=$3, count=$4, enabled=$5 WHERE id=$6`,
		r.Host, r.Metric, r.Threshold, r.Count, r.Enabled, r.ID,
	)
	return err
}

// DeleteRule deletes a rule by ID.
func (db *postgresDB) DeleteRule(ctx context.Context, id int64) error {
	_, err := db.pool.Exec(ctx, `DELETE FROM rules WHERE id=$1`, id)
	return err
}

// InitSpeedtest ensures speedtest table exists (no-op).
func (db *postgresDB) InitSpeedtest(ctx context.Context) error {
	// Table should exist via migrations; we can do nothing.
	return nil
}

// SaveSpeedtest inserts a speedtest result.
func (db *postgresDB) SaveSpeedtest(ctx context.Context, r models.SpeedtestResult) error {
	_, err := db.pool.Exec(ctx, `
		INSERT INTO speedtest (host, ts, download_mbps, upload_mbps, ping_ms, server)
		VALUES ($1, $2, $3, $4, $5, $6)
	`,
		r.Host, r.Time, r.DownloadMbps, r.UploadMbps, r.PingMs, r.Server,
	)
	return err
}

// GetSpeedtestHistory returns speedtest results for a host.
func (db *postgresDB) GetSpeedtestHistory(ctx context.Context, host string, hours int) ([]models.SpeedtestResult, error) {
	since := time.Now().Add(-time.Duration(hours) * time.Hour)
	rows, err := db.pool.Query(ctx, `
		SELECT ts, download_mbps, upload_mbps, ping_ms, server
		FROM speedtest
		WHERE host = $1 AND ts > $2
		ORDER BY ts ASC
	`, host, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.SpeedtestResult
	for rows.Next() {
		var r models.SpeedtestResult
		if err := rows.Scan(&r.Time, &r.DownloadMbps, &r.UploadMbps, &r.PingMs, &r.Server); err != nil {
			return nil, err
		}
		r.Host = host
		results = append(results, r)
	}
	return results, nil
}

// InitMutes ensures mutes table exists.
func (db *postgresDB) InitMutes(ctx context.Context) error {
	return nil
}

// SetMute inserts or updates a mute record.
func (db *postgresDB) SetMute(ctx context.Context, host string, until time.Time) error {
	_, err := db.pool.Exec(ctx, `
		INSERT INTO mutes (host, muted_until) VALUES ($1, $2)
		ON CONFLICT (host) DO UPDATE SET muted_until = EXCLUDED.muted_until
	`, host, until)
	return err
}

// DeleteMute removes a mute record.
func (db *postgresDB) DeleteMute(ctx context.Context, host string) error {
	_, err := db.pool.Exec(ctx, `DELETE FROM mutes WHERE host=$1`, host)
	return err
}

// GetMutes returns active mutes.
func (db *postgresDB) GetMutes(ctx context.Context) (map[string]time.Time, error) {
	rows, err := db.pool.Query(ctx, `SELECT host, muted_until FROM mutes WHERE muted_until > now()`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]time.Time)
	for rows.Next() {
		var host string
		var until time.Time
		if err := rows.Scan(&host, &until); err != nil {
			return nil, err
		}
		result[host] = until
	}
	return result, nil
}

// InitIncidents ensures incidents table exists.
func (db *postgresDB) InitIncidents(ctx context.Context) error {
	return nil
}

// OpenIncident inserts a new incident and returns its ID.
func (db *postgresDB) OpenIncident(ctx context.Context, host, metric string, value, threshold float64, startedAt time.Time) (int64, error) {
	var id int64
	err := db.pool.QueryRow(ctx, `
		INSERT INTO incidents (host, metric, value, threshold, started_at)
		VALUES ($1, $2, $3, $4, $5) RETURNING id
	`, host, metric, value, threshold, startedAt).Scan(&id)
	return id, err
}

// CloseIncident sets resolved_at for the latest open incident of a host+metric.
func (db *postgresDB) CloseIncident(ctx context.Context, host, metric string, resolvedAt time.Time) error {
	_, err := db.pool.Exec(ctx, `
		UPDATE incidents
		SET resolved_at = $1
		WHERE id = (
			SELECT id FROM incidents
			WHERE host = $2 AND metric = $3 AND resolved_at IS NULL
			ORDER BY started_at DESC
			LIMIT 1
		)
	`, resolvedAt, host, metric)
	return err
}

// GetOpenIncidents returns all currently open incidents.
func (db *postgresDB) GetOpenIncidents(ctx context.Context) ([]models.Incident, error) {
	rows, err := db.pool.Query(ctx, `
		SELECT id, host, metric, value, threshold, started_at
		FROM incidents
		WHERE resolved_at IS NULL
		ORDER BY started_at ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var incidents []models.Incident // <-- Исправлено
	for rows.Next() {
		var inc models.Incident // <-- Исправлено
		var resolvedAt *time.Time
		if err := rows.Scan(&inc.ID, &inc.Host, &inc.Metric, &inc.Value, &inc.Threshold, &inc.StartedAt); err != nil {
			return nil, err
		}
		inc.ResolvedAt = resolvedAt
		inc.Duration = "активен"
		incidents = append(incidents, inc)
	}
	return incidents, nil
}

// GetIncidents returns incidents within the last hours.
func (db *postgresDB) GetIncidents(ctx context.Context, hours int) ([]models.Incident, error) {
	since := time.Now().Add(-time.Duration(hours) * time.Hour)
	rows, err := db.pool.Query(ctx, `
		SELECT id, host, metric, value, threshold, started_at, resolved_at
		FROM incidents
		WHERE started_at > $1
		ORDER BY started_at DESC
		LIMIT 100
	`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var incidents []models.Incident // <-- Исправлено
	for rows.Next() {
		var inc models.Incident // <-- Исправлено
		var resolvedAt *time.Time
		if err := rows.Scan(&inc.ID, &inc.Host, &inc.Metric, &inc.Value, &inc.Threshold, &inc.StartedAt, &resolvedAt); err != nil {
			return nil, err
		}
		inc.ResolvedAt = resolvedAt
		if resolvedAt != nil {
			d := resolvedAt.Sub(inc.StartedAt)
			if d.Hours() >= 1 {
				inc.Duration = fmt.Sprintf("%.0f ч", d.Hours())
			} else if d.Minutes() >= 1 {
				inc.Duration = fmt.Sprintf("%.0f мин", d.Minutes())
			} else {
				inc.Duration = fmt.Sprintf("%.0f сек", d.Seconds())
			}
		} else {
			inc.Duration = "активен"
		}
		incidents = append(incidents, inc)
	}
	return incidents, nil
}
