package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "modernc.org/sqlite"
)

func main() {
	// Load environment variables
	sqlitePath := os.Getenv("DB_PATH")
	if sqlitePath == "" {
		sqlitePath = "metrics.db"
	}
	pgConnStr := os.Getenv("DATABASE_URL")
	if pgConnStr == "" {
		slog.Error("DATABASE_URL environment variable is required for migration")
		os.Exit(1)
	}

	// Open SQLite (read-only mode)
	sqliteDB, err := sql.Open("sqlite", sqlitePath+"?_journal_mode=WAL&_mode=ro")
	if err != nil {
		slog.Error("failed to open SQLite database", "err", err)
		os.Exit(1)
	}
	defer sqliteDB.Close()
	if err = sqliteDB.Ping(); err != nil {
		slog.Error("failed to ping SQLite database", "err", err)
		os.Exit(1)
	}

	// Configure PostgreSQL pool
	pgConfig, err := pgxpool.ParseConfig(pgConnStr)
	if err != nil {
		slog.Error("failed to parse PostgreSQL connection string", "err", err)
		os.Exit(1)
	}
	pgConfig.MaxConns = 5
	pgConfig.MinConns = 2
	pgConfig.MaxConnLifetime = time.Hour
	pgPool, err := pgxpool.NewWithConfig(context.Background(), pgConfig)
	if err != nil {
		slog.Error("failed to create PostgreSQL pool", "err", err)
		os.Exit(1)
	}
	defer pgPool.Close()
	if err = pgPool.Ping(context.Background()); err != nil {
		slog.Error("failed to ping PostgreSQL", "err", err)
		os.Exit(1)
	}

	slog.Info("Starting migration from SQLite to PostgreSQL",
		"sqlite", sqlitePath, "postgres", pgConnStr)

	// Migrate each table using watermark-based approach
	if err = migrateMetrics(context.Background(), sqliteDB, pgPool); err != nil {
		slog.Error("failed to migrate metrics", "err", err)
		os.Exit(1)
	}
	if err = migrateSpeedtest(context.Background(), sqliteDB, pgPool); err != nil {
		slog.Error("failed to migrate speedtest", "err", err)
		os.Exit(1)
	}
	if err = migrateRules(context.Background(), sqliteDB, pgPool); err != nil {
		slog.Error("failed to migrate rules", "err", err)
		os.Exit(1)
	}
	if err = migrateMutes(context.Background(), sqliteDB, pgPool); err != nil {
		slog.Error("failed to migrate mutes", "err", err)
		os.Exit(1)
	}
	if err = migrateIncidents(context.Background(), sqliteDB, pgPool); err != nil {
		slog.Error("failed to migrate incidents", "err", err)
		os.Exit(1)
	}

	slog.Info("Migration completed successfully")
}

// getMaxID returns the maximum id in the given table, or 0 if the table is empty.
func getMaxID(ctx context.Context, pgPool *pgxpool.Pool, tableName string) (int64, error) {
	var maxID int64
	err := pgPool.QueryRow(ctx, fmt.Sprintf("SELECT COALESCE(MAX(id), 0) FROM %s", tableName)).Scan(&maxID)
	return maxID, err
}

// migrateMetrics migrates the metrics table
func migrateMetrics(ctx context.Context, sqliteDB *sql.DB, pgPool *pgxpool.Pool) error {
	watermark, err := getMaxID(ctx, pgPool, "metrics")
	if err != nil {
		return fmt.Errorf("get max id for metrics: %w", err)
	}

	rows, err := sqliteDB.QueryContext(ctx, `
		SELECT host, ts, cpu_pct, ram_pct, ram_free,
		       net_sent, net_recv, disk_pct, disk_free,
		       net_sent_mbps, net_recv_mbps,
		       disk_read_mbps, disk_write_mbps
		FROM metrics
		WHERE id > $1
		ORDER BY id
	`, watermark)
	if err != nil {
		return fmt.Errorf("query metrics: %w", err)
	}
	defer rows.Close()

	var data [][]any
	var host string
	var ts time.Time
	var cpuPct, ramPct, ramFree, netSent, netRecv, diskPct, diskFree float64
	var netSentMbps, netRecvMbps, diskReadMbps, diskWriteMbps float64

	for rows.Next() {
		if err := rows.Scan(&host, &ts, &cpuPct, &ramPct, &ramFree,
			&netSent, &netRecv, &diskPct, &diskFree,
			&netSentMbps, &netRecvMbps, &diskReadMbps, &diskWriteMbps); err != nil {
			return err
		}
		data = append(data, []any{
			host, ts, cpuPct, ramPct, ramFree,
			netSent, netRecv, diskPct, diskFree,
			netSentMbps, netRecvMbps, diskReadMbps, diskWriteMbps,
		})
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if len(data) == 0 {
		return nil
	}

	_, err = pgPool.CopyFrom(ctx,
		pgx.Identifier{"metrics"},
		[]string{
			"host", "ts", "cpu_pct", "ram_pct", "ram_free",
			"net_sent", "net_recv", "disk_pct", "disk_free",
			"net_sent_mbps", "net_recv_mbps",
			"disk_read_mbps", "disk_write_mbps",
		},
		pgx.CopyFromRows(data),
	)
	if err != nil {
		return fmt.Errorf("copy metrics: %w", err)
	}

	slog.Info("migrated metrics", "count", len(data))
	return nil
}

// migrateSpeedtest migrates the speedtest table
func migrateSpeedtest(ctx context.Context, sqliteDB *sql.DB, pgPool *pgxpool.Pool) error {
	watermark, err := getMaxID(ctx, pgPool, "speedtest")
	if err != nil {
		return fmt.Errorf("get max id for speedtest: %w", err)
	}

	rows, err := sqliteDB.QueryContext(ctx, `
		SELECT host, ts, download_mbps, upload_mbps, ping_ms, server
		FROM speedtest
		WHERE id > $1
		ORDER BY id
	`, watermark)
	if err != nil {
		return fmt.Errorf("query speedtest: %w", err)
	}
	defer rows.Close()

	var data [][]any
	var host string
	var ts time.Time
	var download, upload, ping float64
	var server string

	for rows.Next() {
		if err := rows.Scan(&host, &ts, &download, &upload, &ping, &server); err != nil {
			return err
		}
		data = append(data, []any{host, ts, download, upload, ping, server})
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if len(data) == 0 {
		return nil
	}

	_, err = pgPool.CopyFrom(ctx,
		pgx.Identifier{"speedtest"},
		[]string{"host", "ts", "download_mbps", "upload_mbps", "ping_ms", "server"},
		pgx.CopyFromRows(data),
	)
	if err != nil {
		return fmt.Errorf("copy speedtest: %w", err)
	}

	slog.Info("migrated speedtest", "count", len(data))
	return nil
}

// migrateRules migrates the rules table
func migrateRules(ctx context.Context, sqliteDB *sql.DB, pgPool *pgxpool.Pool) error {
	watermark, err := getMaxID(ctx, pgPool, "rules")
	if err != nil {
		return fmt.Errorf("get max id for rules: %w", err)
	}

	rows, err := sqliteDB.QueryContext(ctx, `
		SELECT host, metric, threshold, count, enabled
		FROM rules
		WHERE id > $1
		ORDER BY id
	`, watermark)
	if err != nil {
		return fmt.Errorf("query rules: %w", err)
	}
	defer rows.Close()

	var data [][]any
	var host string
	var metric string
	var threshold float64
	var count int
	var enabledInt int

	for rows.Next() {
		if err := rows.Scan(&host, &metric, &threshold, &count, &enabledInt); err != nil {
			return err
		}
		data = append(data, []any{host, metric, threshold, count, enabledInt == 1})
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if len(data) == 0 {
		return nil
	}

	_, err = pgPool.CopyFrom(ctx,
		pgx.Identifier{"rules"},
		[]string{"host", "metric", "threshold", "count", "enabled"},
		pgx.CopyFromRows(data),
	)
	if err != nil {
		return fmt.Errorf("copy rules: %w", err)
	}

	slog.Info("migrated rules", "count", len(data))
	return nil
}

// migrateMutes migrates the mutes table
func migrateMutes(ctx context.Context, sqliteDB *sql.DB, pgPool *pgxpool.Pool) error {
	_, err := pgPool.Exec(ctx, "TRUNCATE TABLE mutes")
	if err != nil {
		return fmt.Errorf("truncate mutes: %w", err)
	}

	rows, err := sqliteDB.QueryContext(ctx, `SELECT host, muted_until FROM mutes`)
	if err != nil {
		return fmt.Errorf("query mutes: %w", err)
	}
	defer rows.Close()

	var data [][]any
	var host string
	var mutedUntil time.Time

	for rows.Next() {
		if err := rows.Scan(&host, &mutedUntil); err != nil {
			return err
		}
		data = append(data, []any{host, mutedUntil})
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if len(data) == 0 {
		return nil
	}

	_, err = pgPool.CopyFrom(ctx,
		pgx.Identifier{"mutes"},
		[]string{"host", "muted_until"},
		pgx.CopyFromRows(data),
	)
	if err != nil {
		return fmt.Errorf("copy mutes: %w", err)
	}

	slog.Info("migrated mutes", "count", len(data))
	return nil
}

// migrateIncidents migrates the incidents table
func migrateIncidents(ctx context.Context, sqliteDB *sql.DB, pgPool *pgxpool.Pool) error {
	watermark, err := getMaxID(ctx, pgPool, "incidents")
	if err != nil {
		return fmt.Errorf("get max id for incidents: %w", err)
	}

	rows, err := sqliteDB.QueryContext(ctx, `
		SELECT host, metric, value, threshold, started_at, resolved_at, notified
		FROM incidents
		WHERE id > $1
		ORDER BY id
	`, watermark)
	if err != nil {
		return fmt.Errorf("query incidents: %w", err)
	}
	defer rows.Close()

	var data [][]any
	var host string
	var metric string
	var value float64
	var threshold float64
	var startedAt time.Time
	var resolvedAt sql.NullTime
	var notifiedInt int

	for rows.Next() {
		if err := rows.Scan(&host, &metric, &value, &threshold, &startedAt, &resolvedAt, &notifiedInt); err != nil {
			return err
		}

		var rTime any
		if resolvedAt.Valid {
			rTime = resolvedAt.Time
		}

		data = append(data, []any{
			host, metric, value, threshold, startedAt,
			rTime, notifiedInt == 1,
		})
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if len(data) == 0 {
		return nil
	}

	_, err = pgPool.CopyFrom(ctx,
		pgx.Identifier{"incidents"},
		[]string{"host", "metric", "value", "threshold", "started_at", "resolved_at", "notified"},
		pgx.CopyFromRows(data),
	)
	if err != nil {
		return fmt.Errorf("copy incidents: %w", err)
	}

	slog.Info("migrated incidents", "count", len(data))
	return nil
}
