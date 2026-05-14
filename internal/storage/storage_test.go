package storage

import (
	"context"
	"testing"
	"time"

	"sentinel/internal/models"
)

// вспомогательная функция — создаёт временную БД для каждого теста
func newTestDB(t *testing.T) Storage {
	t.Helper()
	// Используем нашу новую фабрику с контекстом
	db, err := NewStorage(context.Background(), ":memory:")
	if err != nil {
		t.Fatalf("не могу создать тестовую БД: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestSaveAndLoad(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	event := models.MetricEvent{
		Host:      "test-host",
		Time:      time.Now(),
		CPU:       45.5,
		RAMUsed:   60.0,
		RAMFreeGB: 8.0,
		DiskUsed:  75.0,
	}

	if err := db.Save(ctx, event); err != nil {
		t.Fatalf("Save вернул ошибку: %v", err)
	}

	// Вместо прямого SQL-запроса (db.conn) используем интерфейс
	events, err := db.GetMetricsHistory(ctx, "test-host", 1)
	if err != nil {
		t.Fatalf("ошибка запроса истории: %v", err)
	}

	if len(events) == 0 {
		t.Fatal("запись не найдена в БД")
	}

	lastEvent := events[len(events)-1]
	if lastEvent.Host != "test-host" {
		t.Errorf("ожидали host=test-host, получили %s", lastEvent.Host)
	}
	if lastEvent.CPU != 45.5 {
		t.Errorf("ожидали cpu=45.5, получили %f", lastEvent.CPU)
	}
}

func TestInitRules_DefaultRules(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	if err := db.InitRules(ctx); err != nil {
		t.Fatalf("InitRules вернул ошибку: %v", err)
	}

	rules, err := db.GetRules(ctx)
	if err != nil {
		t.Fatalf("GetRules вернул ошибку: %v", err)
	}

	// по умолчанию должно быть 3 правила
	if len(rules) != 3 {
		t.Errorf("ожидали 3 правила, получили %d", len(rules))
	}
}

func TestAddAndDeleteRule(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	db.InitRules(ctx)

	rule := models.Rule{
		Host:      "test-host",
		Metric:    "cpu",
		Threshold: 80.0,
		Count:     5,
		Enabled:   true,
	}

	id, err := db.AddRule(ctx, rule)
	if err != nil {
		t.Fatalf("AddRule вернул ошибку: %v", err)
	}
	if id == 0 {
		t.Error("ожидали ненулевой id")
	}

	if err := db.DeleteRule(ctx, id); err != nil {
		t.Fatalf("DeleteRule вернул ошибку: %v", err)
	}

	// проверяем что удалилось
	rules, _ := db.GetRules(ctx)
	for _, r := range rules {
		if r.ID == id {
			t.Error("правило не было удалено")
		}
	}
}

func TestUpdateRule(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	db.InitRules(ctx)

	rules, _ := db.GetRules(ctx)
	rule := rules[0]
	rule.Threshold = 99.0
	rule.Enabled = false

	if err := db.UpdateRule(ctx, rule); err != nil {
		t.Fatalf("UpdateRule вернул ошибку: %v", err)
	}

	updated, _ := db.GetRules(ctx)
	for _, r := range updated {
		if r.ID == rule.ID {
			if r.Threshold != 99.0 {
				t.Errorf("ожидали threshold=99, получили %f", r.Threshold)
			}
			if r.Enabled {
				t.Error("ожидали enabled=false")
			}
		}
	}
}

func TestIncidents(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	db.InitIncidents(ctx)

	// открываем инцидент
	id, err := db.OpenIncident(ctx, "test-host", "cpu", 95.0, 60.0, time.Now())
	if err != nil {
		t.Fatalf("OpenIncident вернул ошибку: %v", err)
	}
	if id == 0 {
		t.Error("ожидали ненулевой id инцидента")
	}

	// проверяем что инцидент активен
	incidents, err := db.GetIncidents(ctx, 24)
	if err != nil {
		t.Fatalf("GetIncidents вернул ошибку: %v", err)
	}
	if len(incidents) != 1 {
		t.Fatalf("ожидали 1 инцидент, получили %d", len(incidents))
	}
	if incidents[0].ResolvedAt != nil {
		t.Error("инцидент не должен быть закрыт")
	}

	// закрываем инцидент
	if err := db.CloseIncident(ctx, "test-host", "cpu", time.Now()); err != nil {
		t.Fatalf("CloseIncident вернул ошибку: %v", err)
	}

	// проверяем что закрылся
	incidents, _ = db.GetIncidents(ctx, 24)
	if incidents[0].ResolvedAt == nil {
		t.Error("инцидент должен быть закрыт")
	}
	if incidents[0].Duration == "активен" {
		t.Error("длительность не должна быть 'активен'")
	}
}
