package main

import (
	"testing"

	"sentinel/internal/models"
)

func TestGetRuleThreshold_Found(t *testing.T) {
	rules := []models.Rule{
		{Metric: "cpu", Threshold: 75.0, Count: 5, Enabled: true},
		{Metric: "ram", Threshold: 85.0, Count: 1, Enabled: true},
	}

	threshold, count, enabled := getRuleThreshold(rules, "cpu")

	if threshold != 75.0 {
		t.Errorf("ожидали threshold=75, получили %f", threshold)
	}
	if count != 5 {
		t.Errorf("ожидали count=5, получили %d", count)
	}
	if !enabled {
		t.Error("ожидали enabled=true")
	}
}

func TestGetRuleThreshold_Default(t *testing.T) {
	rules := []models.Rule{}

	threshold, count, enabled := getRuleThreshold(rules, "cpu")

	if threshold != 60.0 {
		t.Errorf("ожидали дефолтный threshold=60, получили %f", threshold)
	}
	if count != 3 {
		t.Errorf("ожидали дефолтный count=3, получили %d", count)
	}
	if !enabled {
		t.Error("ожидали enabled=true по умолчанию")
	}
}

func TestGetRuleThreshold_Disabled(t *testing.T) {
	rules := []models.Rule{
		{Metric: "cpu", Threshold: 60.0, Count: 3, Enabled: false},
	}

	_, _, enabled := getRuleThreshold(rules, "cpu")

	if enabled {
		t.Error("правило должно быть выключено")
	}
}

func TestFmtFloat(t *testing.T) {
	cases := []struct {
		input    float64
		expected string
	}{
		{45.5678, "45.6"},
		{0.0, "0.0"},
		{100.0, "100.0"},
		{3.14159, "3.1"},
	}

	for _, c := range cases {
		result := fmtFloat(c.input)
		if result != c.expected {
			t.Errorf("fmtFloat(%f) = %s, ожидали %s", c.input, result, c.expected)
		}
	}
}

func TestAlertState_CPUCounter(t *testing.T) {
	e := models.MetricEvent{
		Host:     "test",
		CPU:      90.0,
		RAMUsed:  50.0,
		DiskUsed: 50.0,
	}

	rules := []models.Rule{
		{Metric: "cpu", Threshold: 60.0, Count: 3, Enabled: true},
		{Metric: "ram", Threshold: 90.0, Count: 1, Enabled: true},
		{Metric: "disk", Threshold: 95.0, Count: 1, Enabled: true},
	}

	// сбрасываем состояние перед тестом
	states = map[string]*alertState{}

	// первый вызов — счётчик 1
	check(e, "fake-token", "fake-chat", rules, nil)
	if states["test"].cpuHighCount != 1 {
		t.Errorf("ожидали cpuHighCount=1, получили %d", states["test"].cpuHighCount)
	}

	// второй вызов — счётчик 2
	check(e, "fake-token", "fake-chat", rules, nil)
	if states["test"].cpuHighCount != 2 {
		t.Errorf("ожидали cpuHighCount=2, получили %d", states["test"].cpuHighCount)
	}

	// CPU в норме — счётчик сбрасывается
	e.CPU = 10.0
	check(e, "fake-token", "fake-chat", rules, nil)
	if states["test"].cpuHighCount != 0 {
		t.Errorf("ожидали cpuHighCount=0 после сброса, получили %d", states["test"].cpuHighCount)
	}
}
