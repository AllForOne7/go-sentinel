package models

import "time"

// Incident represents an open or resolved alert incident.
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