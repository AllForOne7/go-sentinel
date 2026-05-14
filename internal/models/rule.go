package models

type Rule struct {
	ID        int64   `json:"id"`
	Host      string  `json:"host"`
	Metric    string  `json:"metric"`    // cpu, ram, disk
	Threshold float64 `json:"threshold"` // порог %
	Count     int     `json:"count"`     // замеров подряд
	Enabled   bool    `json:"enabled"`
}
