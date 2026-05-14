package models

import "time"

// DiskInfo содержит метрики одного диска/раздела.
type DiskInfo struct {
	Mount   string  `json:"mount"`    // точка монтирования или буква диска (C:, D:, /)
	UsedPct float64 `json:"used_pct"` // использовано %
	FreeGB  float64 `json:"free_gb"`  // свободно GB
	TotalGB float64 `json:"total_gb"` // всего GB
}

// ProcessInfo содержит информацию о процессе (Top N).
type ProcessInfo struct {
	PID    int32   `json:"pid"`
	Name   string  `json:"name"`
	CPUPct float64 `json:"cpu_pct"`
	MemMB  float64 `json:"mem_mb"`
	MemPct float64 `json:"mem_pct"`
}

// TemperatureInfo содержит данные о температуре датчика.
type TemperatureInfo struct {
	Label   string  `json:"label"`   // "CPU Package", "GPU Core"
	Current float64 `json:"current"` // градусы Цельсия
}

// SystemInfo содержит статическую информацию о системе.
type SystemInfo struct {
	OS       string  `json:"os"`        // "Windows 11 Pro 23H2"
	Hostname string  `json:"hostname"`  // реальное имя компьютера
	CPUModel string  `json:"cpu_model"` // "Intel Core i7-12700K"
	CPUCores int     `json:"cpu_cores"` // 12
	TotalRAM float64 `json:"total_ram"` // GB
	Uptime   uint64  `json:"uptime"`    // секунды с загрузки
}

// PortStatus содержит результат проверки TCP-порта.
type PortStatus struct {
	Port    int     `json:"port"`
	Name    string  `json:"name"` // "HTTP", "PostgreSQL"
	Open    bool    `json:"open"`
	Latency float64 `json:"latency"` // ms
}

type MetricEvent struct {
	Host          string     `json:"host"`
	Time          time.Time  `json:"time"`
	CPU           float64    `json:"cpu_pct"`
	RAMUsed       float64    `json:"ram_used_pct"`
	RAMFreeGB     float64    `json:"ram_free_gb"`
	NetSentMBps   float64    `json:"net_sent_mbps"`
	NetRecvMBps   float64    `json:"net_recv_mbps"`
	DiskUsed      float64    `json:"disk_used_pct"` // агрегированный % (для совместимости)
	DiskFreeGB    float64    `json:"disk_free_gb"`  // агрегированный GB (для совместимости)
	DiskReadMBps  float64    `json:"disk_read_mbps"`
	DiskWriteMBps float64    `json:"disk_write_mbps"`
	Disks         []DiskInfo `json:"disks,omitempty"` // детальная информация по каждому диску

	// Новые метрики
	TopProcesses []ProcessInfo     `json:"top_processes,omitempty"`
	Temperatures []TemperatureInfo `json:"temperatures,omitempty"`
	SysInfo      *SystemInfo       `json:"sys_info,omitempty"`
	Ports        []PortStatus      `json:"ports,omitempty"`
}

type SpeedtestResult struct {
	Host         string    `json:"host"`
	Time         time.Time `json:"time"`
	DownloadMbps float64   `json:"download_mbps"`
	UploadMbps   float64   `json:"upload_mbps"`
	PingMs       float64   `json:"ping_ms"`
	Server       string    `json:"server"`
}
