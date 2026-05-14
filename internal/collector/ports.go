package collector

import (
	"fmt"
	"net"
	"strings"
	"time"

	"sentinel/internal/models"
)

// PortConfig описывает порт для мониторинга.
type PortConfig struct {
	Port int
	Name string
}

// ParsePortsConfig парсит строку вида "80:HTTP,443:HTTPS,5432:PostgreSQL".
func ParsePortsConfig(config string) []PortConfig {
	if config == "" {
		return nil
	}

	var ports []PortConfig
	for _, entry := range strings.Split(config, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		parts := strings.SplitN(entry, ":", 2)
		if len(parts) == 0 {
			continue
		}

		var port int
		if _, err := fmt.Sscanf(parts[0], "%d", &port); err != nil || port <= 0 || port > 65535 {
			continue
		}

		name := fmt.Sprintf("Port %d", port)
		if len(parts) == 2 && parts[1] != "" {
			name = strings.TrimSpace(parts[1])
		}

		ports = append(ports, PortConfig{Port: port, Name: name})
	}
	return ports
}

// CheckPorts проверяет доступность TCP-портов на localhost.
func CheckPorts(configs []PortConfig) []models.PortStatus {
	if len(configs) == 0 {
		return nil
	}

	results := make([]models.PortStatus, 0, len(configs))
	for _, cfg := range configs {
		status := models.PortStatus{
			Port: cfg.Port,
			Name: cfg.Name,
		}

		addr := fmt.Sprintf("127.0.0.1:%d", cfg.Port)
		start := time.Now()
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		elapsed := time.Since(start)

		if err == nil {
			status.Open = true
			status.Latency = float64(elapsed.Microseconds()) / 1000.0 // ms
			conn.Close()
		} else {
			status.Open = false
			status.Latency = 0
		}

		results = append(results, status)
	}
	return results
}
