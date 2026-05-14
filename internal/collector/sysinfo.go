package collector

import (
	"fmt"
	"runtime"
	"strings"

	"sentinel/internal/models"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/mem"
)

// CollectSystemInfo собирает статическую информацию о системе.
func CollectSystemInfo() *models.SystemInfo {
	info := &models.SystemInfo{}

	// ОС и hostname
	if h, err := host.Info(); err == nil {
		info.Hostname = h.Hostname
		info.Uptime = h.Uptime

		switch runtime.GOOS {
		case "windows":
			info.OS = fmt.Sprintf("%s %s (Build %s)", h.Platform, h.PlatformVersion, h.KernelVersion)
		case "linux":
			info.OS = fmt.Sprintf("%s %s", h.Platform, h.PlatformVersion)
		case "darwin":
			info.OS = fmt.Sprintf("macOS %s", h.PlatformVersion)
		default:
			info.OS = fmt.Sprintf("%s %s", h.OS, h.PlatformVersion)
		}
	}

	// CPU модель и количество ядер
	if cpuInfo, err := cpu.Info(); err == nil && len(cpuInfo) > 0 {
		info.CPUModel = strings.TrimSpace(cpuInfo[0].ModelName)
		// Считаем общее количество ядер
		totalCores := 0
		for _, c := range cpuInfo {
			totalCores += int(c.Cores)
		}
		info.CPUCores = totalCores
	}

	// Если ядра не определились — используем логические процессоры
	if info.CPUCores == 0 {
		if count, err := cpu.Counts(true); err == nil {
			info.CPUCores = count
		}
	}

	// Общий объём RAM
	if m, err := mem.VirtualMemory(); err == nil && m != nil {
		info.TotalRAM = float64(m.Total) / 1024 / 1024 / 1024
	}

	return info
}

// UpdateUptime обновляет только uptime в существующей SystemInfo.
func UpdateUptime(si *models.SystemInfo) {
	if h, err := host.Info(); err == nil {
		si.Uptime = h.Uptime
	}
}
