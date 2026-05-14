package collector

import (
	"log/slog"
	"math"
	"runtime"
	"sort"
	"time"

	"sentinel/internal/models"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/mem"
	netstat "github.com/shirou/gopsutil/v3/net"
	"github.com/shirou/gopsutil/v3/process"
)

var (
	prevSent    uint64
	prevRecv    uint64
	prevNetTime time.Time

	prevDiskRead  uint64
	prevDiskWrite uint64
	prevDiskTime  time.Time
)

func netSpeed() (sentMBps, recvMBps float64) {
	ifaces, err := netstat.IOCounters(true)
	if err != nil || len(ifaces) == 0 {
		return 0, 0
	}
	var totalSent, totalRecv uint64
	for _, iface := range ifaces {
		if iface.Name == "lo" || iface.Name == "Loopback Pseudo-Interface 1" {
			continue
		}
		totalSent += iface.BytesSent
		totalRecv += iface.BytesRecv
	}
	now := time.Now()
	if !prevNetTime.IsZero() && totalSent >= prevSent && totalRecv >= prevRecv {
		elapsed := now.Sub(prevNetTime).Seconds()
		if elapsed > 0 {
			sentMBps = float64(totalSent-prevSent) / elapsed / 1024 / 1024
			recvMBps = float64(totalRecv-prevRecv) / elapsed / 1024 / 1024
			if math.IsNaN(sentMBps) {
				sentMBps = 0
			}
			if math.IsNaN(recvMBps) {
				recvMBps = 0
			}
		}
	}
	prevSent = totalSent
	prevRecv = totalRecv
	prevNetTime = now
	return
}

func diskSpeed() (readMBps, writeMBps float64) {
	counters, err := disk.IOCounters()
	if err != nil || len(counters) == 0 {
		return 0, 0
	}

	var totalRead, totalWrite uint64
	for _, c := range counters {
		totalRead += c.ReadBytes
		totalWrite += c.WriteBytes
	}

	now := time.Now()
	if !prevDiskTime.IsZero() && totalRead >= prevDiskRead && totalWrite >= prevDiskWrite {
		elapsed := now.Sub(prevDiskTime).Seconds()
		if elapsed > 0 {
			readMBps = float64(totalRead-prevDiskRead) / elapsed / 1024 / 1024
			writeMBps = float64(totalWrite-prevDiskWrite) / elapsed / 1024 / 1024
			if math.IsNaN(readMBps) {
				readMBps = 0
			}
			if math.IsNaN(writeMBps) {
				writeMBps = 0
			}
		}
	}
	prevDiskRead = totalRead
	prevDiskWrite = totalWrite
	prevDiskTime = now
	return
}

// collectDisks возвращает список дисков с метриками каждого.
// Также возвращает агрегированные usedPct и freeGB для обратной совместимости.
func collectDisks() (disks []models.DiskInfo, usedPct, freeGB float64) {
	var totalSize, totalFree uint64

	if runtime.GOOS == "windows" {
		for _, letter := range []string{"C:", "D:", "E:", "F:", "G:", "H:"} {
			stat, err := disk.Usage(letter + "\\")
			if err != nil || stat == nil || stat.Total == 0 {
				continue
			}
			disks = append(disks, models.DiskInfo{
				Mount:   letter,
				UsedPct: stat.UsedPercent,
				FreeGB:  float64(stat.Free) / 1024 / 1024 / 1024,
				TotalGB: float64(stat.Total) / 1024 / 1024 / 1024,
			})
			totalSize += stat.Total
			totalFree += stat.Free
		}
	} else {
		partitions, err := disk.Partitions(false)
		if err == nil {
			for _, p := range partitions {
				if p.Fstype == "tmpfs" || p.Fstype == "devtmpfs" ||
					p.Fstype == "sysfs" || p.Fstype == "proc" ||
					p.Fstype == "devpts" || p.Fstype == "cgroup" ||
					p.Fstype == "cgroup2" || p.Fstype == "overlay" {
					continue
				}
				stat, err := disk.Usage(p.Mountpoint)
				if err != nil || stat == nil || stat.Total == 0 {
					continue
				}
				disks = append(disks, models.DiskInfo{
					Mount:   p.Mountpoint,
					UsedPct: stat.UsedPercent,
					FreeGB:  float64(stat.Free) / 1024 / 1024 / 1024,
					TotalGB: float64(stat.Total) / 1024 / 1024 / 1024,
				})
				totalSize += stat.Total
				totalFree += stat.Free
			}
		}
		// Fallback
		if totalSize == 0 {
			if stat, err := disk.Usage("/"); err == nil && stat != nil {
				disks = append(disks, models.DiskInfo{
					Mount:   "/",
					UsedPct: stat.UsedPercent,
					FreeGB:  float64(stat.Free) / 1024 / 1024 / 1024,
					TotalGB: float64(stat.Total) / 1024 / 1024 / 1024,
				})
				totalSize = stat.Total
				totalFree = stat.Free
			}
		}
	}

	if totalSize > 0 {
		usedPct = float64(totalSize-totalFree) / float64(totalSize) * 100
		freeGB = float64(totalFree) / 1024 / 1024 / 1024
		if math.IsNaN(usedPct) {
			usedPct = 0
		}
		if math.IsNaN(freeGB) {
			freeGB = 0
		}
	}
	return
}

// collectTopProcesses возвращает топ-5 процессов по CPU.
func collectTopProcesses() []models.ProcessInfo {
	procs, err := process.Processes()
	if err != nil {
		slog.Debug("ошибка получения процессов", "err", err)
		return nil
	}

	type procData struct {
		pid    int32
		name   string
		cpuPct float64
		memMB  float64
		memPct float64
	}

	var data []procData
	for _, p := range procs {
		name, err := p.Name()
		if err != nil || name == "" {
			continue
		}

		cpuPct, err := p.CPUPercent()
		if err != nil {
			continue
		}

		memInfo, err := p.MemoryInfo()
		if err != nil {
			continue
		}

		memPct, _ := p.MemoryPercent()

		memMB := float64(memInfo.RSS) / 1024 / 1024

		if cpuPct > 0 || memMB > 50 {
			data = append(data, procData{
				pid:    p.Pid,
				name:   name,
				cpuPct: cpuPct,
				memMB:  memMB,
				memPct: float64(memPct),
			})
		}
	}

	// Сортируем по CPU (убывание)
	sort.Slice(data, func(i, j int) bool {
		return data[i].cpuPct > data[j].cpuPct
	})

	// Берём топ-5
	limit := 5
	if len(data) < limit {
		limit = len(data)
	}

	result := make([]models.ProcessInfo, limit)
	for i := 0; i < limit; i++ {
		result[i] = models.ProcessInfo{
			PID:    data[i].pid,
			Name:   data[i].name,
			CPUPct: data[i].cpuPct,
			MemMB:  data[i].memMB,
			MemPct: data[i].memPct,
		}
	}
	return result
}

// collectTemperatures собирает температуру датчиков (best-effort).
func collectTemperatures() []models.TemperatureInfo {
	temps, err := host.SensorsTemperatures()
	if err != nil || len(temps) == 0 {
		return nil
	}

	var result []models.TemperatureInfo
	for _, t := range temps {
		if t.Temperature > 0 && t.Temperature < 150 {
			result = append(result, models.TemperatureInfo{
				Label:   t.SensorKey,
				Current: t.Temperature,
			})
		}
	}
	return result
}

func Collect(host string) models.MetricEvent {
	cpuPct, _ := cpu.Percent(1*time.Second, false)
	memory, _ := mem.VirtualMemory()

	disks, diskUsedPct, diskFreeGB := collectDisks()

	sentMBps, recvMBps := netSpeed()
	readMBps, writeMBps := diskSpeed()

	var cpuVal float64
	if len(cpuPct) > 0 {
		if !math.IsNaN(cpuPct[0]) {
			cpuVal = cpuPct[0]
		}
	}

	var ramUsed, ramFreeGB float64
	if memory != nil {
		if !math.IsNaN(memory.UsedPercent) {
			ramUsed = memory.UsedPercent
		}
		ramFreeGB = float64(memory.Available) / 1024 / 1024 / 1024
	}

	topProcs := collectTopProcesses()
	temps := collectTemperatures()

	return models.MetricEvent{
		Host:          host,
		Time:          time.Now(),
		CPU:           cpuVal,
		RAMUsed:       ramUsed,
		RAMFreeGB:     ramFreeGB,
		NetSentMBps:   sentMBps,
		NetRecvMBps:   recvMBps,
		DiskUsed:      diskUsedPct,
		DiskFreeGB:    diskFreeGB,
		DiskReadMBps:  readMBps,
		DiskWriteMBps: writeMBps,
		Disks:         disks,
		TopProcesses:  topProcs,
		Temperatures:  temps,
	}
}
