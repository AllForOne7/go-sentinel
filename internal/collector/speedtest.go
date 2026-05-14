package collector

import (
	"fmt"
	"time"

	"sentinel/internal/models"

	"github.com/showwin/speedtest-go/speedtest"
)

func RunSpeedtest(host string) (models.SpeedtestResult, error) {
	fmt.Println("Запускаю speedtest...")

	_, err := speedtest.FetchUserInfo()
	if err != nil {
		return models.SpeedtestResult{}, fmt.Errorf("ошибка получения информации: %w", err)
	}

	serverList, err := speedtest.FetchServers()
	if err != nil {
		return models.SpeedtestResult{}, fmt.Errorf("ошибка получения серверов: %w", err)
	}

	// берём ближайший сервер (FindServer с nil ищет все серверы и сортирует по расстоянию)
	targets, err := serverList.FindServer(nil)
	if err != nil || len(targets) == 0 {
		return models.SpeedtestResult{}, fmt.Errorf("нет доступных серверов")
	}

	server := targets[0]

	// пинг с callback для получения результата
	err = server.PingTest(func(latency time.Duration) {})
	if err != nil {
		return models.SpeedtestResult{}, fmt.Errorf("ошибка пинга: %w", err)
	}

	// скорость загрузки
	err = server.DownloadTest()
	if err != nil {
		return models.SpeedtestResult{}, fmt.Errorf("ошибка теста загрузки: %w", err)
	}

	// скорость отдачи
	err = server.UploadTest()
	if err != nil {
		return models.SpeedtestResult{}, fmt.Errorf("ошибка теста отдачи: %w", err)
	}

	result := models.SpeedtestResult{
		Host:         host,
		Time:         time.Now(),
		DownloadMbps: server.DLSpeed.Mbps(),
		UploadMbps:   server.ULSpeed.Mbps(),
		PingMs:       float64(server.Latency.Milliseconds()),
		Server:       server.Name + ", " + server.Country,
	}

	fmt.Printf("Speedtest готов: ↓%.1f Mbps  ↑%.1f Mbps  ping: %.0f ms  сервер: %s\n",
		result.DownloadMbps, result.UploadMbps, result.PingMs, result.Server)

	return result, nil
}
