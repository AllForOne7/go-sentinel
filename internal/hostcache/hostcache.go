// Package hostcache предоставляет потокобезопасное хранилище последних метрик хостов.
package hostcache

import (
	"sync"
	"time"

	"sentinel/internal/models"
)

// Cache хранит последние метрики каждого хоста и время последнего обновления.
type Cache struct {
	mu       sync.RWMutex
	hosts    map[string]models.MetricEvent
	lastSeen map[string]time.Time
}

// New создаёт новый пустой кэш хостов.
func New() *Cache {
	return &Cache{
		hosts:    make(map[string]models.MetricEvent),
		lastSeen: make(map[string]time.Time),
	}
}

// Set обновляет метрики хоста и время последнего обновления.
func (c *Cache) Set(e models.MetricEvent) {
	c.mu.Lock()
	c.hosts[e.Host] = e
	c.lastSeen[e.Host] = time.Now()
	c.mu.Unlock()
}

// Get возвращает последние метрики хоста. Второй аргумент false если хост не найден.
func (c *Cache) Get(host string) (models.MetricEvent, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.hosts[host]
	return e, ok
}

// GetAll возвращает срез последних метрик всех хостов.
func (c *Cache) GetAll() []models.MetricEvent {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]models.MetricEvent, 0, len(c.hosts))
	for _, e := range c.hosts {
		result = append(result, e)
	}
	return result
}

// IsOnline возвращает true если от хоста получали данные менее 30 секунд назад.
func (c *Cache) IsOnline(host string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	t, ok := c.lastSeen[host]
	if !ok {
		return false
	}
	return time.Since(t) < 30*time.Second
}

// LastSeen возвращает время последнего обновления хоста.
func (c *Cache) LastSeen(host string) (time.Time, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	t, ok := c.lastSeen[host]
	return t, ok
}
