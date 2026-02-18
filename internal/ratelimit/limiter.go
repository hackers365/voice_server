package ratelimit

import (
	"errors"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// RateLimiter 速率限制器（引擎级，独立于通讯协议实现）。
type RateLimiter struct {
	enabled   bool
	limiters  map[string]*rate.Limiter
	mu        sync.RWMutex
	r         rate.Limit
	b         int
	cleanupMu sync.Once
	maxConns  int
	connCount int32
	connMu    sync.Mutex
}

var ErrRateLimitExceeded = errors.New("rate limit exceeded")
var ErrTooManyConnections = errors.New("too many connections")

// NewRateLimiter 创建新的速率限制器。
func NewRateLimiter(enabled bool, requestsPerSecond int, burstSize int, maxConnections int) *RateLimiter {
	return &RateLimiter{
		enabled:  enabled,
		limiters: make(map[string]*rate.Limiter),
		r:        rate.Limit(requestsPerSecond),
		b:        burstSize,
		maxConns: maxConnections,
	}
}

// getLimiter 获取或创建业务 key 的限流器。
func (rl *RateLimiter) getLimiter(key string) *rate.Limiter {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	limiter, exists := rl.limiters[key]
	if !exists {
		limiter = rate.NewLimiter(rl.r, rl.b)
		rl.limiters[key] = limiter
	}

	return limiter
}

// cleanupLimiters 清理过期的限制器。
func (rl *RateLimiter) cleanupLimiters() {
	ticker := time.NewTicker(time.Minute)
	go func() {
		for range ticker.C {
			rl.mu.Lock()
			for key, limiter := range rl.limiters {
				if limiter.Allow() {
					// 如果限制器允许请求，说明可能长时间未使用，删除它
					delete(rl.limiters, key)
				}
			}
			rl.mu.Unlock()
		}
	}()
}

func (rl *RateLimiter) ensureCleanupStarted() {
	rl.cleanupMu.Do(func() {
		rl.cleanupLimiters()
	})
}

// AcquireConnection 占用一个连接配额，用于长连接场景（如 WS）。
func (rl *RateLimiter) AcquireConnection() error {
	if rl == nil || !rl.enabled {
		return nil
	}
	if rl.maxConns <= 0 {
		return nil
	}

	rl.connMu.Lock()
	defer rl.connMu.Unlock()
	if rl.connCount >= int32(rl.maxConns) {
		return ErrTooManyConnections
	}
	rl.connCount++
	return nil
}

// ReleaseConnection 释放一个连接配额。
func (rl *RateLimiter) ReleaseConnection() {
	if rl == nil || !rl.enabled {
		return
	}

	rl.connMu.Lock()
	if rl.connCount > 0 {
		rl.connCount--
	}
	rl.connMu.Unlock()
}

// Allow 使用业务 key 检查速率限制（不处理连接数上限）。
func (rl *RateLimiter) Allow(key string) error {
	if rl == nil || !rl.enabled {
		return nil
	}
	rl.ensureCleanupStarted()

	normalized := strings.TrimSpace(key)
	if normalized == "" {
		normalized = "global"
	}

	limiter := rl.getLimiter(normalized)
	if !limiter.Allow() {
		return ErrRateLimitExceeded
	}
	return nil
}

// GetStats 获取统计信息。
func (rl *RateLimiter) GetStats() map[string]interface{} {
	rl.connMu.Lock()
	currentConns := rl.connCount
	rl.connMu.Unlock()

	rl.mu.RLock()
	activeLimiters := len(rl.limiters)
	rl.mu.RUnlock()

	return map[string]interface{}{
		"enabled":             rl.enabled,
		"active_limiters":     activeLimiters,
		"current_connections": currentConns,
		"max_connections":     rl.maxConns,
		"requests_per_second": float64(rl.r),
		"burst_size":          rl.b,
	}
}
