package handlers

import (
	"time"
	"voice_server/internal/bootstrap"

	"github.com/gin-gonic/gin"
)

// HealthHandler 健康检查接口（依赖注入）
func HealthHandler(deps *bootstrap.AppDependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		components := make(map[string]interface{})

		if deps.Engine != nil {
			engineStats := deps.Engine.GetStats()
			components["engine"] = engineStats
			if poolStats, ok := engineStats["pool_stats"]; ok {
				components["vad_pool"] = poolStats
			} else {
				components["vad_pool"] = map[string]interface{}{"status": "not_initialized"}
			}
		} else {
			components["engine"] = map[string]interface{}{"status": "not_initialized"}
			components["vad_pool"] = map[string]interface{}{"status": "not_initialized"}
		}
		if deps.SessionManager != nil {
			components["sessions"] = deps.SessionManager.GetStats()
		} else {
			components["sessions"] = map[string]interface{}{"status": "not_initialized"}
		}
		if deps.RateLimiter != nil {
			components["rate_limit"] = deps.RateLimiter.GetStats()
		} else {
			components["rate_limit"] = map[string]interface{}{"status": "not_initialized"}
		}
		if deps.Engine != nil && deps.Engine.HasSpeakerService() {
			components["speaker"] = deps.Engine.GetSpeakerStats("", "") // 传入空字符串获取全局统计
		} else {
			components["speaker"] = map[string]interface{}{"status": "disabled"}
		}

		status := "healthy"
		if deps.Engine == nil || deps.SessionManager == nil || deps.RateLimiter == nil {
			status = "initializing"
			c.Status(503)
		}

		health := map[string]interface{}{
			"status":     status,
			"timestamp":  time.Now().Format(time.RFC3339),
			"components": components,
		}
		c.JSON(200, health)
	}
}
