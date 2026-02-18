package handlers

import (
	"time"
	"voice_server/internal/bootstrap"

	"github.com/gin-gonic/gin"
)

// StatsHandler 统计信息接口（依赖注入）
func StatsHandler(deps *bootstrap.AppDependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		stats := map[string]interface{}{
			"timestamp": time.Now().Format(time.RFC3339),
		}
		if deps.Engine != nil {
			engineStats := deps.Engine.GetStats()
			stats["engine"] = engineStats
			if poolStats, ok := engineStats["pool_stats"]; ok {
				stats["vad_pool"] = poolStats
			}
		}
		if deps.SessionManager != nil {
			stats["sessions"] = deps.SessionManager.GetStats()
		}
		if deps.RateLimiter != nil {
			stats["rate_limit"] = deps.RateLimiter.GetStats()
		}
		if deps.Engine != nil && deps.Engine.HasSpeakerService() {
			stats["speaker"] = deps.Engine.GetSpeakerStats("", "")
		}
		c.JSON(200, stats)
	}
}
