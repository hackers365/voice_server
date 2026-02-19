package handlers

import (
	"voice_server/internal/bootstrap"

	"github.com/gin-gonic/gin"
)

// StatsHandler 统计信息接口（依赖注入）
func StatsHandler(deps *bootstrap.AppDependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		if deps.Engine == nil {
			c.JSON(200, map[string]interface{}{
				"status": "not_initialized",
			})
			return
		}
		c.JSON(200, deps.Engine.GetStats())
	}
}
