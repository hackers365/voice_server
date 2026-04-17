package handlers

import (
	"net/http"
	"time"
	"voice_server/internal/bootstrap"

	"github.com/gin-gonic/gin"
)

func defaultHealthComponents() map[string]interface{} {
	return map[string]interface{}{
		"vad_pool":   map[string]interface{}{"status": "not_initialized"},
		"sessions":   map[string]interface{}{"status": "not_initialized"},
		"rate_limit": map[string]interface{}{"status": "not_initialized"},
		"speaker":    map[string]interface{}{"status": "disabled"},
	}
}

func notInitializedHealth() map[string]interface{} {
	return map[string]interface{}{
		"status":     "not_initialized",
		"timestamp":  time.Now().Format(time.RFC3339),
		"components": defaultHealthComponents(),
	}
}

// HealthHandler 健康检查接口（依赖注入）
func HealthHandler(deps *bootstrap.AppDependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		if deps.Engine == nil {
			c.JSON(http.StatusServiceUnavailable, notInitializedHealth())
			return
		}

		health := deps.Engine.GetHealth()
		if health == nil {
			health = notInitializedHealth()
		}

		statusCode := http.StatusOK
		if status, ok := health["status"].(string); ok && (status == "initializing" || status == "not_initialized") {
			statusCode = http.StatusServiceUnavailable
		}
		c.JSON(statusCode, health)
	}
}
