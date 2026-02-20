package router

import (
	"voice_server/internal/bootstrap"
	"voice_server/internal/handlers"
	"voice_server/internal/ws"

	"github.com/gin-gonic/gin"
)

// NewRouter 注册所有路由，返回 *gin.Engine
func NewRouter(deps *bootstrap.AppDependencies) *gin.Engine {
	ginRouter := gin.New()
	ginRouter.Use(gin.Recovery())
	// TODO: 根据需要注入 gin.Logger()

	// 注册基础路由
	ginRouter.GET("/ws", func(c *gin.Context) {
		if deps == nil || deps.Engine == nil {
			c.JSON(503, gin.H{"error": "engine is not initialized"})
			return
		}
		sessionManager := deps.Engine.GetSessionManager()
		if sessionManager == nil {
			c.JSON(503, gin.H{"error": "session manager is not initialized"})
			return
		}
		ws.HandleWebSocket(c.Writer, c.Request, sessionManager)
	})
	ginRouter.GET("/health", handlers.HealthHandler(deps))
	ginRouter.GET("/stats", handlers.StatsHandler(deps))

	// 静态文件服务
	ginRouter.Static("/static", "./static")
	ginRouter.StaticFile("/", "./static/index.html")

	// 注册声纹识别路由（如果启用）
	if deps.SpeakerHandler != nil {
		deps.SpeakerHandler.RegisterRoutes(ginRouter)
	}

	return ginRouter
}
