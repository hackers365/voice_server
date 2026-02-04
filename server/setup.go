// Package server 提供供主程序内嵌 asr_server 时使用的入口，避免主程序直接引用 internal 包。
package server

import (
	"fmt"
	"net/http"
	"time"

	"voice_server/config"
	"voice_server/internal/bootstrap"
	"voice_server/internal/logger"
	"voice_server/internal/router"
)

// Setup 加载配置、初始化 asr_server 依赖并返回 HTTP Handler 与监听地址，供主进程内嵌时使用。
// 返回: handler, addr (如 "0.0.0.0:8080"), readTimeout, error
func Setup(configPath string) (http.Handler, string, time.Duration, error) {
	if err := config.InitConfig(configPath); err != nil {
		return nil, "", 0, err
	}
	cfg := config.GetConfig()

	logger.InitLoggerFromConfig(logger.LoggingConfig{
		Level:      cfg.Logging.Level,
		Format:     cfg.Logging.Format,
		Output:     cfg.Logging.Output,
		FilePath:   cfg.Logging.FilePath,
		MaxSize:    cfg.Logging.MaxSize,
		MaxBackups: cfg.Logging.MaxBackups,
		MaxAge:     cfg.Logging.MaxAge,
		Compress:   cfg.Logging.Compress,
	})

	deps, err := bootstrap.InitApp(cfg)
	if err != nil {
		return nil, "", 0, err
	}

	r := router.NewRouter(deps)
	handler := deps.RateLimiter.Middleware(r)
	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	readTimeout := time.Duration(cfg.Server.ReadTimeout) * time.Second
	if readTimeout <= 0 {
		readTimeout = 30 * time.Second
	}
	return handler, addr, readTimeout, nil
}
