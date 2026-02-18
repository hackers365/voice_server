package server

import (
	"fmt"
	"strings"
	"sync"

	"voice_server/config"
	"voice_server/core"
	"voice_server/internal/bootstrap"
	"voice_server/internal/logger"
)

// EngineProvider 统一负责共享引擎的初始化与获取。
// HTTP/Embed 适配层只依赖该提供者，不直接处理创建细节。
type EngineProvider struct{}

var sharedEngineProvider = &EngineProvider{}
var (
	sharedDepsMu sync.Mutex
	sharedDeps   *bootstrap.AppDependencies
)

type configOverrideFunc func(cfg *config.Config)

// SharedEngineProvider 返回全局共享引擎提供者。
func SharedEngineProvider() *EngineProvider {
	return sharedEngineProvider
}

// InitForEmbed 显式初始化内嵌场景共享依赖（强制开启 recognition）。
func (p *EngineProvider) InitForEmbed(configPath string) error {
	return ensureSharedDependencies(configPath, func(cfg *config.Config) {
		cfg.Recognition.Enabled = true
	})
}

// InitForHTTP 显式初始化 HTTP 场景共享依赖。
func (p *EngineProvider) InitForHTTP(configPath string) error {
	return ensureSharedDependencies(configPath, nil)
}

// Engine 返回当前共享核心引擎。
// 不会触发懒初始化；调用前必须先执行 InitForEmbed/InitForHTTP。
func (p *EngineProvider) Engine() (core.Engine, error) {
	return p.currentCoreEngine()
}

// Dependencies 返回共享依赖。
// 不会触发懒初始化；调用前必须先执行 InitForEmbed/InitForHTTP。
func (p *EngineProvider) Dependencies() (*bootstrap.AppDependencies, error) {
	return p.currentDependencies()
}

// currentCoreEngine 返回带统一限流封装的核心引擎实例。
func (p *EngineProvider) currentCoreEngine() (core.Engine, error) {
	deps, err := p.currentDependencies()
	if err != nil {
		return nil, err
	}
	if deps.Engine == nil {
		return nil, fmt.Errorf("shared guarded engine is not initialized")
	}
	return deps.Engine, nil
}

// currentDependencies 返回共享依赖。
func (p *EngineProvider) currentDependencies() (*bootstrap.AppDependencies, error) {
	deps := sharedDependencies()
	if deps == nil {
		return nil, fmt.Errorf("shared dependencies are not initialized, call InitForEmbed/InitForHTTP first")
	}
	return deps, nil
}

// sharedDependencies 返回当前共享依赖（若尚未初始化则返回 nil）。
func sharedDependencies() *bootstrap.AppDependencies {
	sharedDepsMu.Lock()
	defer sharedDepsMu.Unlock()
	return sharedDeps
}

// ensureSharedDependencies 初始化并复用 asr_server 的共享依赖。
// 首次初始化后，后续调用将直接复用同一份 Engine/SessionManager/RateLimiter 等实例。
func ensureSharedDependencies(configPath string, override configOverrideFunc) error {
	sharedDepsMu.Lock()
	defer sharedDepsMu.Unlock()

	if sharedDeps != nil {
		return nil
	}

	resolvedPath, err := resolveConfigPath(configPath)
	if err != nil {
		return err
	}
	if err := config.InitConfig(resolvedPath); err != nil {
		return err
	}

	cfg := config.GetConfig()
	if override != nil {
		override(cfg)
	}

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
		return fmt.Errorf("failed to initialize app dependencies: %w", err)
	}

	sharedDeps = deps
	return nil
}

func resolveConfigPath(configPath string) (string, error) {
	trimmed := strings.TrimSpace(configPath)
	if trimmed != "" {
		return trimmed, nil
	}
	return "", fmt.Errorf("asr config path is required")
}
