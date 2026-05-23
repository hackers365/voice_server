package pool

import (
	"fmt"

	"voice_server/config"
	"voice_server/internal/logger"

	sherpa "github.com/hackers365/sherpa-onnx-go/sherpa_onnx"
)

// VADFactory VAD工厂
type VADFactory struct {
	factories map[string]VADPoolFactory
}

// NewVADFactory 创建新的VAD工厂
func NewVADFactory() *VADFactory {
	factory := &VADFactory{
		factories: make(map[string]VADPoolFactory),
	}

	// 注册支持的VAD类型
	factory.RegisterFactory(SILERO_TYPE, &SileroVADPoolFactory{})
	factory.RegisterFactory(TEN_VAD_TYPE, &TenVADPoolFactory{})

	return factory
}

// RegisterFactory 注册VAD池工厂
func (f *VADFactory) RegisterFactory(vadType string, factory VADPoolFactory) {
	f.factories[vadType] = factory
	logger.Infof("🔧 Registered VAD factory for type: %s", vadType)
}

// CreateVADPool 根据配置创建VAD池
func (f *VADFactory) CreateVADPool() (VADPoolInterface, error) {
	vadType := config.GlobalConfig.VAD.Provider

	logger.Infof("🔧 Creating VAD pool with type: %s", vadType)

	factory, exists := f.factories[vadType]
	if !exists {
		return nil, fmt.Errorf("unsupported VAD type: %s", vadType)
	}

	// 根据VAD类型创建配置
	var config interface{}
	var err error

	switch vadType {
	case SILERO_TYPE:
		config, err = f.createSileroConfig()
	case TEN_VAD_TYPE:
		config, err = f.createTenVADConfig()
	default:
		return nil, fmt.Errorf("unsupported VAD type: %s", vadType)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create config for %s: %v", vadType, err)
	}

	// 使用工厂创建池
	pool, err := factory.CreatePool(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create %s VAD pool: %v", vadType, err)
	}

	return pool, nil
}

// createSileroConfig 创建Silero VAD配置
func (f *VADFactory) createSileroConfig() (*SileroVADConfig, error) {
	// 创建VAD配置
	vadConfig := &sherpa.VadModelConfig{
		SileroVad: sherpa.SileroVadModelConfig{
			Model:              config.GlobalConfig.VAD.SileroVAD.ModelPath,
			Threshold:          config.GlobalConfig.VAD.SileroVAD.Threshold,
			MinSilenceDuration: config.GlobalConfig.VAD.SileroVAD.MinSilenceDuration,
			MinSpeechDuration:  config.GlobalConfig.VAD.SileroVAD.MinSpeechDuration,
			WindowSize:         config.GlobalConfig.VAD.SileroVAD.WindowSize,
			MaxSpeechDuration:  config.GlobalConfig.VAD.SileroVAD.MaxSpeechDuration,
		},
		SampleRate: config.GlobalConfig.Audio.SampleRate,
		NumThreads: config.GlobalConfig.Recognition.NumThreads,
		Provider:   config.GlobalConfig.Recognition.Provider,
		Debug:      0,
	}

	return &SileroVADConfig{
		ModelConfig:       vadConfig,
		BufferSizeSeconds: config.GlobalConfig.VAD.SileroVAD.BufferSizeSeconds,
		PoolSize:          config.GlobalConfig.VAD.PoolSize,
		MaxIdle:           0, // 暂时不支持MaxIdle
	}, nil
}

// createTenVADConfig 创建TEN-VAD配置
func (f *VADFactory) createTenVADConfig() (*TenVADConfig, error) {
	return &TenVADConfig{
		HopSize:   config.GlobalConfig.VAD.TenVAD.HopSize,
		Threshold: config.GlobalConfig.VAD.Threshold,
		PoolSize:  config.GlobalConfig.VAD.PoolSize,
		MaxIdle:   0, // 暂时不支持MaxIdle
	}, nil
}

// GetVADType 获取当前VAD类型
func (f *VADFactory) GetVADType() string {
	return config.GlobalConfig.VAD.Provider
}

// GetSupportedTypes 获取支持的VAD类型
func (f *VADFactory) GetSupportedTypes() []string {
	types := make([]string, 0, len(f.factories))
	for vadType := range f.factories {
		types = append(types, vadType)
	}
	return types
}

// SileroVADPoolFactory Silero VAD池工厂
type SileroVADPoolFactory struct{}

// CreatePool 创建Silero VAD池
func (f *SileroVADPoolFactory) CreatePool(config interface{}) (VADPoolInterface, error) {
	sileroConfig, ok := config.(*SileroVADConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type for Silero VAD")
	}

	pool := NewSileroVADPool(sileroConfig)
	return pool, nil
}

// GetSupportedTypes 获取支持的VAD类型
func (f *SileroVADPoolFactory) GetSupportedTypes() []string {
	return []string{SILERO_TYPE}
}

// TenVADPoolFactory TEN-VAD池工厂
type TenVADPoolFactory struct{}

// CreatePool 创建TEN-VAD池
func (f *TenVADPoolFactory) CreatePool(config interface{}) (VADPoolInterface, error) {
	tenVADConfig, ok := config.(*TenVADConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type for TEN-VAD")
	}

	pool := NewTenVADPool(tenVADConfig)
	return pool, nil
}

// GetSupportedTypes 获取支持的VAD类型
func (f *TenVADPoolFactory) GetSupportedTypes() []string {
	return []string{TEN_VAD_TYPE}
}
