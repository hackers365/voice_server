package core

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"voice_server/config"
	"voice_server/internal/logger"
	"voice_server/internal/pool"

	sherpa "github.com/k2-fsa/sherpa-onnx-go/sherpa_onnx"
)

// ResultHandler 识别结果回调。
type ResultHandler func(sessionID, result string, err error)

// SessionConn 抽象连接对象，避免核心层绑定具体传输协议实现。
type SessionConn interface {
	WriteJSON(v interface{}) error
	Close() error
}

// SessionManager 抽象会话管理能力，由引擎持有并统一管理生命周期。
type SessionManager interface {
	CreateSession(sessionID string, conn SessionConn) error
	RemoveSession(sessionID string)
	HandleAudioMessage(sessionID string, audioData []byte) error
	Shutdown()
}

// SessionManagerFactory 用于由外部注入会话管理实现，但由 Engine 负责创建和销毁。
type SessionManagerFactory func(engine Engine) SessionManager

// SpeakerIdentifyResult 表示声纹识别结果。
type SpeakerIdentifyResult struct {
	Identified  bool    `json:"identified"`
	SpeakerID   string  `json:"speaker_id"`
	SpeakerName string  `json:"speaker_name"`
	Confidence  float32 `json:"confidence"`
	Threshold   float32 `json:"threshold"`
}

// SpeakerVerifyResult 表示声纹验证结果。
type SpeakerVerifyResult struct {
	SpeakerID   string  `json:"speaker_id"`
	SpeakerName string  `json:"speaker_name"`
	Verified    bool    `json:"verified"`
	Confidence  float32 `json:"confidence"`
	Threshold   float32 `json:"threshold"`
}

// SpeakerInfo 表示已注册说话人信息。
type SpeakerInfo struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	UUID        string    `json:"uuid"`
	AgentID     string    `json:"agent_id"`
	SampleCount int       `json:"sample_count"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// SpeakerStreamingSession 抽象流式声纹识别会话。
type SpeakerStreamingSession interface {
	AcceptAudio(audioData []float32) error
	FinishAndIdentify() (*SpeakerIdentifyResult, error)
	Close()
}

// SpeakerService 抽象声纹核心能力。
type SpeakerService interface {
	RegisterSpeaker(uid, agentID, speakerID, speakerName, uuid string, audioData []float32, sampleRate int) error
	IdentifySpeaker(uid, agentID, speakerID, speakerName string, audioData []float32, sampleRate int, threshold ...float32) (*SpeakerIdentifyResult, error)
	VerifySpeaker(uid, agentID, speakerID string, audioData []float32, sampleRate int) (*SpeakerVerifyResult, error)
	DeleteSpeaker(uid, agentID, speakerID string) error
	DeleteSpeakerByUUID(uid, agentID, uuid string) error
	GetAllSpeakers(uid, agentID string) []*SpeakerInfo
	GetStats(uid, agentID string) map[string]interface{}
	NewStreamingSession(uid, agentID, speakerID, speakerName string, sampleRate int, threshold ...float32) (SpeakerStreamingSession, error)
}

// Engine 定义对外完整 ASR 能力抽象，HTTP/API 均应依赖该接口。
type Engine interface {
	SetResultHandler(handler ResultHandler)

	OpenSession(sessionID string) error
	CloseSession(sessionID string)
	ProcessAudioData(sessionID string, audioData []byte) error

	RecognizeFloat32(samples []float32, sampleRate int) (string, error)
	RecognizeWithVAD(samples []float32, sampleRate int) (string, error)

	HasSpeakerService() bool
	RegisterSpeaker(uid, agentID, speakerID, speakerName, uuid string, audioData []float32, sampleRate int) error
	IdentifySpeaker(uid, agentID, speakerID, speakerName string, audioData []float32, sampleRate int, threshold ...float32) (*SpeakerIdentifyResult, error)
	VerifySpeaker(uid, agentID, speakerID string, audioData []float32, sampleRate int) (*SpeakerVerifyResult, error)
	DeleteSpeaker(uid, agentID, speakerID string) error
	DeleteSpeakerByUUID(uid, agentID, uuid string) error
	GetAllSpeakers(uid, agentID string) []*SpeakerInfo
	GetSpeakerStats(uid, agentID string) map[string]interface{}
	NewSpeakerStreamingSession(uid, agentID, speakerID, speakerName string, sampleRate int, threshold ...float32) (SpeakerStreamingSession, error)

	GetSessionManager() SessionManager

	GetStats() map[string]interface{}
	GetHealth() map[string]interface{}
	Shutdown()
}

// engine 是 Engine 的具体实现，内部维护 ASR 核心状态（VAD 实例、会话状态、识别器）。
type engine struct {
	recognizer *sherpa.OfflineRecognizer
	vadPool    pool.VADPoolInterface
	speakerMu  sync.RWMutex
	speaker    SpeakerService
	startTime  time.Time

	sessions map[string]*sessionState
	mu       sync.RWMutex

	// legacy 模式下串行 decode；pool 模式下此锁不参与 decode。
	decodeMu sync.Mutex

	// 解码执行模式：legacy（回滚模式）或 pool（并发解码模式）。
	decodeMode string

	// pool 模式：解码任务有界队列与工作协程。
	decodeQueue   chan decodeTask
	decodeWorkers int
	decodeWG      sync.WaitGroup

	// pool 模式：识别器实例池。
	decodePool        chan *sherpa.OfflineRecognizer
	decodeRecognizers []*sherpa.OfflineRecognizer
	decodeDroppedJobs int64

	// 识别结果回调（由外层连接管理器注入）。
	resultMu     sync.RWMutex
	resultHandle ResultHandler

	// 会话管理器由引擎统一持有和销毁，具体实现通过工厂注入。
	sessionMu      sync.RWMutex
	sessionManager SessionManager

	// 关闭标记，避免 Shutdown 期间继续投递异步任务。
	stopping int32

	// 关闭幂等保护。
	shutdownOnce sync.Once

	// 统计信息
	totalSessions  int64
	activeSessions int64
	totalMessages  int64
}

type sessionState struct {
	mu sync.Mutex

	lastSeen int64

	vadInstance pool.VADInstanceInterface

	// ten-vad 会话状态
	isInSpeech        bool
	currentSegment    []float32
	silenceFrameCount int
}

type decodeTask struct {
	sessionID  string
	samples    []float32
	sampleRate int
}

const (
	decodeModeLegacy   = "legacy"
	decodeModePool     = "pool"
	defaultDecodePool  = 4
	defaultDecodeQueue = 512
)

type decodeEnqueueResult int

const (
	decodeEnqueueOK decodeEnqueueResult = iota
	decodeEnqueueFull
	decodeEnqueueClosed
)

var float32Pool = sync.Pool{}

func getFloat32PoolSlice() []float32 {
	chunkSize := config.GlobalConfig.Audio.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 4096
	}
	return make([]float32, chunkSize)
}

func buildRecognizerConfigFromGlobal() sherpa.OfflineRecognizerConfig {
	c := sherpa.OfflineRecognizerConfig{}
	c.FeatConfig.SampleRate = config.GlobalConfig.Audio.SampleRate
	c.FeatConfig.FeatureDim = config.GlobalConfig.Audio.FeatureDim
	c.ModelConfig.SenseVoice.Model = config.GlobalConfig.Recognition.ModelPath
	c.ModelConfig.Tokens = config.GlobalConfig.Recognition.TokensPath
	c.ModelConfig.NumThreads = config.GlobalConfig.Recognition.NumThreads
	if config.GlobalConfig.Recognition.Debug {
		c.ModelConfig.Debug = 1
	}
	c.ModelConfig.Provider = config.GlobalConfig.Recognition.Provider
	return c
}

func newOfflineRecognizerFromGlobal() (*sherpa.OfflineRecognizer, error) {
	cfg := buildRecognizerConfigFromGlobal()
	recognizer := sherpa.NewOfflineRecognizer(&cfg)
	if recognizer == nil {
		return nil, fmt.Errorf("failed to create offline recognizer")
	}
	return recognizer, nil
}

// NewEngine 创建核心引擎，并在构建期注入可选的声纹服务。
func NewEngine(recognizer *sherpa.OfflineRecognizer, vadPool pool.VADPoolInterface, speakerService SpeakerService) Engine {
	e := &engine{
		recognizer: recognizer,
		vadPool:    vadPool,
		speaker:    speakerService,
		startTime:  time.Now(),
		sessions:   make(map[string]*sessionState),
	}
	e.initDecodeExecutor()
	return e
}

func (e *engine) GetSessionManager() SessionManager {
	e.sessionMu.RLock()
	defer e.sessionMu.RUnlock()
	return e.sessionManager
}

func (e *engine) initDecodeExecutor() {
	mode := strings.ToLower(strings.TrimSpace(config.GlobalConfig.Recognition.DecodeMode))
	switch mode {
	case "", decodeModeLegacy:
		mode = decodeModeLegacy
	case decodeModePool:
	default:
		logger.Warnf("未知 decode_mode=%q，回退 legacy", mode)
		mode = decodeModeLegacy
	}
	e.decodeMode = mode

	if mode != decodeModePool {
		logger.Infof("ASR decode mode=legacy（可通过 recognition.decode_mode=pool 启用并发解码）")
		return
	}

	if e.recognizer == nil {
		logger.Warnf("ASR decode pool 初始化失败：recognizer 未初始化，回退 legacy")
		e.decodeMode = decodeModeLegacy
		return
	}

	poolSize := config.GlobalConfig.Recognition.DecodePoolSize
	if poolSize <= 0 {
		poolSize = defaultDecodePool
	}
	queueSize := config.GlobalConfig.Recognition.DecodeQueueSize
	if queueSize <= 0 {
		queueSize = defaultDecodeQueue
	}

	if poolSize < 2 {
		logger.Warnf("ASR decode pool_size=%d，不足以并发，回退 legacy", poolSize)
		e.decodeMode = decodeModeLegacy
		return
	}

	recognizers := make([]*sherpa.OfflineRecognizer, 0, poolSize)
	recognizers = append(recognizers, e.recognizer)
	for i := 1; i < poolSize; i++ {
		rec, err := newOfflineRecognizerFromGlobal()
		if err != nil {
			logger.Warnf("创建解码识别器实例失败（idx=%d）：%v", i, err)
			break
		}
		recognizers = append(recognizers, rec)
	}

	if len(recognizers) < 2 {
		logger.Warnf("ASR decode pool 可用实例不足，回退 legacy")
		e.decodeMode = decodeModeLegacy
		return
	}

	e.decodeRecognizers = recognizers
	e.decodePool = make(chan *sherpa.OfflineRecognizer, len(recognizers))
	for _, rec := range recognizers {
		e.decodePool <- rec
	}

	e.decodeQueue = make(chan decodeTask, queueSize)
	e.decodeWorkers = len(recognizers)
	for i := 0; i < e.decodeWorkers; i++ {
		e.decodeWG.Add(1)
		go e.decodeWorkerLoop()
	}

	logger.Infof(
		"ASR decode mode=pool（recognizers=%d, queue_size=%d）。回滚开关：recognition.decode_mode=legacy",
		len(recognizers),
		queueSize,
	)
}

func (e *engine) decodeWorkerLoop() {
	defer e.decodeWG.Done()
	for task := range e.decodeQueue {
		text, err := e.decodeWithRecognizerPool(task.samples, task.sampleRate)
		if err != nil || text != "" {
			e.emitResult(task.sessionID, text, err)
		}
	}
}

func (e *engine) enqueueDecodeTask(task decodeTask) (result decodeEnqueueResult) {
	if e.decodeQueue == nil {
		return decodeEnqueueClosed
	}
	defer func() {
		if recover() != nil {
			result = decodeEnqueueClosed
		}
	}()
	select {
	case e.decodeQueue <- task:
		return decodeEnqueueOK
	default:
		return decodeEnqueueFull
	}
}

// SetResultHandler 设置识别结果回调。
func (e *engine) SetResultHandler(handler ResultHandler) {
	e.resultMu.Lock()
	defer e.resultMu.Unlock()
	e.resultHandle = handler
}

// HasSpeakerService 返回声纹服务是否可用。
func (e *engine) HasSpeakerService() bool {
	e.speakerMu.RLock()
	defer e.speakerMu.RUnlock()
	return e.speaker != nil
}

func (e *engine) getSpeakerService() SpeakerService {
	e.speakerMu.RLock()
	defer e.speakerMu.RUnlock()
	return e.speaker
}

// RegisterSpeaker 注册声纹。
func (e *engine) RegisterSpeaker(uid, agentID, speakerID, speakerName, uuid string, audioData []float32, sampleRate int) error {
	service := e.getSpeakerService()
	if service == nil {
		return fmt.Errorf("speaker service is not initialized")
	}
	return service.RegisterSpeaker(uid, agentID, speakerID, speakerName, uuid, audioData, sampleRate)
}

// IdentifySpeaker 识别声纹。
func (e *engine) IdentifySpeaker(uid, agentID, speakerID, speakerName string, audioData []float32, sampleRate int, threshold ...float32) (*SpeakerIdentifyResult, error) {
	service := e.getSpeakerService()
	if service == nil {
		return nil, fmt.Errorf("speaker service is not initialized")
	}
	return service.IdentifySpeaker(uid, agentID, speakerID, speakerName, audioData, sampleRate, threshold...)
}

// VerifySpeaker 验证声纹。
func (e *engine) VerifySpeaker(uid, agentID, speakerID string, audioData []float32, sampleRate int) (*SpeakerVerifyResult, error) {
	service := e.getSpeakerService()
	if service == nil {
		return nil, fmt.Errorf("speaker service is not initialized")
	}
	return service.VerifySpeaker(uid, agentID, speakerID, audioData, sampleRate)
}

// DeleteSpeaker 删除整组声纹。
func (e *engine) DeleteSpeaker(uid, agentID, speakerID string) error {
	service := e.getSpeakerService()
	if service == nil {
		return fmt.Errorf("speaker service is not initialized")
	}
	return service.DeleteSpeaker(uid, agentID, speakerID)
}

// DeleteSpeakerByUUID 删除单条声纹样本。
func (e *engine) DeleteSpeakerByUUID(uid, agentID, uuid string) error {
	service := e.getSpeakerService()
	if service == nil {
		return fmt.Errorf("speaker service is not initialized")
	}
	return service.DeleteSpeakerByUUID(uid, agentID, uuid)
}

// GetAllSpeakers 获取说话人列表。
func (e *engine) GetAllSpeakers(uid, agentID string) []*SpeakerInfo {
	service := e.getSpeakerService()
	if service == nil {
		return []*SpeakerInfo{}
	}
	return service.GetAllSpeakers(uid, agentID)
}

// GetSpeakerStats 获取声纹统计信息。
func (e *engine) GetSpeakerStats(uid, agentID string) map[string]interface{} {
	service := e.getSpeakerService()
	if service == nil {
		return map[string]interface{}{"status": "disabled"}
	}
	return service.GetStats(uid, agentID)
}

// NewSpeakerStreamingSession 创建流式声纹识别会话。
func (e *engine) NewSpeakerStreamingSession(uid, agentID, speakerID, speakerName string, sampleRate int, threshold ...float32) (SpeakerStreamingSession, error) {
	service := e.getSpeakerService()
	if service == nil {
		return nil, fmt.Errorf("speaker service is not initialized")
	}
	return service.NewStreamingSession(uid, agentID, speakerID, speakerName, sampleRate, threshold...)
}

// OpenSession 注册一个会话状态。
func (e *engine) OpenSession(sessionID string) error {
	if atomic.LoadInt32(&e.stopping) == 1 {
		return fmt.Errorf("engine is shutting down")
	}
	if e.vadPool == nil {
		return fmt.Errorf("VAD pool is not initialized")
	}
	if e.recognizer == nil {
		return fmt.Errorf("recognition service is disabled")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.sessions[sessionID]; exists {
		return fmt.Errorf("session %s already exists", sessionID)
	}

	e.sessions[sessionID] = &sessionState{
		lastSeen: time.Now().UnixNano(),
	}
	atomic.AddInt64(&e.totalSessions, 1)
	atomic.AddInt64(&e.activeSessions, 1)
	return nil
}

// CloseSession 释放一个会话占用的核心资源。
func (e *engine) CloseSession(sessionID string) {
	e.mu.Lock()
	state, exists := e.sessions[sessionID]
	if exists {
		delete(e.sessions, sessionID)
	}
	e.mu.Unlock()

	if !exists {
		return
	}

	state.mu.Lock()
	vadInstance := state.vadInstance
	state.vadInstance = nil
	state.currentSegment = nil
	state.silenceFrameCount = 0
	state.isInSpeech = false
	state.mu.Unlock()

	if vadInstance != nil && e.vadPool != nil {
		e.vadPool.Put(vadInstance)
		logger.Infof("🔄 Returned VAD instance to pool for session %s", sessionID)
	}

	atomic.AddInt64(&e.activeSessions, -1)
}

// ProcessAudioData 处理一段音频并触发异步识别。
func (e *engine) ProcessAudioData(sessionID string, audioData []byte) error {
	if atomic.LoadInt32(&e.stopping) == 1 {
		return fmt.Errorf("engine is shutting down")
	}
	if e.recognizer == nil {
		return fmt.Errorf("recognizer is not initialized")
	}
	if len(audioData) == 0 {
		return fmt.Errorf("empty audio data")
	}
	if len(audioData)%2 != 0 {
		return fmt.Errorf("invalid audio data length: %d", len(audioData))
	}

	state, exists := e.getSession(sessionID)
	if !exists {
		return fmt.Errorf("session %s not found", sessionID)
	}

	atomic.AddInt64(&e.totalMessages, 1)
	atomic.StoreInt64(&state.lastSeen, time.Now().UnixNano())

	numSamples := len(audioData) / 2
	samples := float32Pool.Get()
	var float32Slice []float32
	if samples == nil {
		float32Slice = getFloat32PoolSlice()
	} else {
		float32Slice = samples.([]float32)
	}
	if cap(float32Slice) < numSamples {
		float32Slice = make([]float32, numSamples)
	}
	float32Slice = float32Slice[:numSamples]
	defer float32Pool.Put(float32Slice)

	normalizeFactor := config.GlobalConfig.Audio.NormalizeFactor
	for i := 0; i < numSamples; i++ {
		sample := int16(audioData[i*2]) | int16(audioData[i*2+1])<<8
		float32Slice[i] = float32(sample) / normalizeFactor
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	if state.vadInstance == nil {
		vadInstance, err := e.vadPool.Get()
		if err != nil {
			return fmt.Errorf("failed to get VAD instance for session %s: %v", sessionID, err)
		}
		state.vadInstance = vadInstance
		logger.Infof("✅ Session %s assigned %s VAD instance %d", sessionID, vadInstance.GetType(), vadInstance.GetID())
	}

	switch state.vadInstance.GetType() {
	case pool.SILERO_TYPE:
		return e.processSileroVAD(sessionID, state, float32Slice)
	case pool.TEN_VAD_TYPE:
		return e.processTenVAD(sessionID, state, float32Slice)
	default:
		return fmt.Errorf("unsupported VAD type: %s", state.vadInstance.GetType())
	}
}

func (e *engine) getSession(sessionID string) (*sessionState, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	state, ok := e.sessions[sessionID]
	return state, ok
}

func (e *engine) processSileroVAD(sessionID string, state *sessionState, float32Slice []float32) error {
	sileroInstance, ok := state.vadInstance.(*pool.SileroVADInstance)
	if !ok {
		return fmt.Errorf("invalid Silero VAD instance type")
	}

	vadTimeout := time.Duration(config.GlobalConfig.Response.Timeout) * time.Second
	vadCtx, cancel := context.WithTimeout(context.Background(), vadTimeout)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		sileroInstance.VAD.AcceptWaveform(float32Slice)
	}()

	select {
	case <-done:
	case <-vadCtx.Done():
		return fmt.Errorf("VAD processing timeout")
	}

	var speechSegments [][]float32
	sampleRate := config.GlobalConfig.Audio.SampleRate
	minSpeechDuration := float64(config.GlobalConfig.VAD.SileroVAD.MinSpeechDuration)
	maxDuration := float64(config.GlobalConfig.VAD.SileroVAD.MaxSpeechDuration)

	for !sileroInstance.VAD.IsEmpty() {
		segment := sileroInstance.VAD.Front()
		sileroInstance.VAD.Pop()

		if segment == nil || len(segment.Samples) == 0 {
			continue
		}

		duration := float64(len(segment.Samples)) / float64(sampleRate)
		if duration < minSpeechDuration {
			continue
		}

		segmentSamples := segment.Samples
		if maxDuration > 0 && duration > maxDuration {
			maxSamples := int(maxDuration * float64(sampleRate))
			if maxSamples > 0 && maxSamples < len(segmentSamples) {
				segmentSamples = segmentSamples[:maxSamples]
			}
		}

		segmentCopy := append([]float32(nil), segmentSamples...)
		speechSegments = append(speechSegments, segmentCopy)
	}

	for _, samples := range speechSegments {
		e.decodeAsync(sessionID, samples, sampleRate)
	}
	return nil
}

func (e *engine) processTenVAD(sessionID string, state *sessionState, float32Slice []float32) error {
	tenVADInstance, ok := state.vadInstance.(*pool.TenVADInstance)
	if !ok {
		return fmt.Errorf("invalid TEN-VAD instance type")
	}

	hopSize := config.GlobalConfig.VAD.TenVAD.HopSize
	minSpeechFrames := config.GlobalConfig.VAD.TenVAD.MinSpeechFrames
	maxSilenceFrames := config.GlobalConfig.VAD.TenVAD.MaxSilenceFrames
	if hopSize <= 0 {
		return fmt.Errorf("invalid TEN-VAD hop_size: %d", hopSize)
	}

	for i := 0; i < len(float32Slice); i += hopSize {
		end := i + hopSize
		if end > len(float32Slice) {
			end = len(float32Slice)
		}
		frame := float32Slice[i:end]

		int16Frame := make([]int16, len(frame))
		for j, f := range frame {
			int16Frame[j] = int16(f * 32768)
		}

		_, flag, err := pool.GetInstance().ProcessAudio(tenVADInstance.Handle, int16Frame)
		if err != nil {
			return fmt.Errorf("TEN-VAD ProcessAudio error: %v", err)
		}

		if flag == 1 {
			if !state.isInSpeech {
				state.isInSpeech = true
				state.currentSegment = make([]float32, 0, hopSize*4)
				state.silenceFrameCount = 0
			}
			state.currentSegment = append(state.currentSegment, frame...)
			state.silenceFrameCount = 0
			continue
		}

		if !state.isInSpeech {
			continue
		}

		state.silenceFrameCount++
		state.currentSegment = append(state.currentSegment, frame...)
		if state.silenceFrameCount < maxSilenceFrames {
			continue
		}

		frameCount := len(state.currentSegment) / hopSize
		if frameCount >= minSpeechFrames {
			segmentCopy := append([]float32(nil), state.currentSegment...)
			e.decodeAsync(sessionID, segmentCopy, config.GlobalConfig.Audio.SampleRate)
		}

		state.isInSpeech = false
		state.silenceFrameCount = 0
		state.currentSegment = nil
	}

	return nil
}

func (e *engine) decodeAsync(sessionID string, samples []float32, sampleRate int) {
	if atomic.LoadInt32(&e.stopping) == 1 {
		return
	}

	if e.decodeMode == decodeModePool {
		task := decodeTask{
			sessionID:  sessionID,
			samples:    samples,
			sampleRate: sampleRate,
		}
		switch e.enqueueDecodeTask(task) {
		case decodeEnqueueOK:
			return
		case decodeEnqueueClosed:
			if atomic.LoadInt32(&e.stopping) == 1 {
				return
			}
			atomic.AddInt64(&e.decodeDroppedJobs, 1)
			e.emitResult(sessionID, "", fmt.Errorf("decode queue is closed"))
			return
		case decodeEnqueueFull:
			dropped := atomic.AddInt64(&e.decodeDroppedJobs, 1)
			if dropped == 1 || dropped%100 == 0 {
				logger.Warnf("ASR decode queue 已满，累计丢弃=%d（queue_size=%d）", dropped, cap(e.decodeQueue))
			}
			e.emitResult(sessionID, "", fmt.Errorf("decode queue is full"))
			return
		}
	}

	go func() {
		text, err := e.decode(samples, sampleRate)
		if err != nil || text != "" {
			e.emitResult(sessionID, text, err)
		}
	}()
}

func (e *engine) decode(samples []float32, sampleRate int) (string, error) {
	if e.decodeMode == decodeModePool {
		return e.decodeWithRecognizerPool(samples, sampleRate)
	}
	return e.decodeLegacy(samples, sampleRate)
}

func (e *engine) decodeLegacy(samples []float32, sampleRate int) (string, error) {
	e.decodeMu.Lock()
	defer e.decodeMu.Unlock()

	return e.decodeWithRecognizer(e.recognizer, samples, sampleRate)
}

func (e *engine) decodeWithRecognizerPool(samples []float32, sampleRate int) (string, error) {
	if e.decodePool == nil {
		return e.decodeLegacy(samples, sampleRate)
	}
	recognizer := <-e.decodePool
	if recognizer == nil {
		return "", fmt.Errorf("decode recognizer is nil")
	}
	defer func() {
		e.decodePool <- recognizer
	}()
	return e.decodeWithRecognizer(recognizer, samples, sampleRate)
}

func (e *engine) decodeWithRecognizer(recognizer *sherpa.OfflineRecognizer, samples []float32, sampleRate int) (string, error) {
	if recognizer == nil {
		return "", fmt.Errorf("recognizer is not initialized")
	}
	if sampleRate <= 0 {
		sampleRate = config.GlobalConfig.Audio.SampleRate
	}

	stream := sherpa.NewOfflineStream(recognizer)
	defer sherpa.DeleteOfflineStream(stream)

	stream.AcceptWaveform(sampleRate, samples)

	recognizer.Decode(stream)

	result := stream.GetResult()
	if result == nil {
		return "", fmt.Errorf("recognition failed")
	}
	return result.Text, nil
}

const defaultInlineAppendTailMS = 400

// RecognizeFloat32 直接识别整段 PCM 浮点采样（不经过 VAD 分段）。
func (e *engine) RecognizeFloat32(samples []float32, sampleRate int) (string, error) {
	if atomic.LoadInt32(&e.stopping) == 1 {
		return "", fmt.Errorf("engine is shutting down")
	}
	if e.recognizer == nil {
		return "", fmt.Errorf("recognizer is not initialized")
	}
	if len(samples) == 0 {
		return "", nil
	}
	if sampleRate <= 0 {
		sampleRate = config.GlobalConfig.Audio.SampleRate
	}
	return e.decode(samples, sampleRate)
}

// RecognizeWithVAD 使用与实时会话一致的 VAD 规则执行一次性识别。
// 该方法是对外可复用的核心能力，HTTP/WS/API 入口都应复用这一层。
func (e *engine) RecognizeWithVAD(samples []float32, sampleRate int) (string, error) {
	if atomic.LoadInt32(&e.stopping) == 1 {
		return "", fmt.Errorf("engine is shutting down")
	}
	if e.recognizer == nil {
		return "", fmt.Errorf("recognizer is not initialized")
	}
	if e.vadPool == nil {
		return "", fmt.Errorf("VAD pool is not initialized")
	}
	if len(samples) == 0 {
		return "", nil
	}
	if sampleRate <= 0 {
		sampleRate = config.GlobalConfig.Audio.SampleRate
	}

	samplesWithTail := appendSilenceTail(samples, sampleRate, defaultInlineAppendTailMS)

	vadInstance, err := e.vadPool.Get()
	if err != nil {
		return "", fmt.Errorf("failed to get VAD instance: %w", err)
	}
	defer e.vadPool.Put(vadInstance)

	switch vadInstance.GetType() {
	case pool.SILERO_TYPE:
		sileroInstance, ok := vadInstance.(*pool.SileroVADInstance)
		if !ok {
			return "", fmt.Errorf("invalid Silero VAD instance type")
		}
		return e.recognizeWithSileroVAD(samplesWithTail, sampleRate, sileroInstance)
	case pool.TEN_VAD_TYPE:
		tenVADInstance, ok := vadInstance.(*pool.TenVADInstance)
		if !ok {
			return "", fmt.Errorf("invalid TEN-VAD instance type")
		}
		return e.recognizeWithTenVAD(samplesWithTail, sampleRate, tenVADInstance)
	default:
		return "", fmt.Errorf("unsupported VAD type: %s", vadInstance.GetType())
	}
}

func (e *engine) recognizeWithSileroVAD(samples []float32, sampleRate int, instance *pool.SileroVADInstance) (string, error) {
	vadTimeout := time.Duration(config.GlobalConfig.Response.Timeout) * time.Second
	vadCtx, cancel := context.WithTimeout(context.Background(), vadTimeout)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		instance.VAD.AcceptWaveform(samples)
	}()

	select {
	case <-done:
	case <-vadCtx.Done():
		return "", fmt.Errorf("VAD processing timeout")
	}

	minSpeechDuration := float64(config.GlobalConfig.VAD.SileroVAD.MinSpeechDuration)
	maxDuration := float64(config.GlobalConfig.VAD.SileroVAD.MaxSpeechDuration)
	parts := make([]string, 0, 4)
	var decodeErr error

	for !instance.VAD.IsEmpty() {
		segment := instance.VAD.Front()
		instance.VAD.Pop()
		if segment == nil || len(segment.Samples) == 0 {
			continue
		}

		duration := float64(len(segment.Samples)) / float64(sampleRate)
		if duration < minSpeechDuration {
			continue
		}

		segmentSamples := segment.Samples
		if maxDuration > 0 && duration > maxDuration {
			maxSamples := int(maxDuration * float64(sampleRate))
			if maxSamples > 0 && maxSamples < len(segmentSamples) {
				segmentSamples = segmentSamples[:maxSamples]
			}
		}

		text, err := e.decode(segmentSamples, sampleRate)
		if err != nil {
			decodeErr = err
			continue
		}
		if trimmed := strings.TrimSpace(text); trimmed != "" {
			parts = append(parts, trimmed)
		}
	}

	merged := strings.TrimSpace(strings.Join(parts, " "))
	if merged != "" {
		return merged, nil
	}
	if decodeErr != nil {
		return "", decodeErr
	}
	return "", nil
}

func (e *engine) recognizeWithTenVAD(samples []float32, sampleRate int, instance *pool.TenVADInstance) (string, error) {
	hopSize := config.GlobalConfig.VAD.TenVAD.HopSize
	minSpeechFrames := config.GlobalConfig.VAD.TenVAD.MinSpeechFrames
	maxSilenceFrames := config.GlobalConfig.VAD.TenVAD.MaxSilenceFrames
	if hopSize <= 0 {
		return "", fmt.Errorf("invalid TEN-VAD hop_size: %d", hopSize)
	}
	if minSpeechFrames <= 0 {
		minSpeechFrames = 1
	}
	if maxSilenceFrames <= 0 {
		maxSilenceFrames = 1
	}

	parts := make([]string, 0, 4)
	var decodeErr error
	isInSpeech := false
	currentSegment := make([]float32, 0, hopSize*4)
	silenceFrameCount := 0

	flush := func() {
		frameCount := len(currentSegment) / hopSize
		if frameCount >= minSpeechFrames {
			text, err := e.decode(currentSegment, sampleRate)
			if err != nil {
				decodeErr = err
			} else if trimmed := strings.TrimSpace(text); trimmed != "" {
				parts = append(parts, trimmed)
			}
		}
		isInSpeech = false
		silenceFrameCount = 0
		currentSegment = currentSegment[:0]
	}

	for i := 0; i < len(samples); i += hopSize {
		end := i + hopSize
		if end > len(samples) {
			end = len(samples)
		}
		frame := samples[i:end]

		int16Frame := make([]int16, len(frame))
		for j, f := range frame {
			int16Frame[j] = int16(f * 32768)
		}

		_, flag, err := pool.GetInstance().ProcessAudio(instance.Handle, int16Frame)
		if err != nil {
			return "", fmt.Errorf("TEN-VAD ProcessAudio error: %v", err)
		}

		if flag == 1 {
			if !isInSpeech {
				isInSpeech = true
				currentSegment = currentSegment[:0]
			}
			currentSegment = append(currentSegment, frame...)
			silenceFrameCount = 0
			continue
		}

		if !isInSpeech {
			continue
		}

		silenceFrameCount++
		currentSegment = append(currentSegment, frame...)
		if silenceFrameCount >= maxSilenceFrames {
			flush()
		}
	}

	if isInSpeech && len(currentSegment) > 0 {
		flush()
	}

	merged := strings.TrimSpace(strings.Join(parts, " "))
	if merged != "" {
		return merged, nil
	}
	if decodeErr != nil {
		return "", decodeErr
	}
	return "", nil
}

func appendSilenceTail(samples []float32, sampleRate int, silenceMS int) []float32 {
	if sampleRate <= 0 || silenceMS <= 0 {
		return samples
	}
	silenceSamples := sampleRate * silenceMS / 1000
	if silenceSamples <= 0 {
		return samples
	}
	out := make([]float32, len(samples)+silenceSamples)
	copy(out, samples)
	return out
}

func (e *engine) emitResult(sessionID, result string, err error) {
	e.resultMu.RLock()
	handler := e.resultHandle
	e.resultMu.RUnlock()
	if handler != nil {
		handler(sessionID, result, err)
	}
}

func (e *engine) getDecodeStats() map[string]interface{} {
	stats := map[string]interface{}{
		"mode":         e.decodeMode,
		"dropped_jobs": atomic.LoadInt64(&e.decodeDroppedJobs),
	}

	if e.decodeMode == decodeModePool {
		queueLen := 0
		queueSize := 0
		availableRecognizers := 0

		if e.decodeQueue != nil {
			queueLen = len(e.decodeQueue)
			queueSize = cap(e.decodeQueue)
		}
		if e.decodePool != nil {
			availableRecognizers = len(e.decodePool)
		}

		stats["workers"] = e.decodeWorkers
		stats["recognizers"] = len(e.decodeRecognizers)
		stats["queue_len"] = queueLen
		stats["queue_size"] = queueSize
		stats["pool_available"] = availableRecognizers
	}

	return stats
}

// GetStats 获取核心引擎统计信息。
func (e *engine) GetStats() map[string]interface{} {
	e.mu.RLock()
	currentSessions := len(e.sessions)
	e.mu.RUnlock()

	totalSessions := atomic.LoadInt64(&e.totalSessions)
	activeSessions := atomic.LoadInt64(&e.activeSessions)
	totalMessages := atomic.LoadInt64(&e.totalMessages)

	var poolStats map[string]interface{}
	if e.vadPool != nil {
		poolStats = e.vadPool.GetStats()
	} else {
		poolStats = map[string]interface{}{"status": "not_initialized"}
	}

	speakerStats := map[string]interface{}{"status": "disabled"}
	if service := e.getSpeakerService(); service != nil {
		speakerStats = service.GetStats("", "")
	}
	decodeStats := e.getDecodeStats()

	sessionStats := map[string]interface{}{
		"total_sessions":   totalSessions,
		"active_sessions":  activeSessions,
		"total_messages":   totalMessages,
		"current_sessions": currentSessions,
	}

	startTime := e.startTime
	if startTime.IsZero() {
		startTime = time.Now()
	}

	return map[string]interface{}{
		"timestamp":        time.Now().Format(time.RFC3339),
		"start_time":       startTime.Format(time.RFC3339),
		"total_sessions":   totalSessions,
		"active_sessions":  activeSessions,
		"total_messages":   totalMessages,
		"current_sessions": currentSessions,
		"sessions":         sessionStats,
		"vad_pool":         poolStats,
		"speaker":          speakerStats,
		"decode":           decodeStats,
	}
}

// GetHealth 获取引擎健康状态
func (e *engine) GetHealth() map[string]interface{} {
	status := "healthy"
	components := make(map[string]interface{})

	recognition := map[string]interface{}{}
	if e.recognizer != nil {
		recognition["status"] = "ready"
		recognition["enabled"] = true
	} else if config.GlobalConfig.Recognition.Enabled {
		recognition["status"] = "not_initialized"
		recognition["enabled"] = true
		status = "initializing"
	} else {
		recognition["status"] = "disabled"
		recognition["enabled"] = false
	}
	components["recognition"] = recognition

	if e.vadPool != nil {
		vadStats := e.vadPool.GetStats()
		if vadStats == nil {
			vadStats = map[string]interface{}{}
		}
		if _, ok := vadStats["status"]; !ok {
			vadStats["status"] = "ready"
		}
		components["vad_pool"] = vadStats
	} else {
		components["vad_pool"] = map[string]interface{}{"status": "not_initialized"}
		status = "initializing"
	}

	service := e.getSpeakerService()
	switch {
	case service != nil:
		speakerStats := service.GetStats("", "")
		if speakerStats == nil {
			speakerStats = map[string]interface{}{}
		}
		if _, ok := speakerStats["status"]; !ok {
			speakerStats["status"] = "ready"
		}
		components["speaker"] = speakerStats
	case config.GlobalConfig.Speaker.Enabled:
		components["speaker"] = map[string]interface{}{"status": "not_initialized"}
		if status == "healthy" {
			status = "degraded"
		}
	default:
		components["speaker"] = map[string]interface{}{"status": "disabled"}
	}

	decodeStats := e.getDecodeStats()
	if e.decodeMode == decodeModePool {
		decodeStats["status"] = "ready"
		if e.decodeQueue == nil || e.decodePool == nil {
			decodeStats["status"] = "not_initialized"
			if status == "healthy" {
				status = "degraded"
			}
		}
	} else {
		decodeStats["status"] = "legacy"
	}
	components["decode"] = decodeStats

	stats := e.GetStats()
	components["sessions"] = map[string]interface{}{
		"total_sessions":   stats["total_sessions"],
		"active_sessions":  stats["active_sessions"],
		"total_messages":   stats["total_messages"],
		"current_sessions": stats["current_sessions"],
	}

	return map[string]interface{}{
		"status":     status,
		"timestamp":  time.Now().Format(time.RFC3339),
		"components": components,
	}
}

// Shutdown 关闭引擎并释放会话占用资源。
func (e *engine) Shutdown() {
	e.shutdownOnce.Do(func() {
		atomic.StoreInt32(&e.stopping, 1)

		if manager := e.GetSessionManager(); manager != nil {
			manager.Shutdown()
		}

		e.mu.RLock()
		ids := make([]string, 0, len(e.sessions))
		for id := range e.sessions {
			ids = append(ids, id)
		}
		e.mu.RUnlock()

		for _, id := range ids {
			e.CloseSession(id)
		}

		if e.decodeQueue != nil {
			close(e.decodeQueue)
			e.decodeWG.Wait()
			e.decodeQueue = nil
		}

		e.decodeWorkers = 0
		e.decodePool = nil

		// 先关闭声纹服务，再关闭共享 VAD 池，避免顺序问题。
		service := e.getSpeakerService()
		if service != nil {
			if closer, ok := service.(interface{ Close() }); ok {
				closer.Close()
			}
			e.speakerMu.Lock()
			e.speaker = nil
			e.speakerMu.Unlock()
		}

		if e.vadPool != nil {
			e.vadPool.Shutdown()
			e.vadPool = nil
		}

		// legacy 路径与识别器释放通过 decodeMu 串行，避免并发释放。
		e.decodeMu.Lock()
		uniqueRecognizers := make(map[*sherpa.OfflineRecognizer]struct{}, len(e.decodeRecognizers)+1)
		for _, rec := range e.decodeRecognizers {
			if rec != nil {
				uniqueRecognizers[rec] = struct{}{}
			}
		}
		if e.recognizer != nil {
			uniqueRecognizers[e.recognizer] = struct{}{}
		}
		for rec := range uniqueRecognizers {
			sherpa.DeleteOfflineRecognizer(rec)
		}
		e.recognizer = nil
		e.decodeRecognizers = nil
		e.decodeMu.Unlock()

		e.sessionMu.Lock()
		e.sessionManager = nil
		e.sessionMu.Unlock()
	})
}
