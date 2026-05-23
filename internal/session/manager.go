package session

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"voice_server/config"
	"voice_server/internal/logger"
	"voice_server/internal/pool"

	sherpa "github.com/hackers365/sherpa-onnx-go/sherpa_onnx"
)

// Session WebSocket会话
type Session struct {
	ID          string
	Conn        *websocket.Conn
	VADInstance pool.VADInstanceInterface // 使用VAD实例接口
	LastSeen    int64                     // 使用int64存储时间戳
	mu          sync.RWMutex
	closed      int32

	// 发送队列和通道
	SendQueue    chan interface{}
	sendDone     chan struct{}
	sendErrCount int32

	// 活跃性检测
	lastActivity time.Time

	// ten-vad 相关
	isInSpeech        bool
	currentSegment    []float32
	silenceFrameCount int
}

// Manager 会话管理器
type Manager struct {
	sessions   map[string]*Session
	recognizer *sherpa.OfflineRecognizer
	vadPool    pool.VADPoolInterface
	mu         sync.RWMutex

	// 统计信息
	totalSessions  int64
	activeSessions int64
	totalMessages  int64

	// 清理
	ctx    context.Context
	cancel context.CancelFunc
}

// 全局缓冲区池（8KB）
var bufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 8192)
	},
}

// 全局float32切片池（最大支持8KB/2=4096采样点）
var float32Pool = sync.Pool{}

func getFloat32PoolSlice() []float32 {
	chunkSize := config.GlobalConfig.Audio.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 4096
	}
	return make([]float32, chunkSize)
}

// NewManager 创建新的会话管理器
func NewManager(recognizer *sherpa.OfflineRecognizer, vadPool pool.VADPoolInterface) *Manager {
	ctx, cancel := context.WithCancel(context.Background())

	manager := &Manager{
		sessions:   make(map[string]*Session),
		recognizer: recognizer,
		vadPool:    vadPool,
		ctx:        ctx,
		cancel:     cancel,
	}

	return manager
}

// CreateSession 创建新会话
func (m *Manager) CreateSession(sessionID string, conn *websocket.Conn) (*Session, error) {
	// 不在此处分配VAD实例，VADInstance初始化为nil
	if m.vadPool == nil {
		return nil, fmt.Errorf("VAD pool is not initialized")
	}

	session := &Session{
		ID:                sessionID,
		Conn:              conn,
		VADInstance:       nil, // 延迟分配
		LastSeen:          time.Now().UnixNano(),
		closed:            0,
		SendQueue:         make(chan interface{}, config.GlobalConfig.Session.SendQueueSize),
		sendDone:          make(chan struct{}),
		sendErrCount:      0,
		lastActivity:      time.Now(),
		isInSpeech:        false,
		currentSegment:    nil,
		silenceFrameCount: 0,
	}

	// 启动发送协程
	go session.sendLoop()

	m.mu.Lock()
	m.sessions[sessionID] = session
	m.mu.Unlock()

	atomic.AddInt64(&m.totalSessions, 1)
	atomic.AddInt64(&m.activeSessions, 1)

	return session, nil
}

// GetSession 获取会话
func (m *Manager) GetSession(sessionID string) (*Session, bool) {
	m.mu.RLock()
	session, exists := m.sessions[sessionID]
	m.mu.RUnlock()

	if exists {
		// 使用原子操作更新LastSeen
		atomic.StoreInt64(&session.LastSeen, time.Now().UnixNano())
	}

	return session, exists
}

// RemoveSession 移除会话
func (m *Manager) RemoveSession(sessionID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if session, exists := m.sessions[sessionID]; exists {
		m.closeSession(session)
		delete(m.sessions, sessionID)
		atomic.AddInt64(&m.activeSessions, -1)
		logger.Infof("🗑️  Session removed")
	}
}

// sendLoop 发送循环
func (s *Session) sendLoop() {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("❌ Send loop panicked for session %s: %v", s.ID, r)
		}
	}()

	for {
		select {
		case msg := <-s.SendQueue:
			if atomic.LoadInt32(&s.closed) == 1 {
				return
			}

			// 直接写消息，不再设置写超时
			if err := s.Conn.WriteJSON(msg); err != nil {
				atomic.AddInt32(&s.sendErrCount, 1)
				logger.Errorf("Failed to send message to session %s: %v", s.ID, err)
				// 如果连续错误超过阈值，关闭会话
				if atomic.LoadInt32(&s.sendErrCount) > int32(config.GlobalConfig.Session.MaxSendErrors) {
					logger.Errorf("Too many send errors for session, closing")
					atomic.StoreInt32(&s.closed, 1)
					return
				}
			} else {
				atomic.StoreInt32(&s.sendErrCount, 0)
			}
		case <-s.sendDone:
			return
		}
	}
}

// ProcessAudioData 处理音频数据
func (m *Manager) ProcessAudioData(sessionID string, audioData []byte) error {
	session, exists := m.GetSession(sessionID)
	if !exists {
		logger.Errorf("Session %s not found when processing audio data", sessionID)
		return fmt.Errorf("session %s not found", sessionID)
	}

	if atomic.LoadInt32(&session.closed) == 1 {
		logger.Errorf("Session %s is closed, cannot process audio data", sessionID)
		return fmt.Errorf("session %s is closed", sessionID)
	}

	// 检查并延迟分配VAD实例
	if session.VADInstance == nil {
		vadInstance, err := m.vadPool.Get()
		if err != nil {
			logger.Errorf("Failed to get VAD instance for session %s: %v", sessionID, err)
			return fmt.Errorf("failed to get VAD instance for session %s: %v", sessionID, err)
		}
		session.VADInstance = vadInstance
		logger.Infof("✅ Session %s assigned %s VAD instance %d", sessionID, vadInstance.GetType(), vadInstance.GetID())
	}

	// 更新会话活跃时间
	atomic.StoreInt64(&session.LastSeen, time.Now().UnixNano())
	atomic.AddInt64(&m.totalMessages, 1)

	// 验证输入数据
	if len(audioData) == 0 {
		logger.Warnf("Session %s: Received empty audio data", sessionID)
		return fmt.Errorf("empty audio data")
	}

	if len(audioData)%2 != 0 {
		logger.Warnf("Session %s: Audio data length %d is not even (expecting 16-bit samples)", sessionID, len(audioData))
		return fmt.Errorf("invalid audio data length: %d", len(audioData))
	}

	// 转换音频数据
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

	logger.Debugf("Session %s: Converted %d bytes to %d float32 samples", sessionID, len(audioData), numSamples)

	// 根据VAD类型处理
	switch session.VADInstance.GetType() {
	case pool.SILERO_TYPE:
		return m.processSileroVAD(session, sessionID, float32Slice)
	case pool.TEN_VAD_TYPE:
		return m.processTenVAD(session, sessionID, float32Slice)
	default:
		return fmt.Errorf("unsupported VAD type: %s", session.VADInstance.GetType())
	}
}

// processSileroVAD 处理Silero VAD
func (m *Manager) processSileroVAD(session *Session, sessionID string, float32Slice []float32) error {
	// 类型断言获取Silero VAD实例
	sileroInstance, ok := session.VADInstance.(*pool.SileroVADInstance)
	if !ok {
		return fmt.Errorf("invalid Silero VAD instance type")
	}

	// VAD检测 - 使用响应超时配置
	vadTimeout := time.Duration(config.GlobalConfig.Response.Timeout) * time.Second
	vadCtx, vadCancel := context.WithTimeout(context.Background(), vadTimeout)
	defer vadCancel()

	// 在goroutine中执行VAD处理，避免阻塞
	vadDone := make(chan struct{})
	go func() {
		defer close(vadDone)
		sileroInstance.VAD.AcceptWaveform(float32Slice)
	}()

	// 等待VAD处理完成或超时
	select {
	case <-vadDone:
		// VAD处理完成
	case <-vadCtx.Done():
		logger.Warnf("Session %s: VAD processing timeout", sessionID)
		return fmt.Errorf("VAD processing timeout")
	}

	// 处理语音段
	segmentCount := 0
	var speechSegments [][]float32
	sampleRate := config.GlobalConfig.Audio.SampleRate

	// 收集所有有效的语音段
	for !sileroInstance.VAD.IsEmpty() {
		segment := sileroInstance.VAD.Front()
		sileroInstance.VAD.Pop()
		segmentCount++

		if segment != nil && len(segment.Samples) > 0 {
			// 再次检查会话状态
			if atomic.LoadInt32(&session.closed) == 1 {
				logger.Warnf("Session %s closed during speech segment processing", sessionID)
				return fmt.Errorf("session %s closed during processing", sessionID)
			}

			// 验证音频数据
			if len(segment.Samples) == 0 {
				logger.Warnf("Session %s: Speech segment %d has no samples", sessionID, segmentCount)
				continue
			}

			// 音频时长检查
			duration := float64(len(segment.Samples)) / float64(sampleRate)
			minSpeechDuration := float64(config.GlobalConfig.VAD.SileroVAD.MinSpeechDuration)
			if duration < minSpeechDuration {
				logger.Debugf("Session %s: Skipping short segment %d (%.2fs < %.2fs)", sessionID, segmentCount, duration, minSpeechDuration)
				continue
			}

			// 检查最大时长
			maxDuration := float64(config.GlobalConfig.VAD.SileroVAD.MaxSpeechDuration)
			if duration > maxDuration {
				logger.Warnf("Session %s: Segment %d too long (%.2fs > %.2fs), truncating", sessionID, segmentCount, duration, maxDuration)
				maxSamples := int(maxDuration * float64(sampleRate))
				segment.Samples = segment.Samples[:maxSamples]
			}

			speechSegments = append(speechSegments, segment.Samples)
			logger.Debugf("Session %s: Collected segment %d with %d samples (%.2fs)", sessionID, segmentCount, len(segment.Samples), duration)
		} else {
			logger.Warnf("Session %s: Empty or null speech segment %d", sessionID, segmentCount)
		}
	}

	// 处理收集到的语音段
	for i, samples := range speechSegments {
		// 提交识别任务
		taskID := fmt.Sprintf("%s_%d_%d", sessionID, time.Now().UnixNano(), i)
		go func(samples []float32, sampleRate int, sessionID string, taskID string) {
			stream := sherpa.NewOfflineStream(m.recognizer)
			defer sherpa.DeleteOfflineStream(stream)
			stream.AcceptWaveform(sampleRate, samples)
			m.recognizer.Decode(stream)
			result := stream.GetResult()
			if result != nil {
				m.handleRecognitionResult(sessionID, result.Text, nil)
			} else {
				m.handleRecognitionResult(sessionID, "", fmt.Errorf("recognition failed"))
			}
		}(samples, sampleRate, sessionID, taskID)
	}

	return nil
}

// processTenVAD 处理TEN-VAD
func (m *Manager) processTenVAD(session *Session, sessionID string, float32Slice []float32) error {
	// 类型断言获取TEN-VAD实例
	tenVADInstance, ok := session.VADInstance.(*pool.TenVADInstance)
	if !ok {
		return fmt.Errorf("invalid TEN-VAD instance type")
	}

	hopSize := config.GlobalConfig.VAD.TenVAD.HopSize
	minSpeechFrames := config.GlobalConfig.VAD.TenVAD.MinSpeechFrames
	maxSilenceFrames := config.GlobalConfig.VAD.TenVAD.MaxSilenceFrames

	// 分帧处理
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
			if !session.isInSpeech {
				logger.Debugf("Session %s: Speech started", sessionID)
				session.isInSpeech = true
				session.currentSegment = make([]float32, 0)
				session.silenceFrameCount = 0
			}
			session.currentSegment = append(session.currentSegment, frame...)
			session.silenceFrameCount = 0 // 重置静音计数
		} else {
			if session.isInSpeech {
				session.silenceFrameCount++
				session.currentSegment = append(session.currentSegment, frame...)
				if session.silenceFrameCount >= maxSilenceFrames {
					frameCount := len(session.currentSegment) / hopSize
					if frameCount >= minSpeechFrames {
						logger.Debugf("Session %s: Speech segment completed with %d samples (%d frames)", sessionID, len(session.currentSegment), frameCount)
						duration := float64(len(session.currentSegment)) / float64(config.GlobalConfig.Audio.SampleRate)
						logger.Infof("ASR segment length: %.2fs, samples: %d", duration, len(session.currentSegment))
						taskID := fmt.Sprintf("%s_%d", sessionID, time.Now().UnixNano())
						segmentCopy := make([]float32, len(session.currentSegment))
						copy(segmentCopy, session.currentSegment)
						go func(segment []float32, sessionID string, taskID string) {
							stream := sherpa.NewOfflineStream(m.recognizer)
							defer sherpa.DeleteOfflineStream(stream)
							stream.AcceptWaveform(config.GlobalConfig.Audio.SampleRate, segment)
							m.recognizer.Decode(stream)
							result := stream.GetResult()
							if result != nil {
								m.handleRecognitionResult(sessionID, result.Text, nil)
							} else {
								m.handleRecognitionResult(sessionID, "", fmt.Errorf("recognition failed"))
							}
						}(segmentCopy, sessionID, taskID)
					} else {
						logger.Debugf("Session %s: Speech segment too short (%d frames), discarding", sessionID, frameCount)
					}
					session.isInSpeech = false
					session.silenceFrameCount = 0
					session.currentSegment = nil
				}
			}
		}
	}

	return nil
}

// handleRecognitionResult 处理识别结果
func (m *Manager) handleRecognitionResult(sessionID, result string, err error) {
	session, exists := m.GetSession(sessionID)
	if !exists {
		logger.Warnf("Session %s not found when handling recognition result, session may have been closed", sessionID)
		return
	}

	// 检查会话是否已关闭
	if atomic.LoadInt32(&session.closed) == 1 {
		logger.Warnf("Session %s is closed when handling recognition result", sessionID)
		return
	}

	// 只在err为nil且result非空时返回识别结果
	if err == nil && len(result) > 0 {
		response := map[string]interface{}{
			"type":      "final",
			"text":      result,
			"timestamp": time.Now().UnixMilli(),
		}
		select {
		case session.SendQueue <- response:
			logger.Infof("Recognition result queued for session %s: %s", sessionID, result)
		default:
			logger.Warnf("Session %s send queue is full, dropping recognition result", sessionID)
		}
		return
	}

	// 有错误时记录日志，但不返回给用户
	if err != nil {
		logger.Errorf("Recognition error for session %s: %v", sessionID, err)
	}
	// 其他情况（如识别失败、错误或结果为空）不返回任何内容
}

// closeSession 关闭会话
func (m *Manager) closeSession(session *Session) {
	if atomic.CompareAndSwapInt32(&session.closed, 0, 1) {
		// 关闭发送通道
		close(session.sendDone)
		// 清空发送队列
		for len(session.SendQueue) > 0 {
			<-session.SendQueue
		}

		// 归还VAD实例到池中
		if session.VADInstance != nil && m.vadPool != nil {
			m.vadPool.Put(session.VADInstance)
			session.VADInstance = nil
			logger.Infof("🔄 Returned VAD instance to pool for session %s", session.ID)
		}

		if session.Conn != nil {
			session.Conn.Close()
		}
	}
}

// GetStats 获取管理器统计信息 - 增强版本
func (m *Manager) GetStats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 获取资源池统计
	var poolStats map[string]interface{}
	if m.vadPool != nil {
		poolStats = m.vadPool.GetStats()
	} else {
		poolStats = map[string]interface{}{"status": "not_initialized"}
	}

	return map[string]interface{}{
		"total_sessions":   atomic.LoadInt64(&m.totalSessions),
		"active_sessions":  atomic.LoadInt64(&m.activeSessions),
		"total_messages":   atomic.LoadInt64(&m.totalMessages),
		"current_sessions": len(m.sessions),
		"pool_stats":       poolStats,
	}
}

// Shutdown 关闭管理器
func (m *Manager) Shutdown() {
	logger.Infof("🛑 Shutting down session manager...")

	// 取消上下文
	m.cancel()

	// 关闭所有会话
	m.mu.Lock()
	for sessionID, session := range m.sessions {
		logger.Infof("🛑 Closing session: %s", sessionID)
		m.closeSession(session)
	}
	m.sessions = make(map[string]*Session)
	m.mu.Unlock()

	logger.Infof("✅ Session manager shutdown complete")
}
