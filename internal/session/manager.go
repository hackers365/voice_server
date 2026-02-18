package session

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"voice_server/config"
	"voice_server/core"
	"voice_server/internal/logger"
)

// SessionConn 抽象会话连接，便于替换 WS/GRPC/测试实现。
type SessionConn interface {
	WriteJSON(v interface{}) error
	Close() error
}

// Session 仅负责连接与发送队列管理，不承载识别核心状态。
type Session struct {
	ID       string
	Conn     SessionConn
	LastSeen int64
	closed   int32

	SendQueue    chan interface{}
	sendDone     chan struct{}
	sendErrCount int32
}

// Manager 会话管理器，负责连接生命周期，并委托 core.Engine 处理识别业务。
type Manager struct {
	sessions map[string]*Session
	engine   core.Engine
	mu       sync.RWMutex

	// 统计信息
	totalSessions  int64
	activeSessions int64
	totalMessages  int64

	ctx    context.Context
	cancel context.CancelFunc
}

// NewManager 创建新的会话管理器。
func NewManager(engine core.Engine) *Manager {
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		sessions: make(map[string]*Session),
		engine:   engine,
		ctx:      ctx,
		cancel:   cancel,
	}

	if engine != nil {
		engine.SetResultHandler(m.handleRecognitionResult)
	}
	return m
}

// CreateSession 创建新会话。
func (m *Manager) CreateSession(sessionID string, conn SessionConn) error {
	if m.engine == nil {
		return fmt.Errorf("engine is not initialized")
	}

	if err := m.engine.OpenSession(sessionID); err != nil {
		return err
	}

	session := &Session{
		ID:           sessionID,
		Conn:         conn,
		LastSeen:     time.Now().UnixNano(),
		closed:       0,
		SendQueue:    make(chan interface{}, config.GlobalConfig.Session.SendQueueSize),
		sendDone:     make(chan struct{}),
		sendErrCount: 0,
	}

	m.mu.Lock()
	if _, exists := m.sessions[sessionID]; exists {
		m.mu.Unlock()
		m.engine.CloseSession(sessionID)
		return fmt.Errorf("session %s already exists", sessionID)
	}
	m.sessions[sessionID] = session
	m.mu.Unlock()

	go session.sendLoop()
	m.enqueueSessionMessage(session, map[string]interface{}{
		"type":       "connection",
		"message":    "WebSocket connected, ready for audio",
		"session_id": sessionID,
	})

	atomic.AddInt64(&m.totalSessions, 1)
	atomic.AddInt64(&m.activeSessions, 1)
	return nil
}

// GetSession 获取会话。
func (m *Manager) GetSession(sessionID string) (*Session, bool) {
	m.mu.RLock()
	session, exists := m.sessions[sessionID]
	m.mu.RUnlock()

	if exists {
		atomic.StoreInt64(&session.LastSeen, time.Now().UnixNano())
	}
	return session, exists
}

// RemoveSession 移除会话。
func (m *Manager) RemoveSession(sessionID string) {
	m.mu.Lock()
	session, exists := m.sessions[sessionID]
	if exists {
		delete(m.sessions, sessionID)
	}
	m.mu.Unlock()

	if !exists {
		return
	}

	m.closeSession(session)
	if m.engine != nil {
		m.engine.CloseSession(sessionID)
	}
	atomic.AddInt64(&m.activeSessions, -1)
	logger.Infof("🗑️  Session removed")
}

// ProcessAudioData 处理音频数据，核心逻辑由 engine 承担。
func (m *Manager) ProcessAudioData(sessionID string, audioData []byte) error {
	session, exists := m.GetSession(sessionID)
	if !exists {
		return fmt.Errorf("session %s not found", sessionID)
	}
	if atomic.LoadInt32(&session.closed) == 1 {
		return fmt.Errorf("session %s is closed", sessionID)
	}
	if m.engine == nil {
		return fmt.Errorf("engine is not initialized")
	}

	atomic.StoreInt64(&session.LastSeen, time.Now().UnixNano())
	atomic.AddInt64(&m.totalMessages, 1)
	return m.engine.ProcessAudioData(sessionID, audioData)
}

// RecognizeWithVAD 对外提供兼容入口，内部直接委托核心 Engine。
func (m *Manager) RecognizeWithVAD(pcmData []float32, sampleRate int) (string, error) {
	if m == nil || m.engine == nil {
		return "", fmt.Errorf("engine is not initialized")
	}
	return m.engine.RecognizeWithVAD(pcmData, sampleRate)
}

// HandleAudioMessage 处理 WS 收到的音频消息，并在失败时负责下发错误协议消息。
func (m *Manager) HandleAudioMessage(sessionID string, audioData []byte) error {
	err := m.ProcessAudioData(sessionID, audioData)
	if err != nil {
		m.enqueueError(sessionID, err)
	}
	return err
}

// GetStats 获取管理器统计信息。
func (m *Manager) GetStats() map[string]interface{} {
	m.mu.RLock()
	currentSessions := len(m.sessions)
	m.mu.RUnlock()

	stats := map[string]interface{}{
		"total_sessions":   atomic.LoadInt64(&m.totalSessions),
		"active_sessions":  atomic.LoadInt64(&m.activeSessions),
		"total_messages":   atomic.LoadInt64(&m.totalMessages),
		"current_sessions": currentSessions,
	}

	if m.engine != nil {
		engineStats := m.engine.GetStats()
		stats["engine_stats"] = engineStats
		if poolStats, ok := engineStats["pool_stats"]; ok {
			stats["pool_stats"] = poolStats
		}
	} else {
		stats["pool_stats"] = map[string]interface{}{"status": "not_initialized"}
	}
	return stats
}

// Shutdown 关闭管理器。
func (m *Manager) Shutdown() {
	logger.Infof("🛑 Shutting down session manager...")
	m.cancel()

	m.mu.RLock()
	ids := make([]string, 0, len(m.sessions))
	for id := range m.sessions {
		ids = append(ids, id)
	}
	m.mu.RUnlock()

	for _, id := range ids {
		m.RemoveSession(id)
	}

	if m.engine != nil {
		m.engine.Shutdown()
	}
	logger.Infof("✅ Session manager shutdown complete")
}

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

			if err := s.Conn.WriteJSON(msg); err != nil {
				atomic.AddInt32(&s.sendErrCount, 1)
				logger.Errorf("Failed to send message to session %s: %v", s.ID, err)
				if atomic.LoadInt32(&s.sendErrCount) > int32(config.GlobalConfig.Session.MaxSendErrors) {
					logger.Errorf("Too many send errors for session %s, closing", s.ID)
					atomic.StoreInt32(&s.closed, 1)
					if s.Conn != nil {
						s.Conn.Close()
					}
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

func (m *Manager) closeSession(session *Session) {
	if !atomic.CompareAndSwapInt32(&session.closed, 0, 1) {
		return
	}

	close(session.sendDone)
	for len(session.SendQueue) > 0 {
		<-session.SendQueue
	}
	if session.Conn != nil {
		session.Conn.Close()
	}
}

func (m *Manager) handleRecognitionResult(sessionID, result string, err error) {
	session, exists := m.GetSession(sessionID)
	if !exists {
		logger.Warnf("Session %s not found when handling recognition result", sessionID)
		return
	}
	if atomic.LoadInt32(&session.closed) == 1 {
		logger.Warnf("Session %s is closed when handling recognition result", sessionID)
		return
	}

	if err == nil && len(result) > 0 {
		m.enqueueSessionMessage(session, map[string]interface{}{
			"type":      "final",
			"text":      result,
			"timestamp": time.Now().UnixMilli(),
		})
		return
	}

	if err != nil {
		logger.Errorf("Recognition error for session %s: %v", sessionID, err)
	}
}

func (m *Manager) enqueueError(sessionID string, err error) {
	session, exists := m.GetSession(sessionID)
	if !exists {
		return
	}
	m.enqueueSessionMessage(session, map[string]interface{}{
		"type":    "error",
		"message": err.Error(),
	})
}

func (m *Manager) enqueueSessionMessage(session *Session, message interface{}) {
	if session == nil {
		return
	}
	if atomic.LoadInt32(&session.closed) == 1 {
		return
	}
	select {
	case session.SendQueue <- message:
	default:
		logger.Warnf("Session %s send queue is full, dropping message", session.ID)
	}
}
