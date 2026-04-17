package core

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// RateGuard 抽象限流能力，供 GuardedEngine 在进程内调用链复用。
type RateGuard interface {
	Allow(key string) error
}

// ConnectionGuard 抽象连接占用配额能力，供长连接场景统一治理。
type ConnectionGuard interface {
	AcquireConnection() error
	ReleaseConnection()
}

// GuardedEngine 在核心能力外包一层统一限流，供 HTTP/API 共享。
type guardedEngine struct {
	engine Engine
	guard  RateGuard

	sessionMu      sync.RWMutex
	sessionManager SessionManager
}

type guardStatsProvider interface {
	GetStats() map[string]interface{}
}

type guardedSpeakerStreamingSession struct {
	session SpeakerStreamingSession
	once    sync.Once
	release func()
}

func defaultGuardedHealthComponents() map[string]interface{} {
	return map[string]interface{}{
		"vad_pool":   map[string]interface{}{"status": "not_initialized"},
		"sessions":   map[string]interface{}{"status": "not_initialized"},
		"rate_limit": map[string]interface{}{"status": "not_initialized"},
		"speaker":    map[string]interface{}{"status": "disabled"},
	}
}

func guardedNotInitializedHealth() map[string]interface{} {
	return map[string]interface{}{
		"status":     "not_initialized",
		"timestamp":  time.Now().Format(time.RFC3339),
		"components": defaultGuardedHealthComponents(),
	}
}

// NewGuardedEngine 创建带统一限流能力的 Engine 封装。
// sessionFactory 可选，若提供则由 Engine 在初始化时创建并持有 SessionManager。
func NewGuardedEngine(engine Engine, guard RateGuard, sessionFactory SessionManagerFactory) (Engine, error) {
	g := &guardedEngine{
		engine: engine,
		guard:  guard,
	}
	if sessionFactory != nil {
		manager := sessionFactory(g)
		if manager == nil {
			return nil, fmt.Errorf("session manager factory returned nil")
		}
		g.sessionManager = manager
	}
	return g, nil
}

func (g *guardedEngine) base() (Engine, error) {
	if g == nil || g.engine == nil {
		return nil, fmt.Errorf("core engine is not initialized")
	}
	return g.engine, nil
}

func (g *guardedEngine) allow(key string) error {
	if g == nil || g.guard == nil {
		return nil
	}
	return g.guard.Allow(key)
}

func (g *guardedEngine) acquireConnection() error {
	if g == nil || g.guard == nil {
		return nil
	}
	connectionGuard, ok := g.guard.(ConnectionGuard)
	if !ok {
		return nil
	}
	return connectionGuard.AcquireConnection()
}

func (g *guardedEngine) releaseConnection() {
	if g == nil || g.guard == nil {
		return
	}
	connectionGuard, ok := g.guard.(ConnectionGuard)
	if !ok {
		return
	}
	connectionGuard.ReleaseConnection()
}

func buildGuardKey(prefix string, parts ...string) string {
	keyParts := make([]string, 0, len(parts)+1)
	keyParts = append(keyParts, prefix)
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			keyParts = append(keyParts, trimmed)
		}
	}
	return strings.Join(keyParts, ":")
}

func (s *guardedSpeakerStreamingSession) AcceptAudio(audioData []float32) error {
	if s == nil || s.session == nil {
		return fmt.Errorf("speaker streaming session is not initialized")
	}
	return s.session.AcceptAudio(audioData)
}

func (s *guardedSpeakerStreamingSession) FinishAndIdentify() (*SpeakerIdentifyResult, error) {
	if s == nil || s.session == nil {
		return nil, fmt.Errorf("speaker streaming session is not initialized")
	}
	return s.session.FinishAndIdentify()
}

func (s *guardedSpeakerStreamingSession) Close() {
	if s == nil {
		return
	}
	if s.session != nil {
		s.session.Close()
		s.session = nil
	}
	s.once.Do(func() {
		if s.release != nil {
			s.release()
		}
	})
}

func (g *guardedEngine) SetResultHandler(handler ResultHandler) {
	engine, err := g.base()
	if err != nil {
		return
	}
	engine.SetResultHandler(handler)
}

func (g *guardedEngine) GetSessionManager() SessionManager {
	g.sessionMu.RLock()
	manager := g.sessionManager
	g.sessionMu.RUnlock()
	if manager != nil {
		return manager
	}

	engine, err := g.base()
	if err != nil {
		return nil
	}
	return engine.GetSessionManager()
}

func (g *guardedEngine) OpenSession(sessionID string) error {
	engine, err := g.base()
	if err != nil {
		return err
	}
	if err := g.acquireConnection(); err != nil {
		return err
	}
	if err := g.allow("asr:ws_open"); err != nil {
		g.releaseConnection()
		return err
	}
	if err := engine.OpenSession(sessionID); err != nil {
		g.releaseConnection()
		return err
	}
	return nil
}

func (g *guardedEngine) CloseSession(sessionID string) {
	engine, err := g.base()
	if err != nil {
		return
	}
	engine.CloseSession(sessionID)
	g.releaseConnection()
}

func (g *guardedEngine) ProcessAudioData(sessionID string, audioData []byte) error {
	engine, err := g.base()
	if err != nil {
		return err
	}
	if err := g.allow(buildGuardKey("asr:ws_audio", sessionID)); err != nil {
		return err
	}
	return engine.ProcessAudioData(sessionID, audioData)
}

func (g *guardedEngine) RecognizeFloat32(samples []float32, sampleRate int) (string, error) {
	engine, err := g.base()
	if err != nil {
		return "", err
	}
	if err := g.allow("asr:recognize"); err != nil {
		return "", err
	}
	return engine.RecognizeFloat32(samples, sampleRate)
}

func (g *guardedEngine) RecognizeWithVAD(samples []float32, sampleRate int) (string, error) {
	engine, err := g.base()
	if err != nil {
		return "", err
	}
	if err := g.allow("asr:recognize_vad"); err != nil {
		return "", err
	}
	return engine.RecognizeWithVAD(samples, sampleRate)
}

func (g *guardedEngine) HasSpeakerService() bool {
	engine, err := g.base()
	if err != nil {
		return false
	}
	return engine.HasSpeakerService()
}

func (g *guardedEngine) RegisterSpeaker(uid, agentID, speakerID, speakerName, uuid string, audioData []float32, sampleRate int) error {
	engine, err := g.base()
	if err != nil {
		return err
	}
	if err := g.allow(buildGuardKey("speaker_register", uid, agentID)); err != nil {
		return err
	}
	return engine.RegisterSpeaker(uid, agentID, speakerID, speakerName, uuid, audioData, sampleRate)
}

func (g *guardedEngine) IdentifySpeaker(uid, agentID, speakerID, speakerName string, audioData []float32, sampleRate int, threshold ...float32) (*SpeakerIdentifyResult, error) {
	engine, err := g.base()
	if err != nil {
		return nil, err
	}
	if err := g.allow(buildGuardKey("speaker_identify", uid, agentID)); err != nil {
		return nil, err
	}
	return engine.IdentifySpeaker(uid, agentID, speakerID, speakerName, audioData, sampleRate, threshold...)
}

func (g *guardedEngine) VerifySpeaker(uid, agentID, speakerID string, audioData []float32, sampleRate int) (*SpeakerVerifyResult, error) {
	engine, err := g.base()
	if err != nil {
		return nil, err
	}
	if err := g.allow(buildGuardKey("speaker_verify", uid, agentID)); err != nil {
		return nil, err
	}
	return engine.VerifySpeaker(uid, agentID, speakerID, audioData, sampleRate)
}

func (g *guardedEngine) DeleteSpeaker(uid, agentID, speakerID string) error {
	engine, err := g.base()
	if err != nil {
		return err
	}
	if err := g.allow(buildGuardKey("speaker_delete", uid, agentID)); err != nil {
		return err
	}
	return engine.DeleteSpeaker(uid, agentID, speakerID)
}

func (g *guardedEngine) DeleteSpeakerByUUID(uid, agentID, uuid string) error {
	engine, err := g.base()
	if err != nil {
		return err
	}
	if err := g.allow(buildGuardKey("speaker_delete_uuid", uid, agentID)); err != nil {
		return err
	}
	return engine.DeleteSpeakerByUUID(uid, agentID, uuid)
}

func (g *guardedEngine) GetAllSpeakers(uid, agentID string) []*SpeakerInfo {
	engine, err := g.base()
	if err != nil {
		return []*SpeakerInfo{}
	}
	return engine.GetAllSpeakers(uid, agentID)
}

func (g *guardedEngine) GetSpeakerStats(uid, agentID string) map[string]interface{} {
	engine, err := g.base()
	if err != nil {
		return map[string]interface{}{"status": "disabled"}
	}
	return engine.GetSpeakerStats(uid, agentID)
}

func (g *guardedEngine) NewSpeakerStreamingSession(uid, agentID, speakerID, speakerName string, sampleRate int, threshold ...float32) (SpeakerStreamingSession, error) {
	engine, err := g.base()
	if err != nil {
		return nil, err
	}
	if err := g.acquireConnection(); err != nil {
		return nil, err
	}
	if err := g.allow(buildGuardKey("speaker_stream", uid, agentID)); err != nil {
		g.releaseConnection()
		return nil, err
	}
	session, err := engine.NewSpeakerStreamingSession(uid, agentID, speakerID, speakerName, sampleRate, threshold...)
	if err != nil {
		g.releaseConnection()
		return nil, err
	}
	return &guardedSpeakerStreamingSession{
		session: session,
		release: g.releaseConnection,
	}, nil
}

func (g *guardedEngine) GetStats() map[string]interface{} {
	engine, err := g.base()
	if err != nil {
		return map[string]interface{}{"status": "not_initialized"}
	}
	stats := engine.GetStats()
	if stats == nil {
		stats = make(map[string]interface{})
	}
	if guardStats, ok := g.guard.(guardStatsProvider); ok && guardStats != nil {
		stats["rate_limit"] = guardStats.GetStats()
	}
	return stats
}

func (g *guardedEngine) GetHealth() map[string]interface{} {
	engine, err := g.base()
	if err != nil {
		return guardedNotInitializedHealth()
	}

	health := engine.GetHealth()
	if health == nil {
		health = guardedNotInitializedHealth()
	}

	if _, ok := health["timestamp"]; !ok {
		health["timestamp"] = time.Now().Format(time.RFC3339)
	}

	components, ok := health["components"].(map[string]interface{})
	if !ok || components == nil {
		components = make(map[string]interface{})
		health["components"] = components
	}

	if guardStats, ok := g.guard.(guardStatsProvider); ok && guardStats != nil {
		rateStats := guardStats.GetStats()
		if rateStats == nil {
			rateStats = map[string]interface{}{}
		}
		if _, hasStatus := rateStats["status"]; !hasStatus {
			if enabled, ok := rateStats["enabled"].(bool); ok && !enabled {
				rateStats["status"] = "disabled"
			} else {
				rateStats["status"] = "ready"
			}
		}
		components["rate_limit"] = rateStats
	} else {
		components["rate_limit"] = map[string]interface{}{"status": "disabled"}
	}

	return health
}

func (g *guardedEngine) Shutdown() {
	engine, err := g.base()
	if err != nil {
		return
	}
	engine.Shutdown()

	g.sessionMu.Lock()
	g.sessionManager = nil
	g.sessionMu.Unlock()
}
