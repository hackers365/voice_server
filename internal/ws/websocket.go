package ws

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"time"

	"voice_server/config"
	"voice_server/internal/logger"
	"voice_server/internal/session"

	"github.com/gorilla/websocket"
)

// Upgrader 用于升级 WebSocket 连接
var Upgrader = websocket.Upgrader{
	CheckOrigin:       func(r *http.Request) bool { return true },
	ReadBufferSize:    config.GlobalConfig.Server.WebSocket.ReadBufferSize,
	WriteBufferSize:   config.GlobalConfig.Server.WebSocket.WriteBufferSize,
	EnableCompression: config.GlobalConfig.Server.WebSocket.EnableCompression,
}

// GenerateSessionID 生成会话ID
func GenerateSessionID() string {
	bytes := make([]byte, 16)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// SessionManager 定义 WS 层所需的最小会话管理能力。
type SessionManager interface {
	CreateSession(sessionID string, conn session.SessionConn) error
	RemoveSession(sessionID string)
	HandleAudioMessage(sessionID string, audioData []byte) error
}

// HandleWebSocket 处理 WebSocket 连接
// 依赖注入 sessionManager
func HandleWebSocket(w http.ResponseWriter, r *http.Request, sessionManager SessionManager) {
	conn, err := Upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Errorf("WebSocket upgrade failed: %v", err)
		return
	}

	wsConfig := config.GlobalConfig.Server.WebSocket

	if wsConfig.ReadTimeout > 0 {
		conn.SetReadDeadline(time.Now().Add(time.Duration(wsConfig.ReadTimeout) * time.Second))
	}

	sessionID := GenerateSessionID()

	// 创建会话
	err = sessionManager.CreateSession(sessionID, conn)
	if err != nil {
		logger.Errorf("Failed to create session, session_id=%s, error=%v", sessionID, err)
		conn.Close()
		return
	}

	defer func() {
		sessionManager.RemoveSession(sessionID)
		logger.Infof("WebSocket connection closed, session_id=%s", sessionID)
	}()

	logger.Infof("New WebSocket connection established, session_id=%s", sessionID)

	// 处理消息
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			logger.Warnf("WebSocket read error")
			break
		}

		// 每次收到消息都刷新读超时
		if wsConfig.ReadTimeout > 0 {
			conn.SetReadDeadline(time.Now().Add(time.Duration(wsConfig.ReadTimeout) * time.Second))
		}

		// 检查消息大小
		if wsConfig.MaxMessageSize > 0 && len(message) > wsConfig.MaxMessageSize {
			logger.Warnf("Message too large, closing connection")
			break
		}

		// 处理音频数据
		if len(message) > 0 {
			if err := sessionManager.HandleAudioMessage(sessionID, message); err != nil {
				logger.Errorf("Failed to process audio data, session_id=%s, error=%v", sessionID, err)
			}
		}
	}
}
