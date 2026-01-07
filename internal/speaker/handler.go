package speaker

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
	"voice_server/config"
	"voice_server/internal/logger"

	"github.com/gin-gonic/gin"
	"github.com/go-audio/audio"
	"github.com/go-audio/wav"
	"github.com/gorilla/websocket"
)

// Handler 声纹识别HTTP处理器
type Handler struct {
	manager *Manager
}

// NewHandler 创建新的处理器
func NewHandler(manager *Manager) *Handler {
	return &Handler{
		manager: manager,
	}
}

// getUIDFromRequest 从请求中提取 UID
// 优先级：请求头 X-User-ID > 查询参数 uid > 表单字段 uid
func getUIDFromRequest(c *gin.Context) string {
	// 1. 从请求头获取
	if uid := c.GetHeader("X-User-ID"); uid != "" {
		return uid
	}

	// 2. 从查询参数获取
	if uid := c.Query("uid"); uid != "" {
		return uid
	}

	// 3. 从表单字段获取
	if uid := c.PostForm("uid"); uid != "" {
		return uid
	}

	// 4. 从认证中间件获取（如果存在）
	if uid, exists := c.Get("user_id"); exists {
		if uidStr, ok := uid.(string); ok && uidStr != "" {
			return uidStr
		}
	}

	return ""
}

// getAgentIDFromRequest 从请求中提取 Agent ID
// 优先级：请求头 X-Agent-ID > 查询参数 agent_id > 表单字段 agent_id
func getAgentIDFromRequest(c *gin.Context) string {
	// 1. 从请求头获取
	if agentID := c.GetHeader("X-Agent-ID"); agentID != "" {
		return agentID
	}

	// 2. 从查询参数获取
	if agentID := c.Query("agent_id"); agentID != "" {
		return agentID
	}

	// 3. 从表单字段获取
	if agentID := c.PostForm("agent_id"); agentID != "" {
		return agentID
	}

	// 4. 从认证中间件获取（如果存在）
	if agentID, exists := c.Get("agent_id"); exists {
		if agentIDStr, ok := agentID.(string); ok && agentIDStr != "" {
			return agentIDStr
		}
	}

	return ""
}

// RegisterRoutes 注册路由
func (h *Handler) RegisterRoutes(router *gin.Engine) {
	speakerGroup := router.Group("/api/v1/speaker")
	{
		// 声纹注册
		speakerGroup.POST("/register", h.RegisterSpeaker)

		// 声纹识别
		speakerGroup.POST("/identify", h.IdentifySpeaker)

		// 声纹验证
		speakerGroup.POST("/verify/:speaker_id", h.VerifySpeaker)

		// 获取所有说话人
		speakerGroup.GET("/list", h.GetAllSpeakers)

		// 删除说话人（支持两种方式：查询参数 uuid 或路径参数 speaker_id）
		speakerGroup.DELETE("/:speaker_id", h.DeleteSpeaker) // 支持 DELETE /api/v1/speaker/:speaker_id

		// 获取数据库统计信息
		speakerGroup.GET("/stats", h.GetStats)

		//Base64 注册与识别接口
		speakerGroup.POST("/register_base64", h.RegisterSpeakerBase64)
		speakerGroup.POST("/identify_base64", h.IdentifySpeakerBase64)

		// WebSocket 流式识别接口
		speakerGroup.GET("/identify_ws", h.IdentifySpeakerWebSocket)
	}
}

// RegisterSpeaker 注册声纹
func (h *Handler) RegisterSpeaker(c *gin.Context) {
	// 获取 UID
	uid := getUIDFromRequest(c)
	if uid == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "uid is required (X-User-ID header, uid query param, or uid form field)",
		})
		return
	}

	// 获取 Agent ID（必填）
	agentID := getAgentIDFromRequest(c)
	if agentID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "agent_id is required (X-Agent-ID header, agent_id query param, or agent_id form field)",
		})
		return
	}

	// 获取表单数据
	speakerID := c.PostForm("speaker_id")
	speakerName := c.PostForm("speaker_name")
	uuid := c.PostForm("uuid") // 新增：UUID 参数

	if speakerID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "speaker_id is required",
		})
		return
	}

	if speakerName == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "speaker_name is required",
		})
		return
	}

	if uuid == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "uuid is required",
		})
		return
	}

	// 获取音频文件
	file, header, err := c.Request.FormFile("audio")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "audio file is required",
		})
		return
	}
	defer file.Close()

	// 解析音频数据
	audioData, sampleRate, err := h.parseAudioFile(file, header)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": fmt.Sprintf("failed to parse audio file: %v", err),
		})
		return
	}

	// 使用VAD过滤静音，保留前后100ms的静音
	filteredAudio, err := h.manager.FilterSilenceWithVADKeepEdges(audioData, sampleRate)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("failed to filter silence: %v", err),
		})
		return
	}

	// 注册声纹（使用过滤后的音频）
	err = h.manager.RegisterSpeaker(uid, agentID, speakerID, speakerName, uuid, filteredAudio, sampleRate)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("failed to register speaker: %v", err),
		})
		return
	}

	// 保存音频文件（异步保存，不阻塞响应）
	go func() {
		if err := saveRegisterAudioToWAV(filteredAudio, sampleRate, uid, agentID); err != nil {
			logger.Warnf("Failed to save register audio file: %v", err)
		} else {
			logger.Infof("Register audio file saved successfully, samples: %d", len(filteredAudio))
		}
	}()

	c.JSON(http.StatusOK, gin.H{
		"message":      "Speaker registered successfully",
		"uid":          uid,
		"agent_id":     agentID,
		"speaker_id":   speakerID,
		"speaker_name": speakerName,
		"uuid":         uuid,
	})
}

// IdentifySpeaker 识别声纹
func (h *Handler) IdentifySpeaker(c *gin.Context) {
	// 获取 UID（可选）
	uid := getUIDFromRequest(c)

	// 获取 Agent ID（可选）
	agentID := getAgentIDFromRequest(c)

	// 获取 speaker_id 参数（可选）
	speakerID := c.Query("speaker_id")
	if speakerID == "" {
		speakerID = c.PostForm("speaker_id")
	}

	// 获取 speaker_name 参数（可选）
	speakerName := c.Query("speaker_name")
	if speakerName == "" {
		speakerName = c.PostForm("speaker_name")
	}

	// 获取音频文件
	file, header, err := c.Request.FormFile("audio")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "audio file is required",
		})
		return
	}
	defer file.Close()

	// 解析音频数据
	audioData, sampleRate, err := h.parseAudioFile(file, header)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": fmt.Sprintf("failed to parse audio file: %v", err),
		})
		return
	}

	// 获取阈值参数（可选）
	var threshold float32
	if thresholdStr := c.Query("threshold"); thresholdStr != "" {
		if parsed, err := parseFloat32(thresholdStr); err == nil && parsed > 0 {
			threshold = parsed
		}
	} else if thresholdStr := c.PostForm("threshold"); thresholdStr != "" {
		if parsed, err := parseFloat32(thresholdStr); err == nil && parsed > 0 {
			threshold = parsed
		}
	}

	// 识别声纹（如果提供了阈值则使用，否则使用默认值）
	var result *IdentifyResult
	if threshold > 0 {
		result, err = h.manager.IdentifySpeaker(uid, agentID, speakerID, speakerName, audioData, sampleRate, threshold)
	} else {
		result, err = h.manager.IdentifySpeaker(uid, agentID, speakerID, speakerName, audioData, sampleRate)
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("failed to identify speaker: %v", err),
		})
		return
	}

	c.JSON(http.StatusOK, result)
}

// VerifySpeaker 验证声纹
func (h *Handler) VerifySpeaker(c *gin.Context) {
	// 获取 UID
	uid := getUIDFromRequest(c)
	if uid == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "uid is required (X-User-ID header, uid query param, or uid form field)",
		})
		return
	}

	// 获取 Agent ID（可选）
	agentID := getAgentIDFromRequest(c)

	speakerID := c.Param("speaker_id")
	if speakerID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "speaker_id is required",
		})
		return
	}

	// 获取音频文件
	file, header, err := c.Request.FormFile("audio")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "audio file is required",
		})
		return
	}
	defer file.Close()

	// 解析音频数据
	audioData, sampleRate, err := h.parseAudioFile(file, header)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": fmt.Sprintf("failed to parse audio file: %v", err),
		})
		return
	}

	// 验证声纹
	result, err := h.manager.VerifySpeaker(uid, agentID, speakerID, audioData, sampleRate)
	if err != nil {
		if strings.Contains(err.Error(), "belongs to different uid") {
			c.JSON(http.StatusForbidden, gin.H{
				"error": err.Error(),
			})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("failed to verify speaker: %v", err),
		})
		return
	}

	c.JSON(http.StatusOK, result)
}

// GetAllSpeakers 获取所有说话人
func (h *Handler) GetAllSpeakers(c *gin.Context) {
	// 获取 UID
	uid := getUIDFromRequest(c)
	if uid == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "uid is required (X-User-ID header, uid query param, or uid form field)",
		})
		return
	}

	// 获取 Agent ID（可选）
	agentID := getAgentIDFromRequest(c)

	speakers := h.manager.GetAllSpeakers(uid, agentID)
	c.JSON(http.StatusOK, gin.H{
		"uid":      uid,
		"agent_id": agentID,
		"speakers": speakers,
		"total":    len(speakers),
	})
}

// DeleteSpeaker 删除说话人
// 支持两种方式：
// 1. 通过查询参数 uuid 删除：DELETE /api/v1/speaker?uuid=xxx
// 2. 通过路径参数 speaker_id 删除：DELETE /api/v1/speaker/:speaker_id（用于删除整个声纹组）
func (h *Handler) DeleteSpeaker(c *gin.Context) {
	// 获取 UID
	uid := getUIDFromRequest(c)
	if uid == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "uid is required (X-User-ID header, uid query param, or uid form field)",
		})
		return
	}

	// 获取 Agent ID（可选）
	agentID := getAgentIDFromRequest(c)

	// 优先使用 uuid 查询参数
	uuid := c.Query("uuid")
	if uuid != "" {
		// 通过 UUID 删除
		err := h.manager.DeleteSpeakerByUUID(uid, agentID, uuid)
		if err != nil {
			if strings.Contains(err.Error(), "not found") {
				c.JSON(http.StatusNotFound, gin.H{
					"error": err.Error(),
				})
				return
			}
			if strings.Contains(err.Error(), "belongs to different uid") {
				c.JSON(http.StatusForbidden, gin.H{
					"error": err.Error(),
				})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": fmt.Sprintf("failed to delete speaker: %v", err),
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"message":  "Speaker deleted successfully",
			"uid":      uid,
			"agent_id": agentID,
			"uuid":     uuid,
		})
		return
	}

	// 通过路径参数 speaker_id 删除（speaker_id 就是组名称）
	speakerID := c.Param("speaker_id")
	if speakerID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "uuid or speaker_id is required",
		})
		return
	}

	err := h.manager.DeleteSpeaker(uid, agentID, speakerID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			c.JSON(http.StatusNotFound, gin.H{
				"error": err.Error(),
			})
			return
		}
		if strings.Contains(err.Error(), "belongs to different uid") {
			c.JSON(http.StatusForbidden, gin.H{
				"error": err.Error(),
			})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("failed to delete speaker: %v", err),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message":    "Speaker deleted successfully",
		"uid":        uid,
		"agent_id":   agentID,
		"speaker_id": speakerID,
	})
}

// GetStats 获取数据库统计信息
func (h *Handler) GetStats(c *gin.Context) {
	// UID 是可选的，如果不提供则返回全局统计
	uid := getUIDFromRequest(c)
	// Agent ID 是可选的
	agentID := getAgentIDFromRequest(c)
	stats := h.manager.GetStats(uid, agentID)
	c.JSON(http.StatusOK, stats)
}

// parseAudioFile 解析音频文件
func (h *Handler) parseAudioFile(file multipart.File, header *multipart.FileHeader) ([]float32, int, error) {
	// 检查文件类型
	filename := strings.ToLower(header.Filename)
	if !strings.HasSuffix(filename, ".wav") {
		return nil, 0, fmt.Errorf("only WAV files are supported")
	}

	// 读取WAV文件
	decoder := wav.NewDecoder(file)
	if !decoder.IsValidFile() {
		return nil, 0, fmt.Errorf("invalid WAV file")
	}

	// 获取音频格式信息
	sampleRate := int(decoder.SampleRate)
	numChannels := int(decoder.NumChans)

	// 只支持单声道或立体声
	if numChannels > 2 {
		return nil, 0, fmt.Errorf("unsupported number of channels: %d", numChannels)
	}

	// 读取音频数据
	buffer, err := decoder.FullPCMBuffer()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to decode audio: %v", err)
	}

	// 转换为float32格式
	samples := make([]float32, len(buffer.Data))
	for i, sample := range buffer.Data {
		// 将int转换为float32，范围[-1.0, 1.0]
		samples[i] = float32(sample) / config.GlobalConfig.Audio.NormalizeFactor
	}

	// 如果是立体声，转换为单声道（取平均值）
	if numChannels == 2 {
		monoSamples := make([]float32, len(samples)/2)
		for i := 0; i < len(monoSamples); i++ {
			monoSamples[i] = (samples[i*2] + samples[i*2+1]) / 2.0
		}
		samples = monoSamples
	}

	return samples, sampleRate, nil
}

// 添加基于Base64的API接口（可选）

// RegisterSpeakerBase64 使用Base64编码的音频数据注册声纹
func (h *Handler) RegisterSpeakerBase64(c *gin.Context) {
	var req struct {
		SpeakerID   string `json:"speaker_id" binding:"required"`
		SpeakerName string `json:"speaker_name" binding:"required"`
		AudioData   string `json:"audio_data" binding:"required"` // Base64编码的WAV数据
		SampleRate  int    `json:"sample_rate" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	// 这里可以添加Base64解码和音频处理逻辑
	// 为简化示例，暂时跳过具体实现

	c.JSON(http.StatusNotImplemented, gin.H{
		"error": "Base64 API not implemented yet",
	})
}

// IdentifySpeakerBase64 使用Base64编码的音频数据识别声纹
func (h *Handler) IdentifySpeakerBase64(c *gin.Context) {
	var req struct {
		AudioData  string `json:"audio_data" binding:"required"` // Base64编码的WAV数据
		SampleRate int    `json:"sample_rate" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	// 这里可以添加Base64解码和音频处理逻辑
	// 为简化示例，暂时跳过具体实现

	c.JSON(http.StatusNotImplemented, gin.H{
		"error": "Base64 API not implemented yet",
	})
}

// WebSocketUpgrader WebSocket升级器
var WebSocketUpgrader = websocket.Upgrader{
	CheckOrigin:       func(r *http.Request) bool { return true },
	ReadBufferSize:    config.GlobalConfig.Server.WebSocket.ReadBufferSize,
	WriteBufferSize:   config.GlobalConfig.Server.WebSocket.WriteBufferSize,
	EnableCompression: config.GlobalConfig.Server.WebSocket.EnableCompression,
}

// IdentifySpeakerWebSocket WebSocket流式识别声纹
// 支持连接复用和多轮次识别：
// - 发送 {"action": "finish"} 完成当前轮次识别，返回结果后自动重置状态，准备下一轮
// - 发送 {"action": "cancel"} 取消当前轮次识别，重置状态，准备下一轮
// - 发送 {"action": "close"} 关闭连接
// - 支持 WebSocket 协议层 ping/pong 心跳保活（自动回复 pong 并刷新超时计时器）
func (h *Handler) IdentifySpeakerWebSocket(c *gin.Context) {
	// 升级为WebSocket连接
	conn, err := WebSocketUpgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		logger.Errorf("WebSocket upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	logger.Infof("WebSocket connection established for speaker identification (multi-round enabled)")

	// 获取采样率参数（默认16000）
	sampleRate := 16000
	if sr := c.Query("sample_rate"); sr != "" {
		if srInt, err := parseInt(sr); err == nil {
			sampleRate = srInt
			logger.Debugf("WebSocket: Using custom sample rate: %d Hz", sampleRate)
		} else {
			logger.Warnf("WebSocket: Invalid sample_rate parameter '%s', using default 16000", sr)
		}
	} else {
		logger.Debugf("WebSocket: No sample_rate parameter, using default: %d Hz", sampleRate)
	}

	// 获取阈值参数（可选）
	var threshold float32
	if thresholdStr := c.Query("threshold"); thresholdStr != "" {
		if parsed, err := parseFloat32(thresholdStr); err == nil && parsed > 0 {
			threshold = parsed
			logger.Debugf("WebSocket: Using custom threshold: %.4f", threshold)
		} else {
			logger.Warnf("WebSocket: Invalid threshold parameter '%s', using default", thresholdStr)
		}
	}

	// 获取 UID（从查询参数或请求头，可选）
	uid := getUIDFromRequest(c)

	// 获取 Agent ID（可选）
	agentID := getAgentIDFromRequest(c)

	// 获取 speaker_id 参数（可选）
	speakerID := c.Query("speaker_id")

	// 获取 speaker_name 参数（可选）
	speakerName := c.Query("speaker_name")

	// 创建流式识别器的辅助函数
	createIdentifier := func() *StreamingIdentifier {
		logger.Debugf("WebSocket: Creating streaming identifier for uid: %s, agent_id: %s, speaker_id: %s, speaker_name: %s, sample rate: %d Hz, threshold: %.4f", uid, agentID, speakerID, speakerName, sampleRate, threshold)
		if threshold > 0 {
			return h.manager.NewStreamingIdentifier(uid, agentID, speakerID, speakerName, sampleRate, threshold)
		}
		return h.manager.NewStreamingIdentifier(uid, agentID, speakerID, speakerName, sampleRate)
	}

	// 创建初始流式识别器
	identifier := createIdentifier()
	defer func() {
		if identifier != nil {
			identifier.Close()
		}
	}()

	// 设置读取超时
	wsConfig := config.GlobalConfig.Server.WebSocket
	if wsConfig.ReadTimeout > 0 {
		conn.SetReadDeadline(time.Now().Add(time.Duration(wsConfig.ReadTimeout) * time.Second))
		logger.Debugf("WebSocket: Set read timeout: %d seconds", wsConfig.ReadTimeout)
	}

	// 设置 WebSocket 协议层 ping handler，收到 ping 时刷新超时并自动回复 pong
	conn.SetPingHandler(func(appData string) error {
		logger.Debugf("WebSocket: Received protocol ping, refreshing timeout and sending pong")
		if wsConfig.ReadTimeout > 0 {
			conn.SetReadDeadline(time.Now().Add(time.Duration(wsConfig.ReadTimeout) * time.Second))
		}
		// 回复 pong（使用相同的 appData）
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(time.Second))
	})

	// 发送连接确认消息
	connectionMsg := map[string]interface{}{
		"type":        "connection",
		"message":     "WebSocket connected, ready for audio (multi-round enabled)",
		"sample_rate": sampleRate,
	}
	if err := conn.WriteJSON(connectionMsg); err != nil {
		logger.Errorf("Failed to send connection message: %v", err)
		return
	}
	logger.Debugf("WebSocket: Sent connection confirmation message: %+v", connectionMsg)

	// 音频缓冲区（用于保存音频文件）
	var audioBuffer []float32
	saveAudioEnabled := config.GlobalConfig.Speaker.SaveAudioOnFinish

	// 读取消息
	totalAudioSamples := 0
	audioChunkCount := 0
	roundCount := 0 // 识别轮次计数
	for {
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				logger.Warnf("WebSocket read error: %v", err)
			} else {
				logger.Debugf("WebSocket: Connection closed normally or read error: %v", err)
			}
			break
		}

		logger.Debugf("WebSocket: Received message, type=%d, size=%d bytes", messageType, len(message))

		// 刷新读超时
		if wsConfig.ReadTimeout > 0 {
			conn.SetReadDeadline(time.Now().Add(time.Duration(wsConfig.ReadTimeout) * time.Second))
		}

		// 检查消息大小
		if wsConfig.MaxMessageSize > 0 && len(message) > wsConfig.MaxMessageSize {
			logger.Warnf("WebSocket: Message too large: %d bytes (max: %d)", len(message), wsConfig.MaxMessageSize)
			conn.WriteJSON(map[string]interface{}{
				"type":    "error",
				"message": "message too large",
			})
			continue
		}

		// 处理文本消息（控制消息）
		if messageType == websocket.TextMessage {
			logger.Debugf("WebSocket: Received text message: %s", string(message))
			var controlMsg map[string]interface{}
			if err := json.Unmarshal(message, &controlMsg); err != nil {
				logger.Warnf("WebSocket: Failed to unmarshal text message: %v", err)
				continue
			}

			logger.Debugf("WebSocket: Parsed control message: %+v", controlMsg)

			if action, ok := controlMsg["action"].(string); ok {
				logger.Debugf("WebSocket: Control action: %s", action)
				switch action {
				case "finish":
					// 完成当前轮次识别
					roundCount++
					logger.Debugf("WebSocket: Finish action received (round %d), total audio samples: %d, chunks: %d", roundCount, totalAudioSamples, audioChunkCount)
					logger.Debugf("WebSocket: Calling FinishAndIdentify()...")
					result, err := identifier.FinishAndIdentify()
					if err != nil {
						logger.Errorf("WebSocket: FinishAndIdentify failed: %v", err)
						conn.WriteJSON(map[string]interface{}{
							"type":    "error",
							"message": err.Error(),
							"round":   roundCount,
						})
						// 重置状态，准备下一轮（即使出错也允许继续）
						identifier.Close()
						identifier = createIdentifier()
						audioBuffer = nil
						totalAudioSamples = 0
						audioChunkCount = 0
						continue
					}

					// 如果启用了保存音频，保存音频文件
					if saveAudioEnabled && len(audioBuffer) > 0 {
						// 复制音频数据，避免在异步执行时数据被修改
						audioDataCopy := make([]float32, len(audioBuffer))
						copy(audioDataCopy, audioBuffer)
						currentRound := roundCount
						go func() {
							// 异步保存，不阻塞响应
							if err := saveAudioToWAV(audioDataCopy, sampleRate, uid, agentID); err != nil {
								logger.Warnf("WebSocket: Failed to save audio file (round %d): %v", currentRound, err)
							} else {
								logger.Infof("WebSocket: Audio file saved successfully (round %d), samples: %d", currentRound, len(audioDataCopy))
							}
						}()
					}

					// 发送识别结果
					conn.WriteJSON(map[string]interface{}{
						"type":   "result",
						"result": result,
						"round":  roundCount,
					})
					logger.Infof("WebSocket: Sent identification result to client (round %d)", roundCount)

					// 重置状态，准备下一轮识别
					identifier.Close()
					identifier = createIdentifier()
					audioBuffer = nil
					totalAudioSamples = 0
					audioChunkCount = 0

					// 发送就绪消息，通知客户端可以开始下一轮
					conn.WriteJSON(map[string]interface{}{
						"type":    "ready",
						"message": "Ready for next round",
						"round":   roundCount + 1,
					})
					logger.Debugf("WebSocket: Reset state, ready for round %d", roundCount+1)

				case "cancel":
					// 取消当前轮次识别，重置状态
					logger.Infof("WebSocket: Cancel action received (round %d), resetting state", roundCount+1)
					identifier.Close()
					identifier = createIdentifier()
					audioBuffer = nil
					totalAudioSamples = 0
					audioChunkCount = 0

					conn.WriteJSON(map[string]interface{}{
						"type":    "cancelled",
						"message": "Current round cancelled, ready for next round",
						"round":   roundCount + 1,
					})

				case "close":
					// 显式关闭连接
					logger.Infof("WebSocket: Close action received, closing connection after %d rounds", roundCount)
					conn.WriteJSON(map[string]interface{}{
						"type":         "closing",
						"message":      "Connection closing",
						"total_rounds": roundCount,
					})
					return

				default:
					logger.Warnf("WebSocket: Unknown action: %s", action)
				}
			} else {
				logger.Debugf("WebSocket: Text message without action field: %+v", controlMsg)
			}
			continue
		}

		// 处理二进制消息（音频数据）
		if messageType == websocket.BinaryMessage {
			logger.Debugf("WebSocket: Received binary message: %d bytes", len(message))

			// 将字节数据转换为float32数组
			// 假设输入是float32的二进制数据（小端序）
			if len(message)%4 != 0 {
				logger.Warnf("WebSocket: Invalid audio data length: %d bytes (not divisible by 4)", len(message))
				conn.WriteJSON(map[string]interface{}{
					"type":    "error",
					"message": "invalid audio data length",
				})
				continue
			}

			sampleCount := len(message) / 4
			audioData := make([]float32, sampleCount)
			for i := 0; i < len(audioData); i++ {
				bits := binary.LittleEndian.Uint32(message[i*4 : (i+1)*4])
				audioData[i] = math.Float32frombits(bits)
			}

			// 检查音频数据范围
			var minVal, maxVal float32 = audioData[0], audioData[0]
			for _, v := range audioData {
				if v < minVal {
					minVal = v
				}
				if v > maxVal {
					maxVal = v
				}
			}
			logger.Debugf("WebSocket: Audio chunk #%d: samples=%d, duration=%.2fms, range=[%.4f, %.4f]",
				audioChunkCount+1, sampleCount, float64(sampleCount)/float64(sampleRate)*1000, minVal, maxVal)

			// 接收音频数据块
			if err := identifier.AcceptAudio(audioData); err != nil {
				logger.Errorf("WebSocket: Failed to accept audio chunk #%d: %v", audioChunkCount+1, err)
				conn.WriteJSON(map[string]interface{}{
					"type":    "error",
					"message": err.Error(),
				})
				return
			}

			// 如果启用了保存音频，将数据追加到缓冲区
			if saveAudioEnabled {
				audioBuffer = append(audioBuffer, audioData...)
			}

			totalAudioSamples += sampleCount
			audioChunkCount++

			// 每10个块打印一次统计信息
			if audioChunkCount%10 == 0 {
				logger.Debugf("WebSocket: Audio progress - chunks: %d, total samples: %d, total duration: %.2fs",
					audioChunkCount, totalAudioSamples, float64(totalAudioSamples)/float64(sampleRate))
			}

			// 发送确认消息（可选）
			ackMsg := map[string]interface{}{
				"type":        "audio_received",
				"samples":     len(audioData),
				"duration_ms": float64(len(audioData)) / float64(sampleRate) * 1000,
			}
			if err := conn.WriteJSON(ackMsg); err != nil {
				logger.Warnf("WebSocket: Failed to send audio_received ack: %v", err)
			}
		} else {
			logger.Debugf("WebSocket: Received unknown message type: %d", messageType)
		}
	}

	logger.Infof("WebSocket: Connection closed, total rounds: %d, current round audio chunks: %d, samples: %d, duration: %.2fs",
		roundCount, audioChunkCount, totalAudioSamples, float64(totalAudioSamples)/float64(sampleRate))
}

// parseInt 解析整数（辅助函数）
func parseInt(s string) (int, error) {
	var result int
	_, err := fmt.Sscanf(s, "%d", &result)
	return result, err
}

// parseFloat32 解析浮点数（辅助函数）
func parseFloat32(s string) (float32, error) {
	var result float32
	_, err := fmt.Sscanf(s, "%f", &result)
	return result, err
}

// saveAudioToWAV 将音频数据保存为 WAV 文件
func saveAudioToWAV(audioData []float32, sampleRate int, uid, agentID string) error {
	if len(audioData) == 0 {
		return fmt.Errorf("audio data is empty")
	}

	// 确定保存目录
	saveDir := config.GlobalConfig.Speaker.AudioSaveDir
	if saveDir == "" {
		// 如果未指定，使用 data_dir
		saveDir = config.GlobalConfig.Speaker.DataDir
	}
	if saveDir == "" {
		saveDir = "data/speaker"
	}

	// 创建保存目录（如果不存在）
	if err := os.MkdirAll(saveDir, 0755); err != nil {
		return fmt.Errorf("failed to create save directory: %w", err)
	}

	// 生成文件名：时间戳_uid_agentid.wav
	timestamp := time.Now().Format("20060102_150405")
	var filename string
	if uid != "" && agentID != "" {
		filename = fmt.Sprintf("%s_%s_%s.wav", timestamp, uid, agentID)
	} else if uid != "" {
		filename = fmt.Sprintf("%s_%s.wav", timestamp, uid)
	} else if agentID != "" {
		filename = fmt.Sprintf("%s_%s.wav", timestamp, agentID)
	} else {
		filename = fmt.Sprintf("%s.wav", timestamp)
	}

	// 清理文件名中的非法字符
	filename = strings.ReplaceAll(filename, "/", "_")
	filename = strings.ReplaceAll(filename, "\\", "_")
	filename = strings.ReplaceAll(filename, ":", "_")

	filePath := filepath.Join(saveDir, filename)

	// 创建文件
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// 将 float32 转换为 int16
	// float32 范围是 [-1.0, 1.0]，需要转换为 int16 范围 [-32768, 32767]
	int16Data := make([]int, len(audioData))
	normalizeFactor := config.GlobalConfig.Audio.NormalizeFactor
	for i, sample := range audioData {
		// 限制范围到 [-1.0, 1.0]
		if sample > 1.0 {
			sample = 1.0
		} else if sample < -1.0 {
			sample = -1.0
		}
		// 转换为 int16
		int16Data[i] = int(sample * normalizeFactor)
	}

	// 创建音频格式
	format := &audio.Format{
		NumChannels: 1, // 单声道
		SampleRate:  sampleRate,
	}

	// 创建 WAV 编码器
	encoder := wav.NewEncoder(file, format.SampleRate, 16, format.NumChannels, 1)

	// 创建音频缓冲区
	buf := &audio.IntBuffer{
		Format:         format,
		SourceBitDepth: 16,
		Data:           int16Data,
	}

	// 写入音频数据
	if err := encoder.Write(buf); err != nil {
		return fmt.Errorf("failed to write audio data: %w", err)
	}

	// 关闭编码器
	if err := encoder.Close(); err != nil {
		return fmt.Errorf("failed to close encoder: %w", err)
	}

	logger.Debugf("Saved audio file: %s, samples: %d, duration: %.2fs", filePath, len(audioData), float64(len(audioData))/float64(sampleRate))
	return nil
}

// saveRegisterAudioToWAV 将注册音频数据保存为 WAV 文件（文件名加前缀 "register_"）
func saveRegisterAudioToWAV(audioData []float32, sampleRate int, uid, agentID string) error {
	if len(audioData) == 0 {
		return fmt.Errorf("audio data is empty")
	}

	// 确定保存目录
	saveDir := config.GlobalConfig.Speaker.AudioSaveDir
	if saveDir == "" {
		// 如果未指定，使用 data_dir
		saveDir = config.GlobalConfig.Speaker.DataDir
	}
	if saveDir == "" {
		saveDir = "data/speaker"
	}

	// 创建保存目录（如果不存在）
	if err := os.MkdirAll(saveDir, 0755); err != nil {
		return fmt.Errorf("failed to create save directory: %w", err)
	}

	// 生成文件名：register_时间戳_uid_agentid.wav
	timestamp := time.Now().Format("20060102_150405")
	var filename string
	if uid != "" && agentID != "" {
		filename = fmt.Sprintf("register_%s_%s_%s.wav", timestamp, uid, agentID)
	} else if uid != "" {
		filename = fmt.Sprintf("register_%s_%s.wav", timestamp, uid)
	} else if agentID != "" {
		filename = fmt.Sprintf("register_%s_%s.wav", timestamp, agentID)
	} else {
		filename = fmt.Sprintf("register_%s.wav", timestamp)
	}

	// 清理文件名中的非法字符
	filename = strings.ReplaceAll(filename, "/", "_")
	filename = strings.ReplaceAll(filename, "\\", "_")
	filename = strings.ReplaceAll(filename, ":", "_")

	filePath := filepath.Join(saveDir, filename)

	// 创建文件
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// 将 float32 转换为 int16
	// float32 范围是 [-1.0, 1.0]，需要转换为 int16 范围 [-32768, 32767]
	int16Data := make([]int, len(audioData))
	normalizeFactor := config.GlobalConfig.Audio.NormalizeFactor
	for i, sample := range audioData {
		// 限制范围到 [-1.0, 1.0]
		if sample > 1.0 {
			sample = 1.0
		} else if sample < -1.0 {
			sample = -1.0
		}
		// 转换为 int16
		int16Data[i] = int(sample * normalizeFactor)
	}

	// 创建音频格式
	format := &audio.Format{
		NumChannels: 1, // 单声道
		SampleRate:  sampleRate,
	}

	// 创建 WAV 编码器
	encoder := wav.NewEncoder(file, format.SampleRate, 16, format.NumChannels, 1)

	// 创建音频缓冲区
	buf := &audio.IntBuffer{
		Format:         format,
		SourceBitDepth: 16,
		Data:           int16Data,
	}

	// 写入音频数据
	if err := encoder.Write(buf); err != nil {
		return fmt.Errorf("failed to write audio data: %w", err)
	}

	// 关闭编码器
	if err := encoder.Close(); err != nil {
		return fmt.Errorf("failed to close encoder: %w", err)
	}

	logger.Debugf("Saved register audio file: %s, samples: %d, duration: %.2fs", filePath, len(audioData), float64(len(audioData))/float64(sampleRate))
	return nil
}
