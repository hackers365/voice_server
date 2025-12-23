package speaker

import (
	"asr_server/config"
	"asr_server/internal/logger"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"mime/multipart"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
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

		// 删除说话人
		speakerGroup.DELETE("/:speaker_id", h.DeleteSpeaker)

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

	// 获取表单数据
	speakerID := c.PostForm("speaker_id")
	speakerName := c.PostForm("speaker_name")

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

	// 注册声纹
	err = h.manager.RegisterSpeaker(uid, speakerID, speakerName, audioData, sampleRate)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("failed to register speaker: %v", err),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message":      "Speaker registered successfully",
		"uid":          uid,
		"speaker_id":   speakerID,
		"speaker_name": speakerName,
	})
}

// IdentifySpeaker 识别声纹
func (h *Handler) IdentifySpeaker(c *gin.Context) {
	// 获取 UID
	uid := getUIDFromRequest(c)
	if uid == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "uid is required (X-User-ID header, uid query param, or uid form field)",
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
		result, err = h.manager.IdentifySpeaker(uid, audioData, sampleRate, threshold)
	} else {
		result, err = h.manager.IdentifySpeaker(uid, audioData, sampleRate)
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
	result, err := h.manager.VerifySpeaker(uid, speakerID, audioData, sampleRate)
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

	speakers := h.manager.GetAllSpeakers(uid)
	c.JSON(http.StatusOK, gin.H{
		"uid":      uid,
		"speakers": speakers,
		"total":    len(speakers),
	})
}

// DeleteSpeaker 删除说话人
func (h *Handler) DeleteSpeaker(c *gin.Context) {
	// 获取 UID
	uid := getUIDFromRequest(c)
	if uid == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "uid is required (X-User-ID header, uid query param, or uid form field)",
		})
		return
	}

	speakerID := c.Param("speaker_id")
	if speakerID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "speaker_id is required",
		})
		return
	}

	err := h.manager.DeleteSpeaker(uid, speakerID)
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
		"speaker_id": speakerID,
	})
}

// GetStats 获取数据库统计信息
func (h *Handler) GetStats(c *gin.Context) {
	// UID 是可选的，如果不提供则返回全局统计
	uid := getUIDFromRequest(c)
	stats := h.manager.GetStats(uid)
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
func (h *Handler) IdentifySpeakerWebSocket(c *gin.Context) {
	// 升级为WebSocket连接
	conn, err := WebSocketUpgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		logger.Errorf("WebSocket upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	logger.Infof("WebSocket connection established for speaker identification")

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

	// 获取 UID（从查询参数或请求头）
	uid := getUIDFromRequest(c)
	if uid == "" {
		logger.Warnf("WebSocket: No uid provided, using empty uid")
		// 可以选择返回错误或使用默认值
		// 这里为了兼容性，允许空 UID，但会在识别时失败
	}

	// 创建流式识别器（如果提供了阈值则使用，否则使用默认值）
	logger.Debugf("WebSocket: Creating streaming identifier for uid: %s, sample rate: %d Hz, threshold: %.4f", uid, sampleRate, threshold)
	var identifier *StreamingIdentifier
	if threshold > 0 {
		identifier = h.manager.NewStreamingIdentifier(uid, sampleRate, threshold)
	} else {
		identifier = h.manager.NewStreamingIdentifier(uid, sampleRate)
	}
	defer identifier.Close()

	// 设置读取超时
	wsConfig := config.GlobalConfig.Server.WebSocket
	if wsConfig.ReadTimeout > 0 {
		conn.SetReadDeadline(time.Now().Add(time.Duration(wsConfig.ReadTimeout) * time.Second))
		logger.Debugf("WebSocket: Set read timeout: %d seconds", wsConfig.ReadTimeout)
	}

	// 发送连接确认消息
	connectionMsg := map[string]interface{}{
		"type":        "connection",
		"message":     "WebSocket connected, ready for audio",
		"sample_rate": sampleRate,
	}
	if err := conn.WriteJSON(connectionMsg); err != nil {
		logger.Errorf("Failed to send connection message: %v", err)
		return
	}
	logger.Debugf("WebSocket: Sent connection confirmation message: %+v", connectionMsg)

	// 读取消息
	totalAudioSamples := 0
	audioChunkCount := 0
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
					// 完成识别
					logger.Debugf("WebSocket: Finish action received, total audio samples: %d, chunks: %d", totalAudioSamples, audioChunkCount)
					logger.Debugf("WebSocket: Calling FinishAndIdentify()...")
					result, err := identifier.FinishAndIdentify()
					if err != nil {
						logger.Errorf("WebSocket: FinishAndIdentify failed: %v", err)
						conn.WriteJSON(map[string]interface{}{
							"type":    "error",
							"message": err.Error(),
						})
						return
					}
					logger.Debugf("WebSocket: Identification result: identified=%v, speaker_id=%s, confidence=%.4f, threshold=%.4f",
						result.Identified, result.SpeakerID, result.Confidence, result.Threshold)
					conn.WriteJSON(map[string]interface{}{
						"type":   "result",
						"result": result,
					})
					logger.Infof("WebSocket: Sent identification result to client")
					return
				case "cancel":
					// 取消识别
					logger.Infof("WebSocket: Cancel action received, closing connection")
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

	logger.Infof("WebSocket: Connection closed, total audio chunks: %d, total samples: %d, total duration: %.2fs",
		audioChunkCount, totalAudioSamples, float64(totalAudioSamples)/float64(sampleRate))
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
