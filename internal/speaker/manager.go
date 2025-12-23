package speaker

import (
	"fmt"
	"os"
	"sync"
	"time"

	"asr_server/internal/logger"

	sherpa "github.com/k2-fsa/sherpa-onnx-go/sherpa_onnx"
)

// Manager 声纹识别管理器
type Manager struct {
	extractor    *sherpa.SpeakerEmbeddingExtractor
	threshold    float32
	embeddingDim int
	dataDir      string

	// Qdrant 向量数据库客户端（唯一存储）
	vectorDB      *QdrantVectorDB
	vectorDBMutex sync.RWMutex
}

// Config 声纹识别配置
type Config struct {
	ModelPath  string  `json:"model_path"`
	NumThreads int     `json:"num_threads"`
	Provider   string  `json:"provider"`
	Threshold  float32 `json:"threshold"`
	DataDir    string  `json:"data_dir"` // 保留用于其他用途（如临时文件）

	// 向量数据库配置（必需）
	VectorDB struct {
		Host           string `json:"host"`            // Qdrant 地址，默认 localhost
		Port           int    `json:"port"`            // Qdrant 端口，默认 6334
		CollectionName string `json:"collection_name"` // Collection 名称，默认 speaker_embeddings
	} `json:"vector_db"`
}

// NewManager 创建声纹识别管理器
func NewManager(config *Config) (*Manager, error) {
	// 确保数据目录存在（用于其他用途，如临时文件）
	if config.DataDir != "" {
		if err := os.MkdirAll(config.DataDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create data directory: %v", err)
		}
	}

	// 创建声纹特征提取器配置
	extractorConfig := &sherpa.SpeakerEmbeddingExtractorConfig{
		Model:      config.ModelPath,
		NumThreads: config.NumThreads,
		Debug:      0,
		Provider:   config.Provider,
	}

	// 创建声纹特征提取器
	extractor := sherpa.NewSpeakerEmbeddingExtractor(extractorConfig)
	if extractor == nil {
		return nil, fmt.Errorf("failed to create speaker embedding extractor")
	}

	// 获取特征维度
	dim := extractor.Dim()
	logger.Infof("Speaker embedding dimension: %d", dim)

	// 初始化 Qdrant 向量数据库
	qdrantConfig := &QdrantConfig{
		Host:           config.VectorDB.Host,
		Port:           config.VectorDB.Port,
		CollectionName: config.VectorDB.CollectionName,
	}

	// 设置默认值
	if qdrantConfig.Host == "" {
		qdrantConfig.Host = "localhost"
	}
	if qdrantConfig.Port == 0 {
		qdrantConfig.Port = 6334
	}
	if qdrantConfig.CollectionName == "" {
		qdrantConfig.CollectionName = "speaker_embeddings"
	}

	vectorDB, err := NewQdrantVectorDB(qdrantConfig, dim)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize vector database: %v", err)
	}

	manager := &Manager{
		extractor:    extractor,
		threshold:    config.Threshold,
		embeddingDim: dim,
		dataDir:      config.DataDir,
		vectorDB:     vectorDB,
	}

	logger.Infof("✅ Speaker Manager initialized with Qdrant vector database")
	return manager, nil
}

// Close 关闭管理器并释放资源
func (m *Manager) Close() {
	// 关闭向量数据库连接
	if m.vectorDB != nil {
		m.vectorDB.Close()
	}

	// 释放提取器
	if m.extractor != nil {
		sherpa.DeleteSpeakerEmbeddingExtractor(m.extractor)
	}

	logger.Infof("Speaker Manager closed, all resources released")
}

// extractEmbedding 从音频数据提取声纹特征（私有方法）
func (m *Manager) extractEmbedding(audioData []float32, sampleRate int) ([]float32, error) {
	// 创建音频流
	stream := m.extractor.CreateStream()
	defer sherpa.DeleteOnlineStream(stream)

	// 接受音频数据
	stream.AcceptWaveform(sampleRate, audioData)
	stream.InputFinished()

	// 检查是否准备就绪
	if !m.extractor.IsReady(stream) {
		return nil, fmt.Errorf("insufficient audio data for embedding extraction")
	}

	// 提取特征
	embedding := m.extractor.Compute(stream)
	if len(embedding) == 0 {
		return nil, fmt.Errorf("failed to extract embedding")
	}

	// 注意：不需要手动归一化向量
	// Qdrant 在使用 Distance_Cosine 时会自动归一化向量（存储和查询时都会自动处理）
	// 这样可以确保向量存储和搜索的一致性，并提高搜索效率

	return embedding, nil
}

// ExtractEmbedding 从音频数据提取声纹特征（公开方法，供外部调用）
func (m *Manager) ExtractEmbedding(audioData []float32, sampleRate int) ([]float32, error) {
	return m.extractEmbedding(audioData, sampleRate)
}

// GetEmbeddingDim 获取 embedding 维度
func (m *Manager) GetEmbeddingDim() int {
	return m.embeddingDim
}

// RegisterSpeaker 注册声纹（支持 UID 维度隔离）
func (m *Manager) RegisterSpeaker(uid, speakerID, speakerName string, audioData []float32, sampleRate int) error {
	if uid == "" {
		return fmt.Errorf("uid is required")
	}

	// 提取声纹特征
	embedding, err := m.extractEmbedding(audioData, sampleRate)
	if err != nil {
		return fmt.Errorf("failed to extract embedding: %v", err)
	}

	// 验证嵌入向量维度
	if len(embedding) != m.embeddingDim {
		return fmt.Errorf("embedding dimension mismatch: expected %d, got %d", m.embeddingDim, len(embedding))
	}

	// 查询该 speaker 已存在的样本数量（用于确定 sample_index）
	sampleIndex, err := m.vectorDB.GetSpeakerSampleCount(uid, speakerID)
	if err != nil {
		// 如果查询失败，可能是 speaker 不存在，从 0 开始
		sampleIndex = 0
	}

	// 插入到 Qdrant 向量数据库
	now := time.Now().Unix()
	err = m.vectorDB.Insert(uid, speakerID, speakerName, embedding, sampleIndex, now, now)
	if err != nil {
		return fmt.Errorf("failed to insert to vector database: %v", err)
	}

	logger.Infof("Successfully registered speaker %s (%s) for uid %s, sample index: %d",
		speakerID, speakerName, uid, sampleIndex)
	return nil
}

// IdentifySpeaker 识别声纹（支持 UID 维度隔离）
// threshold: 识别阈值，如果 <= 0 则使用默认阈值
func (m *Manager) IdentifySpeaker(uid string, audioData []float32, sampleRate int, threshold ...float32) (*IdentifyResult, error) {
	if uid == "" {
		return nil, fmt.Errorf("uid is required")
	}

	// 确定使用的阈值：如果传入了有效的阈值（> 0），使用传入的；否则使用默认阈值
	useThreshold := m.threshold
	if len(threshold) > 0 && threshold[0] > 0 {
		useThreshold = threshold[0]
	}

	// 提取声纹特征
	embedding, err := m.extractEmbedding(audioData, sampleRate)
	if err != nil {
		return nil, fmt.Errorf("failed to extract embedding: %v", err)
	}

	// 在 Qdrant 向量数据库中搜索（按 UID 过滤，返回 top 1）
	results, err := m.vectorDB.Search(uid, embedding, useThreshold, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to search in vector database: %v", err)
	}

	result := &IdentifyResult{
		Identified:  false,
		SpeakerID:   "",
		SpeakerName: "",
		Confidence:  0.0,
		Threshold:   useThreshold,
	}

	if len(results) > 0 {
		bestMatch := results[0]
		result.Identified = true
		result.SpeakerID = bestMatch.SpeakerID
		result.SpeakerName = bestMatch.SpeakerName
		result.Confidence = bestMatch.Confidence
	}

	return result, nil
}

// VerifySpeaker 验证声纹（支持 UID 维度隔离）
func (m *Manager) VerifySpeaker(uid, speakerID string, audioData []float32, sampleRate int) (*VerifyResult, error) {
	if uid == "" {
		return nil, fmt.Errorf("uid is required")
	}

	// 提取声纹特征
	embedding, err := m.extractEmbedding(audioData, sampleRate)
	if err != nil {
		return nil, fmt.Errorf("failed to extract embedding: %v", err)
	}

	// 在 Qdrant 中搜索该 speaker 的所有样本
	// Filter: uid = xxx AND speaker_id = xxx
	results, err := m.vectorDB.SearchWithFilter(uid, speakerID, embedding, m.threshold, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to search in vector database: %v", err)
	}

	verified := len(results) > 0
	confidence := float32(0.0)
	speakerName := ""

	if verified {
		confidence = results[0].Confidence
		speakerName = results[0].SpeakerName
	} else {
		// 如果未找到，尝试获取 speaker 信息（验证是否存在）
		speakerInfo, err := m.vectorDB.GetSpeakerInfo(uid, speakerID)
		if err != nil {
			return nil, fmt.Errorf("speaker %s not found", speakerID)
		}
		speakerName = speakerInfo.Name
	}

	return &VerifyResult{
		SpeakerID:   speakerID,
		SpeakerName: speakerName,
		Verified:    verified,
		Confidence:  confidence,
		Threshold:   m.threshold,
	}, nil
}

// GetAllSpeakers 获取指定 UID 的所有注册的说话人
func (m *Manager) GetAllSpeakers(uid string) []*SpeakerInfo {
	speakers, err := m.vectorDB.GetAllSpeakers(uid)
	if err != nil {
		logger.Errorf("Failed to get speakers from vector database: %v", err)
		return []*SpeakerInfo{}
	}
	return speakers
}

// DeleteSpeaker 删除说话人（支持 UID 维度隔离）
func (m *Manager) DeleteSpeaker(uid, speakerID string) error {
	if uid == "" {
		return fmt.Errorf("uid is required")
	}

	// 从 Qdrant 向量数据库删除
	err := m.vectorDB.DeleteSpeaker(uid, speakerID)
	if err != nil {
		return fmt.Errorf("failed to delete from vector database: %v", err)
	}

	logger.Infof("Successfully deleted speaker %s for uid %s", speakerID, uid)
	return nil
}

// GetStats 获取统计信息（用于主服务监控，支持按 UID 过滤）
func (m *Manager) GetStats(uid string) map[string]interface{} {
	stats := m.GetDatabaseStats(uid)

	return map[string]interface{}{
		"speaker_count": stats.TotalSpeakers,
		"total_samples": stats.TotalSamples,
		"embedding_dim": stats.EmbeddingDim,
		"threshold":     stats.Threshold,
		"version":       stats.Version,
		"last_updated":  stats.UpdatedAt.Format(time.RFC3339),
	}
}

// GetDatabaseStats 获取数据库统计信息（支持按 UID 过滤）
func (m *Manager) GetDatabaseStats(uid string) *DatabaseStats {
	// 从向量数据库获取统计信息
	speakers, err := m.vectorDB.GetAllSpeakers(uid)
	if err != nil {
		logger.Errorf("Failed to get speakers from vector database: %v", err)
		return &DatabaseStats{
			TotalSpeakers: 0,
			TotalSamples:  0,
			EmbeddingDim:  m.embeddingDim,
			Threshold:     m.threshold,
			Version:       "2.0.0",
			UpdatedAt:     time.Now(),
		}
	}

	totalSamples := 0
	for _, speaker := range speakers {
		totalSamples += speaker.SampleCount
	}

	return &DatabaseStats{
		TotalSpeakers: len(speakers),
		TotalSamples:  totalSamples,
		EmbeddingDim:  m.embeddingDim,
		Threshold:     m.threshold,
		Version:       "2.0.0",
		UpdatedAt:     time.Now(),
	}
}

// 响应结构体定义
type IdentifyResult struct {
	Identified  bool    `json:"identified"`
	SpeakerID   string  `json:"speaker_id"`
	SpeakerName string  `json:"speaker_name"`
	Confidence  float32 `json:"confidence"`
	Threshold   float32 `json:"threshold"`
}

type VerifyResult struct {
	SpeakerID   string  `json:"speaker_id"`
	SpeakerName string  `json:"speaker_name"`
	Verified    bool    `json:"verified"`
	Confidence  float32 `json:"confidence"`
	Threshold   float32 `json:"threshold"`
}

type SpeakerInfo struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	SampleCount int       `json:"sample_count"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type DatabaseStats struct {
	TotalSpeakers int       `json:"total_speakers"`
	TotalSamples  int       `json:"total_samples"`
	EmbeddingDim  int       `json:"embedding_dim"`
	Threshold     float32   `json:"threshold"`
	Version       string    `json:"version"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// StreamingIdentifier 流式声纹识别器
type StreamingIdentifier struct {
	manager    *Manager
	uid        string // 用户ID
	stream     *sherpa.OnlineStream
	sampleRate int
	threshold  float32 // 识别阈值，如果 <= 0 则使用默认阈值
	mutex      sync.Mutex
	isFinished bool
}

// NewStreamingIdentifier 创建流式识别器（支持 UID 维度隔离）
// threshold: 识别阈值，如果 <= 0 则使用默认阈值
func (m *Manager) NewStreamingIdentifier(uid string, sampleRate int, threshold ...float32) *StreamingIdentifier {
	stream := m.extractor.CreateStream()
	useThreshold := m.threshold
	if len(threshold) > 0 && threshold[0] > 0 {
		useThreshold = threshold[0]
	}
	return &StreamingIdentifier{
		manager:    m,
		uid:        uid,
		stream:     stream,
		sampleRate: sampleRate,
		threshold:  useThreshold,
		isFinished: false,
	}
}

// AcceptAudio 接收音频数据块（流式输入）
func (si *StreamingIdentifier) AcceptAudio(audioData []float32) error {
	si.mutex.Lock()
	defer si.mutex.Unlock()

	if si.isFinished {
		return fmt.Errorf("stream already finished")
	}

	if si.stream == nil {
		return fmt.Errorf("stream is nil")
	}

	// 接受音频数据块
	si.stream.AcceptWaveform(si.sampleRate, audioData)
	return nil
}

// FinishAndIdentify 完成输入并识别声纹
func (si *StreamingIdentifier) FinishAndIdentify() (*IdentifyResult, error) {
	si.mutex.Lock()
	defer si.mutex.Unlock()

	if si.isFinished {
		return nil, fmt.Errorf("stream already finished")
	}

	if si.stream == nil {
		return nil, fmt.Errorf("stream is nil")
	}

	// 标记输入完成
	si.stream.InputFinished()
	si.isFinished = true

	// 检查是否准备就绪
	if !si.manager.extractor.IsReady(si.stream) {
		si.cleanup()
		return nil, fmt.Errorf("insufficient audio data for embedding extraction")
	}

	// 提取特征
	embedding := si.manager.extractor.Compute(si.stream)
	if len(embedding) == 0 {
		si.cleanup()
		return nil, fmt.Errorf("failed to extract embedding")
	}

	// 确定使用的阈值：如果设置了自定义阈值则使用，否则使用默认阈值
	useThreshold := si.manager.threshold
	if si.threshold > 0 {
		useThreshold = si.threshold
	}

	// 在 Qdrant 向量数据库中搜索（按 UID 过滤，返回 top 1）
	results, err := si.manager.vectorDB.Search(si.uid, embedding, useThreshold, 1)
	if err != nil {
		si.cleanup()
		return nil, fmt.Errorf("failed to search in vector database: %v", err)
	}

	result := &IdentifyResult{
		Identified:  false,
		SpeakerID:   "",
		SpeakerName: "",
		Confidence:  0.0,
		Threshold:   useThreshold,
	}

	if len(results) > 0 {
		bestMatch := results[0]
		result.Identified = true
		result.SpeakerID = bestMatch.SpeakerID
		result.SpeakerName = bestMatch.SpeakerName
		result.Confidence = bestMatch.Confidence
	}

	// 清理资源
	si.cleanup()

	return result, nil
}

// cleanup 清理资源
func (si *StreamingIdentifier) cleanup() {
	if si.stream != nil {
		sherpa.DeleteOnlineStream(si.stream)
		si.stream = nil
	}
}

// Close 关闭流式识别器并释放资源
func (si *StreamingIdentifier) Close() {
	si.mutex.Lock()
	defer si.mutex.Unlock()
	si.cleanup()
}
