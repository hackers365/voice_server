package speaker

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"voice_server/internal/logger"
)

// JSONVectorDB JSON 文件存储实现
// 使用本地 JSON 文件存储声纹向量数据，适合小型部署
type JSONVectorDB struct {
	filePath     string
	data         *SpeakerData
	mutex        sync.RWMutex
	embeddingDim int
}

// JSONVectorDBConfig JSON 存储配置
type JSONVectorDBConfig struct {
	FilePath     string // JSON 文件路径
	EmbeddingDim int    // 向量维度
}

// SpeakerData JSON 数据结构
type SpeakerData struct {
	Version   int64                    `json:"version"`
	UpdatedAt int64                    `json:"updated_at"`
	Speakers  map[string]*SpeakerEntry `json:"speakers"`
}

// SpeakerEntry 说话人条目
type SpeakerEntry struct {
	UID         string       `json:"uid"`
	AgentID     string       `json:"agent_id"`
	SpeakerID   string       `json:"speaker_id"`
	SpeakerName string       `json:"speaker_name"`
	CreatedAt   int64        `json:"created_at"`
	UpdatedAt   int64        `json:"updated_at"`
	Embeddings  []*Embedding `json:"embeddings"`
}

// Embedding 向量条目
type Embedding struct {
	UUID        string    `json:"uuid"`
	SampleIndex int       `json:"sample_index"`
	Vector      []float32 `json:"vector"`
	CreatedAt   int64     `json:"created_at"`
}

// NewJSONVectorDB 创建 JSON 向量数据库
func NewJSONVectorDB(config *JSONVectorDBConfig) (*JSONVectorDB, error) {
	// 确保目录存在
	dir := filepath.Dir(config.FilePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %v", err)
	}

	db := &JSONVectorDB{
		filePath:     config.FilePath,
		embeddingDim: config.EmbeddingDim,
		data: &SpeakerData{
			Version:  1,
			Speakers: make(map[string]*SpeakerEntry),
		},
	}

	// 加载现有数据
	if err := db.load(); err != nil {
		// 如果文件不存在，创建空数据
		if os.IsNotExist(err) {
			logger.Infof("JSON vector DB file not found, creating new one: %s", config.FilePath)
			if err := db.save(); err != nil {
				return nil, fmt.Errorf("failed to create new DB file: %v", err)
			}
		} else {
			return nil, fmt.Errorf("failed to load JSON DB: %v", err)
		}
	}

	return db, nil
}

// Init 初始化数据库（接口实现）
func (db *JSONVectorDB) Init() error {
	return nil // 已在构造函数中初始化
}

// Close 关闭数据库（接口实现）
func (db *JSONVectorDB) Close() error {
	// 保存数据
	return db.save()
}

// load 从文件加载数据
func (db *JSONVectorDB) load() error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	data, err := os.ReadFile(db.filePath)
	if err != nil {
		return err
	}

	if len(data) == 0 {
		return fmt.Errorf("empty file")
	}

	if err := json.Unmarshal(data, db.data); err != nil {
		return err
	}

	logger.Infof("Loaded JSON vector DB: %d speakers from %s", len(db.data.Speakers), db.filePath)
	return nil
}

// save 保存数据到文件（会加锁，供未持锁的调用方使用，如 Close、NewJSONVectorDB）
func (db *JSONVectorDB) save() error {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	return db.saveUnlocked()
}

// saveUnlocked 仅写盘逻辑，不加锁；调用方必须已持有 db.mutex 写锁，避免死锁
func (db *JSONVectorDB) saveUnlocked() error {
	db.data.UpdatedAt = time.Now().Unix()

	data, err := json.MarshalIndent(db.data, "", "  ")
	if err != nil {
		return err
	}

	// 原子写入：先写到临时文件，然后重命名
	tempPath := db.filePath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return err
	}

	if err := os.Rename(tempPath, db.filePath); err != nil {
		return err
	}

	return nil
}

// generateKey 生成唯一键
func generateKey(uid, agentID, speakerID string) string {
	return fmt.Sprintf("%s:%s:%s", uid, agentID, speakerID)
}

// Insert 插入声纹向量（接口实现）；若 uid+agentID+uuid 已存在则更新该条（uuid 相同视为同一声纹）
func (db *JSONVectorDB) Insert(uid, agentID, speakerID, speakerName, uuid string, embedding []float32, sampleIndex int, createdAt, updatedAt int64) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	key := generateKey(uid, agentID, speakerID)

	entry, exists := db.data.Speakers[key]
	if !exists {
		entry = &SpeakerEntry{
			UID:         uid,
			AgentID:     agentID,
			SpeakerID:   speakerID,
			SpeakerName: speakerName,
			CreatedAt:   createdAt,
			UpdatedAt:   updatedAt,
			Embeddings:  make([]*Embedding, 0),
		}
		db.data.Speakers[key] = entry
	}

	// 先按 uuid 查找：uuid 相同则更新该条
	for i, emb := range entry.Embeddings {
		if emb.UUID == uuid {
			entry.Embeddings[i] = &Embedding{
				UUID:        uuid,
				SampleIndex: emb.SampleIndex, // 保留原 sample_index
				Vector:      embedding,
				CreatedAt:   createdAt,
			}
			entry.UpdatedAt = updatedAt
			return db.saveToDiskAsync()
		}
	}

	// 检查是否已存在相同 sample_index 的 embedding（覆盖旧数据）
	for i, emb := range entry.Embeddings {
		if emb.SampleIndex == sampleIndex {
			entry.Embeddings[i] = &Embedding{
				UUID:        uuid,
				SampleIndex: sampleIndex,
				Vector:      embedding,
				CreatedAt:   createdAt,
			}
			entry.UpdatedAt = updatedAt
			return db.saveToDiskAsync()
		}
	}

	// 添加新 embedding
	entry.Embeddings = append(entry.Embeddings, &Embedding{
		UUID:        uuid,
		SampleIndex: sampleIndex,
		Vector:      embedding,
		CreatedAt:   createdAt,
	})
	entry.UpdatedAt = updatedAt

	return db.saveToDiskAsync()
}

// saveToDiskAsync 在已持写锁时落盘，内部只调 saveUnlocked 避免重复加锁死锁
func (db *JSONVectorDB) saveToDiskAsync() error {
	return db.saveUnlocked()
}

// Search 搜索相似向量（接口实现）
func (db *JSONVectorDB) Search(uid string, queryEmbedding []float32, threshold float32, topK int) ([]SearchResult, error) {
	return db.SearchWithOptionalFilters(uid, "", "", "", queryEmbedding, threshold, topK)
}

// SearchWithOptionalFilters 搜索相似向量（接口实现）
func (db *JSONVectorDB) SearchWithOptionalFilters(uid, agentID, speakerID, speakerName string, queryEmbedding []float32, threshold float32, topK int) ([]SearchResult, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	var results []SearchResult

	for _, speaker := range db.data.Speakers {
		// 应用过滤条件
		if uid != "" && speaker.UID != uid {
			continue
		}
		if agentID != "" && speaker.AgentID != agentID {
			continue
		}
		if speakerID != "" && speaker.SpeakerID != speakerID {
			continue
		}
		if speakerName != "" && speaker.SpeakerName != speakerName {
			continue
		}

		// 计算每个 embedding 的相似度
		for _, emb := range speaker.Embeddings {
			similarity := cosineSimilarity(queryEmbedding, emb.Vector)
			if similarity >= threshold {
				results = append(results, SearchResult{
					SpeakerID:   speaker.SpeakerID,
					SpeakerName: speaker.SpeakerName,
					Confidence:  similarity,
					Distance:    1.0 - similarity,
					SampleIndex: emb.SampleIndex,
				})
			}
		}
	}

	// 按置信度排序
	sort.Slice(results, func(i, j int) bool {
		return results[i].Confidence > results[j].Confidence
	})

	// 返回 topK
	if len(results) > topK && topK > 0 {
		results = results[:topK]
	}

	return results, nil
}

// SearchWithFilter 搜索相似向量（接口实现）
func (db *JSONVectorDB) SearchWithFilter(uid, agentID, speakerID string, queryEmbedding []float32, threshold float32, topK int) ([]SearchResult, error) {
	return db.SearchWithOptionalFilters(uid, agentID, speakerID, "", queryEmbedding, threshold, topK)
}

// GetSpeakerSampleCount 获取说话人的样本数量（接口实现）
func (db *JSONVectorDB) GetSpeakerSampleCount(uid, agentID, speakerID string) (int, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	key := generateKey(uid, agentID, speakerID)
	if entry, exists := db.data.Speakers[key]; exists {
		return len(entry.Embeddings), nil
	}
	return 0, nil
}

// GetSpeakerInfo 获取说话人信息（接口实现）
func (db *JSONVectorDB) GetSpeakerInfo(uid, agentID, speakerID string) (*SpeakerInfo, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	key := generateKey(uid, agentID, speakerID)
	entry, exists := db.data.Speakers[key]
	if !exists {
		return nil, fmt.Errorf("speaker %s not found", speakerID)
	}

	return &SpeakerInfo{
		ID:          entry.SpeakerID,
		Name:        entry.SpeakerName,
		SampleCount: len(entry.Embeddings),
		CreatedAt:   time.Unix(entry.CreatedAt, 0),
		UpdatedAt:   time.Unix(entry.UpdatedAt, 0),
	}, nil
}

// GetAllSpeakers 获取所有说话人列表（接口实现）
func (db *JSONVectorDB) GetAllSpeakers(uid, agentID string) ([]*SpeakerInfo, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	var speakers []*SpeakerInfo

	for _, entry := range db.data.Speakers {
		// 应用过滤条件
		if uid != "" && entry.UID != uid {
			continue
		}
		if agentID != "" && entry.AgentID != agentID {
			continue
		}

		speakers = append(speakers, &SpeakerInfo{
			ID:          entry.SpeakerID,
			Name:        entry.SpeakerName,
			AgentID:     entry.AgentID,
			SampleCount: len(entry.Embeddings),
			CreatedAt:   time.Unix(entry.CreatedAt, 0),
			UpdatedAt:   time.Unix(entry.UpdatedAt, 0),
		})
	}

	return speakers, nil
}

// DeleteByFilters 删除说话人（接口实现）
func (db *JSONVectorDB) DeleteByFilters(uid, agentID, speakerID string) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	key := generateKey(uid, agentID, speakerID)
	if _, exists := db.data.Speakers[key]; !exists {
		return nil // 不存在也视为成功
	}

	delete(db.data.Speakers, key)
	return db.saveUnlocked()
}

// DeleteByUUID 通过 UUID 删除说话人（接口实现）
func (db *JSONVectorDB) DeleteByUUID(uid, agentID, uuid string) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	// 查找包含该 UUID 的 speaker
	for key, entry := range db.data.Speakers {
		if entry.UID != uid || (agentID != "" && entry.AgentID != agentID) {
			continue
		}

		// 查找并删除匹配的 embedding
		for i, emb := range entry.Embeddings {
			if emb.UUID == uuid {
				// 删除该 embedding
				entry.Embeddings = append(entry.Embeddings[:i], entry.Embeddings[i+1:]...)

				// 如果没有 embedding 了，删除整个 speaker
				if len(entry.Embeddings) == 0 {
					delete(db.data.Speakers, key)
				}

				return db.saveUnlocked()
			}
		}
	}

	return fmt.Errorf("speaker with uuid %s not found for uid %s", uuid, uid)
}

// cosineSimilarity 计算余弦相似度
func cosineSimilarity(a, b []float32) float32 {
	if len(a) != len(b) {
		return 0
	}

	var dotProduct float32
	var normA float32
	var normB float32

	for i := 0; i < len(a); i++ {
		dotProduct += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	if normA == 0 || normB == 0 {
		return 0
	}

	return dotProduct / (float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB))))
}
