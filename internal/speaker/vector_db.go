package speaker

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"strconv"
	"strings"
	"time"

	"asr_server/internal/logger"

	"github.com/qdrant/go-client/qdrant"
)

// QdrantConfig Qdrant 配置
type QdrantConfig struct {
	Host           string
	Port           int
	CollectionName string
}

// QdrantVectorDB Qdrant 向量数据库客户端
type QdrantVectorDB struct {
	client         *qdrant.Client
	collectionName string
	embeddingDim   int
}

// SearchResult 搜索结果
type SearchResult struct {
	SpeakerID   string
	SpeakerName string
	Confidence  float32
	Distance    float32
	SampleIndex int
}

// NewQdrantVectorDB 创建 Qdrant 向量数据库客户端
func NewQdrantVectorDB(config *QdrantConfig, embeddingDim int) (*QdrantVectorDB, error) {
	// 连接 Qdrant
	client, err := qdrant.NewClient(&qdrant.Config{
		Host: config.Host,
		Port: config.Port,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Qdrant: %v", err)
	}

	db := &QdrantVectorDB{
		client:         client,
		collectionName: config.CollectionName,
		embeddingDim:   embeddingDim,
	}

	// 确保 Collection 存在
	ctx := context.Background()
	_, err = client.GetCollectionInfo(ctx, config.CollectionName)
	if err != nil {
		// Collection 不存在，创建它
		logger.Infof("Collection '%s' does not exist, creating it...", config.CollectionName)
		err = client.CreateCollection(ctx, &qdrant.CreateCollection{
			CollectionName: config.CollectionName,
			VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
				Size:     uint64(embeddingDim),
				Distance: qdrant.Distance_Cosine, // 使用余弦距离（Qdrant 自动归一化）
			}),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create collection: %v", err)
		}
		logger.Infof("✅ Collection '%s' created successfully", config.CollectionName)
	}

	return db, nil
}

// normalizeVector 对向量进行 L2 归一化
// 公式: v_normalized = v / ||v||
// 当向量归一化后，点积 = 余弦相似度
func normalizeVector(v []float32) []float32 {
	// 计算 L2 范数
	var norm float32
	for _, val := range v {
		norm += val * val
	}
	norm = float32(math.Sqrt(float64(norm)))

	// 归一化
	if norm == 0 {
		return v // 零向量直接返回
	}

	normalized := make([]float32, len(v))
	for i := range v {
		normalized[i] = v[i] / norm
	}
	return normalized
}

// generatePointID 生成唯一的 Point ID
func generatePointID(uid, speakerID string, sampleIndex int) uint64 {
	hash := fnv.New64a()
	hash.Write([]byte(fmt.Sprintf("%s:%s:%d", uid, speakerID, sampleIndex)))
	return hash.Sum64()
}

// Insert 插入 embedding 到向量数据库
func (db *QdrantVectorDB) Insert(uid, speakerID, speakerName string, embedding []float32, sampleIndex int, createdAt, updatedAt int64) error {
	ctx := context.Background()

	// 注意：使用 Distance_Cosine 时，Qdrant 会自动对向量进行归一化
	// 因此不需要在程序中手动归一化（即使传入的向量已经归一化，Qdrant 再次归一化也没问题）

	// 生成唯一的 Point ID
	pointID := generatePointID(uid, speakerID, sampleIndex)

	// 构建 Point
	point := &qdrant.PointStruct{
		Id:      qdrant.NewIDNum(pointID),
		Vectors: qdrant.NewVectors(embedding...),
		Payload: qdrant.NewValueMap(map[string]any{
			"uid":          uid,
			"speaker_id":   speakerID,
			"speaker_name": speakerName,
			"sample_index": sampleIndex,
			"created_at":   createdAt,
			"updated_at":   updatedAt,
		}),
	}

	_, err := db.client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: db.collectionName,
		Points:         []*qdrant.PointStruct{point},
	})
	if err != nil {
		return fmt.Errorf("failed to insert point: %v", err)
	}

	return nil
}

// Search 搜索相似向量（按 UID 过滤）
func (db *QdrantVectorDB) Search(uid string, queryEmbedding []float32, threshold float32, topK int) ([]SearchResult, error) {
	ctx := context.Background()

	// 构建过滤条件（按 UID 过滤）
	filter := &qdrant.Filter{
		Must: []*qdrant.Condition{
			qdrant.NewMatch("uid", uid),
		},
	}

	limit := uint64(topK)
	if limit == 0 {
		limit = 1
	}

	// 对 queryEmbedding 进行 L2 归一化（DOT 距离要求向量归一化）
	normalizedQueryEmbedding := normalizeVector(queryEmbedding)

	// 使用 Query API 搜索
	searchPoints, err := db.client.Query(ctx, &qdrant.QueryPoints{
		CollectionName: db.collectionName,
		Query:          qdrant.NewQuery(normalizedQueryEmbedding...),
		Filter:         filter,
		Limit:          &limit,
		WithPayload:    qdrant.NewWithPayload(true),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to search: %v", err)
	}

	// 转换结果
	results := make([]SearchResult, 0)
	for _, point := range searchPoints {
		if point.Payload == nil {
			continue
		}

		payload := point.GetPayload()
		var speakerID string
		var speakerName string
		var sampleIndex int

		if val, ok := payload["speaker_id"]; ok {
			speakerID = val.GetStringValue()
		}
		if val, ok := payload["speaker_name"]; ok {
			speakerName = val.GetStringValue()
		}
		if val, ok := payload["sample_index"]; ok {
			sampleIndex = int(val.GetIntegerValue())
		}

		// Query API 返回的 score 是余弦相似度（范围 [-1, 1]）
		// 使用 Distance_Cosine 时，Qdrant 会自动归一化向量并计算余弦相似度
		score := float32(point.Score)

		// 重要：Manager 的 cosineSimilarity() 直接返回余弦相似度（范围 [-1, 1]）
		// 为了与 Manager 保持一致，Qdrant 也应该直接使用 score，不做转换
		var confidence float32
		if score < -1 {
			confidence = -1.0
		} else if score > 1 {
			confidence = 1.0
		} else {
			// 直接使用 score（范围 [-1, 1]），与 Manager 的余弦相似度保持一致
			confidence = score
		}

		// 应用阈值过滤
		if confidence < threshold {
			continue
		}

		distance := 1.0 - confidence

		results = append(results, SearchResult{
			SpeakerID:   speakerID,
			SpeakerName: speakerName,
			Confidence:  confidence,
			Distance:    distance,
			SampleIndex: sampleIndex,
		})
	}

	return results, nil
}

// SearchWithFilter 搜索相似向量（按 UID 和 speaker_id 过滤）
func (db *QdrantVectorDB) SearchWithFilter(uid, speakerID string, queryEmbedding []float32, threshold float32, topK int) ([]SearchResult, error) {
	ctx := context.Background()

	// 构建过滤条件（按 UID 和 speaker_id 过滤）
	filter := &qdrant.Filter{
		Must: []*qdrant.Condition{
			qdrant.NewMatch("uid", uid),
			qdrant.NewMatch("speaker_id", speakerID),
		},
	}

	limit := uint64(topK)
	if limit == 0 {
		limit = 1
	}

	// 注意：使用 Distance_Cosine 时，Qdrant 会自动对查询向量进行归一化
	// 因此不需要在程序中手动归一化（即使传入的向量已经归一化，Qdrant 再次归一化也没问题）

	// 使用 Query API 搜索
	searchPoints, err := db.client.Query(ctx, &qdrant.QueryPoints{
		CollectionName: db.collectionName,
		Query:          qdrant.NewQuery(queryEmbedding...),
		Filter:         filter,
		Limit:          &limit,
		WithPayload:    qdrant.NewWithPayload(true),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to search: %v", err)
	}

	// 转换结果（与 Search 方法相同）
	results := make([]SearchResult, 0)
	for _, point := range searchPoints {
		if point.Payload == nil {
			continue
		}

		payload := point.GetPayload()
		var foundSpeakerID string
		var speakerName string
		var sampleIndex int

		if val, ok := payload["speaker_id"]; ok {
			foundSpeakerID = val.GetStringValue()
		}
		if val, ok := payload["speaker_name"]; ok {
			speakerName = val.GetStringValue()
		}
		if val, ok := payload["sample_index"]; ok {
			sampleIndex = int(val.GetIntegerValue())
		}

		// Query API 返回的 score 是余弦相似度（范围 [-1, 1]）
		// 使用 Distance_Cosine 时，Qdrant 会自动归一化向量并计算余弦相似度
		score := float32(point.Score)
		// 重要：Manager 的 cosineSimilarity() 直接返回余弦相似度（范围 [-1, 1]）
		// 为了与 Manager 保持一致，Qdrant 也应该直接使用 score，不做转换
		var confidence float32
		if score < -1 {
			confidence = -1.0
		} else if score > 1 {
			confidence = 1.0
		} else {
			// 直接使用 score（范围 [-1, 1]），与 Manager 的余弦相似度保持一致
			confidence = score
		}

		if confidence < threshold {
			continue
		}

		distance := 1.0 - confidence

		results = append(results, SearchResult{
			SpeakerID:   foundSpeakerID,
			SpeakerName: speakerName,
			Confidence:  confidence,
			Distance:    distance,
			SampleIndex: sampleIndex,
		})
	}

	return results, nil
}

// GetSpeakerSampleCount 获取说话人的样本数量
func (db *QdrantVectorDB) GetSpeakerSampleCount(uid, speakerID string) (int, error) {
	ctx := context.Background()

	// 使用 Scroll API 获取所有匹配的 points
	filter := &qdrant.Filter{
		Must: []*qdrant.Condition{
			qdrant.NewMatch("uid", uid),
			qdrant.NewMatch("speaker_id", speakerID),
		},
	}

	limit := uint32(10000) // 足够大的值
	scrollResult, err := db.client.Scroll(ctx, &qdrant.ScrollPoints{
		CollectionName: db.collectionName,
		Filter:         filter,
		Limit:          &limit,
		WithPayload:    qdrant.NewWithPayload(true),
	})
	if err != nil {
		return 0, fmt.Errorf("failed to scroll points: %v", err)
	}

	return len(scrollResult), nil
}

// GetSpeakerInfo 获取说话人信息
func (db *QdrantVectorDB) GetSpeakerInfo(uid, speakerID string) (*SpeakerInfo, error) {
	ctx := context.Background()

	// 使用 Scroll API 获取所有匹配的 points
	filter := &qdrant.Filter{
		Must: []*qdrant.Condition{
			qdrant.NewMatch("uid", uid),
			qdrant.NewMatch("speaker_id", speakerID),
		},
	}

	limit := uint32(10000)
	scrollResult, err := db.client.Scroll(ctx, &qdrant.ScrollPoints{
		CollectionName: db.collectionName,
		Filter:         filter,
		Limit:          &limit,
		WithPayload:    qdrant.NewWithPayload(true),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to scroll points: %v", err)
	}

	if len(scrollResult) == 0 {
		return nil, fmt.Errorf("speaker %s not found", speakerID)
	}

	// 从第一个 point 提取信息
	firstPoint := scrollResult[0]
	payload := firstPoint.GetPayload()

	var speakerName string
	var minCreatedAt, maxUpdatedAt int64 = -1, -1

	if val, ok := payload["speaker_name"]; ok {
		speakerName = val.GetStringValue()
	}

	// 遍历所有 points，找到最早的 created_at 和最新的 updated_at
	for _, point := range scrollResult {
		payload := point.GetPayload()
		if val, ok := payload["created_at"]; ok {
			ts := val.GetIntegerValue()
			if minCreatedAt == -1 || ts < minCreatedAt {
				minCreatedAt = ts
			}
		}
		if val, ok := payload["updated_at"]; ok {
			ts := val.GetIntegerValue()
			if ts > maxUpdatedAt {
				maxUpdatedAt = ts
			}
		}
	}

	if minCreatedAt == -1 {
		minCreatedAt = time.Now().Unix()
	}
	if maxUpdatedAt == -1 {
		maxUpdatedAt = time.Now().Unix()
	}

	return &SpeakerInfo{
		ID:          speakerID,
		Name:        speakerName,
		SampleCount: len(scrollResult),
		CreatedAt:   time.Unix(minCreatedAt, 0),
		UpdatedAt:   time.Unix(maxUpdatedAt, 0),
	}, nil
}

// GetAllSpeakers 获取指定 UID 的所有说话人列表
func (db *QdrantVectorDB) GetAllSpeakers(uid string) ([]*SpeakerInfo, error) {
	ctx := context.Background()

	// 使用 Scroll API 获取所有匹配的 points
	filter := &qdrant.Filter{
		Must: []*qdrant.Condition{
			qdrant.NewMatch("uid", uid),
		},
	}

	limit := uint32(10000)
	scrollResult, err := db.client.Scroll(ctx, &qdrant.ScrollPoints{
		CollectionName: db.collectionName,
		Filter:         filter,
		Limit:          &limit,
		WithPayload:    qdrant.NewWithPayload(true),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to scroll points: %v", err)
	}

	// 按 speaker_id 聚合
	speakerMap := make(map[string]*SpeakerInfo)
	for _, point := range scrollResult {
		payload := point.GetPayload()
		var speakerID string
		var speakerName string
		var createdAt, updatedAt int64

		if val, ok := payload["speaker_id"]; ok {
			speakerID = val.GetStringValue()
		}
		if val, ok := payload["speaker_name"]; ok {
			speakerName = val.GetStringValue()
		}
		if val, ok := payload["created_at"]; ok {
			createdAt = val.GetIntegerValue()
		}
		if val, ok := payload["updated_at"]; ok {
			updatedAt = val.GetIntegerValue()
		}

		if speakerID == "" {
			continue
		}

		info, exists := speakerMap[speakerID]
		if !exists {
			info = &SpeakerInfo{
				ID:          speakerID,
				Name:        speakerName,
				SampleCount: 0,
				CreatedAt:   time.Unix(createdAt, 0),
				UpdatedAt:   time.Unix(updatedAt, 0),
			}
			speakerMap[speakerID] = info
		}

		info.SampleCount++

		// 更新最早创建时间和最晚更新时间
		if createdAt > 0 {
			pointCreatedAt := time.Unix(createdAt, 0)
			if info.CreatedAt.IsZero() || pointCreatedAt.Before(info.CreatedAt) {
				info.CreatedAt = pointCreatedAt
			}
		}
		if updatedAt > 0 {
			pointUpdatedAt := time.Unix(updatedAt, 0)
			if info.UpdatedAt.IsZero() || pointUpdatedAt.After(info.UpdatedAt) {
				info.UpdatedAt = pointUpdatedAt
			}
		}
	}

	// 转换为切片
	speakers := make([]*SpeakerInfo, 0, len(speakerMap))
	for _, info := range speakerMap {
		speakers = append(speakers, info)
	}

	return speakers, nil
}

// DeleteSpeaker 删除说话人的所有向量
func (db *QdrantVectorDB) DeleteSpeaker(uid, speakerID string) error {
	ctx := context.Background()

	// 使用 Scroll API 获取所有匹配的 points
	filter := &qdrant.Filter{
		Must: []*qdrant.Condition{
			qdrant.NewMatch("uid", uid),
			qdrant.NewMatch("speaker_id", speakerID),
		},
	}

	limit := uint32(10000)
	scrollResult, err := db.client.Scroll(ctx, &qdrant.ScrollPoints{
		CollectionName: db.collectionName,
		Filter:         filter,
		Limit:          &limit,
		WithPayload:    qdrant.NewWithPayload(false), // 不需要 payload
	})
	if err != nil {
		return fmt.Errorf("failed to scroll points: %v", err)
	}

	if len(scrollResult) == 0 {
		return nil // 没有数据需要删除
	}

	// 提取所有 Point IDs
	ids := make([]*qdrant.PointId, 0, len(scrollResult))
	for _, point := range scrollResult {
		ids = append(ids, point.Id)
	}

	// 删除这些 points
	_, err = db.client.Delete(ctx, &qdrant.DeletePoints{
		CollectionName: db.collectionName,
		Points: &qdrant.PointsSelector{
			PointsSelectorOneOf: &qdrant.PointsSelector_Points{
				Points: &qdrant.PointsIdsList{
					Ids: ids,
				},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to delete points: %v", err)
	}

	return nil
}

// Close 关闭向量数据库连接
func (db *QdrantVectorDB) Close() error {
	// Qdrant Go Client 可能不需要显式关闭，但保留接口以便未来扩展
	return nil
}

// parseQdrantAddress 解析 Qdrant 地址（格式：host:port 或 host）
func parseQdrantAddress(addr string) (string, int) {
	host := "localhost"
	port := 6334

	if addr == "" {
		return host, port
	}

	parts := strings.Split(addr, ":")
	if len(parts) == 2 {
		host = parts[0]
		if p, err := strconv.Atoi(parts[1]); err == nil {
			port = p
		}
	} else if len(parts) == 1 {
		host = parts[0]
	}

	return host, port
}
