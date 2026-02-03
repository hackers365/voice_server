package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/go-audio/audio"
	"github.com/go-audio/wav"
	"github.com/gorilla/websocket"
)

const (
	baseURL        = "http://192.168.209.138:9000"
	speakerAPI     = baseURL + "/api/v1/speaker"
	speakerID      = "test_speaker_001"
	speakerName    = "测试说话人"
	defaultUID     = "test_user_001"
	defaultAgentID = "test_agent_001"
	defaultUUID    = "test_uuid_001"
)

// IdentifyResult 识别结果结构
type IdentifyResult struct {
	Identified  bool    `json:"identified"`
	SpeakerID   string  `json:"speaker_id"`
	SpeakerName string  `json:"speaker_name"`
	Confidence  float32 `json:"confidence"`
	Threshold   float32 `json:"threshold"`
}

// RegisterResponse 注册响应结构
type RegisterResponse struct {
	Message     string `json:"message"`
	UID         string `json:"uid"`
	SpeakerID   string `json:"speaker_id"`
	SpeakerName string `json:"speaker_name"`
}

// ErrorResponse 错误响应结构
type ErrorResponse struct {
	Error string `json:"error"`
}

// SpeakerInfo 说话人信息结构
type SpeakerInfo struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	SampleCount int    `json:"sample_count"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
}

// ListResponse 列表响应结构
type ListResponse struct {
	UID      string        `json:"uid"`
	Speakers []SpeakerInfo `json:"speakers"`
	Total    int           `json:"total"`
}

// DeleteResponse 删除响应结构
type DeleteResponse struct {
	Message   string `json:"message"`
	UID       string `json:"uid"`
	SpeakerID string `json:"speaker_id"`
}

func main() {
	// 解析命令行参数
	var registerFile string
	var identifyFile string
	var listSpeakers bool
	var deleteSpeakerID string
	var customSpeakerID string
	var customSpeakerName string
	var customUID string
	var customAgentID string
	var customUUID string
	var maxFrames int
	var threshold float64

	flag.StringVar(&registerFile, "register", "", "注册声纹的音频文件路径（WAV格式）")
	flag.StringVar(&identifyFile, "identify", "", "识别声纹的音频文件路径（WAV格式）")
	flag.BoolVar(&listSpeakers, "list", false, "列出所有已注册的声纹")
	flag.StringVar(&deleteSpeakerID, "delete", "", "删除指定说话人ID的声纹")
	flag.StringVar(&customSpeakerID, "speaker-id", speakerID, "说话人ID（默认：test_speaker_001）")
	flag.StringVar(&customSpeakerName, "speaker-name", speakerName, "说话人名称（默认：测试说话人）")
	flag.StringVar(&customUID, "uid", defaultUID, "用户ID（默认：test_user_001）")
	flag.StringVar(&customAgentID, "agent-id", defaultAgentID, "代理ID（默认：test_agent_001，服务端必填）")
	flag.StringVar(&customUUID, "uuid", defaultUUID, "注册会话UUID（默认：test_uuid_001，服务端必填）")
	flag.IntVar(&maxFrames, "frames", 0, "流式识别时发送的帧数（0表示发送所有帧，可选）")
	flag.Float64Var(&threshold, "threshold", 0, "识别阈值（>0时使用，否则使用服务端默认值，可选）")
	flag.Parse()

	fmt.Println("========================================")
	fmt.Println("声纹识别测试程序")
	fmt.Println("========================================")

	// 如果所有参数都没有指定，显示使用说明
	if registerFile == "" && identifyFile == "" && !listSpeakers && deleteSpeakerID == "" {
		fmt.Println("\n使用方法:")
		fmt.Println("  go run test_speaker.go -register <注册文件>")
		fmt.Println("  go run test_speaker.go -identify <识别文件>")
		fmt.Println("  go run test_speaker.go -list")
		fmt.Println("  go run test_speaker.go -delete <说话人ID>")
		fmt.Println("  go run test_speaker.go -register <注册文件> -identify <识别文件>")
		fmt.Println("\n参数说明:")
		fmt.Println("  -register <文件路径>    注册声纹的音频文件（WAV格式，可选）")
		fmt.Println("  -identify <文件路径>    识别声纹的音频文件（WAV格式，可选）")
		fmt.Println("  -list                   列出所有已注册的声纹（可选）")
		fmt.Println("  -delete <说话人ID>       删除指定说话人ID的声纹（可选）")
		fmt.Println("  -speaker-id <ID>        说话人ID（可选，默认：test_speaker_001）")
		fmt.Println("  -speaker-name <名称>    说话人名称（可选，默认：测试说话人）")
		fmt.Println("  -uid <用户ID>           用户ID（可选，默认：test_user_001）")
		fmt.Println("  -agent-id <代理ID>      代理ID（可选，默认：test_agent_001）")
		fmt.Println("  -uuid <UUID>            注册会话UUID（可选，默认：test_uuid_001，注册时服务端必填）")
		fmt.Println("  -frames <帧数>         流式识别时发送的帧数（0表示发送所有帧，可选）")
		fmt.Println("  -threshold <阈值>      识别阈值（>0时使用，否则使用服务端默认值，可选）")
		fmt.Println("\n示例:")
		fmt.Println("  # 仅注册声纹")
		fmt.Println("  go run test_speaker.go -register register.wav")
		fmt.Println("  # 仅识别声纹")
		fmt.Println("  go run test_speaker.go -identify identify.wav")
		fmt.Println("  # 列出所有声纹")
		fmt.Println("  go run test_speaker.go -list")
		fmt.Println("  # 删除声纹")
		fmt.Println("  go run test_speaker.go -delete test_speaker_001")
		fmt.Println("  # 注册并识别")
		fmt.Println("  go run test_speaker.go -register register.wav -identify identify.wav")
		fmt.Println("  go run test_speaker.go -register test.wav -identify test.wav -speaker-id user001 -uid user001")
		fmt.Println("  # 流式识别时只发送前10帧")
		fmt.Println("  go run test_speaker.go -identify test.wav -frames 10")
		fmt.Println("  # 使用自定义阈值识别")
		fmt.Println("  go run test_speaker.go -identify test.wav -threshold 0.7")
		os.Exit(1)
	}

	// 处理列表查询
	if listSpeakers {
		fmt.Printf("\n步骤 1: 获取声纹列表 (用户ID: %s", customUID)
		if customAgentID != "" {
			fmt.Printf(", 代理ID: %s", customAgentID)
		}
		fmt.Println(")...")
		if err := listSpeakersFunc(customUID, customAgentID); err != nil {
			fmt.Printf("❌ 获取列表失败: %v\n", err)
			os.Exit(1)
		}
		// 如果只执行列表查询，直接退出
		if registerFile == "" && identifyFile == "" && deleteSpeakerID == "" {
			return
		}
	}

	// 处理删除
	if deleteSpeakerID != "" {
		stepNum := 1
		if listSpeakers {
			stepNum = 2
		}
		fmt.Printf("\n步骤 %d: 删除声纹 (说话人ID: %s, 用户ID: %s", stepNum, deleteSpeakerID, customUID)
		if customAgentID != "" {
			fmt.Printf(", 代理ID: %s", customAgentID)
		}
		fmt.Println(")...")
		if err := deleteSpeaker(deleteSpeakerID, customUID, customAgentID); err != nil {
			fmt.Printf("❌ 删除失败: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("✅ 删除成功")
		// 如果只执行删除操作，直接退出
		if registerFile == "" && identifyFile == "" {
			return
		}
	}

	// 处理注册
	if registerFile != "" {
		// 检查注册文件是否存在
		registerPath, err := filepath.Abs(registerFile)
		if err != nil {
			fmt.Printf("❌ 错误: 无法解析注册文件路径: %v\n", err)
			os.Exit(1)
		}
		if _, err := os.Stat(registerPath); os.IsNotExist(err) {
			fmt.Printf("❌ 错误: 找不到注册文件 %s\n", registerPath)
			os.Exit(1)
		}
		fmt.Printf("✅ 找到注册音频文件: %s\n", registerPath)

		// 计算步骤编号
		stepNum := 1
		if listSpeakers {
			stepNum++
		}
		if deleteSpeakerID != "" {
			stepNum++
		}

		// 注册声纹
		fmt.Printf("\n步骤 %d: 注册声纹 (使用文件: %s)...\n", stepNum, filepath.Base(registerPath))
		if err := registerSpeaker(registerPath, customSpeakerID, customSpeakerName, customUID, customAgentID, customUUID); err != nil {
			fmt.Printf("❌ 注册失败: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("✅ 注册成功")

		// 等待一下，确保数据已保存
		time.Sleep(500 * time.Millisecond)

		// 如果只执行注册操作，直接退出
		if identifyFile == "" {
			return
		}
	}

	// 处理识别
	if identifyFile != "" {
		// 检查识别文件是否存在
		identifyPath, err := filepath.Abs(identifyFile)
		if err != nil {
			fmt.Printf("❌ 错误: 无法解析识别文件路径: %v\n", err)
			os.Exit(1)
		}
		if _, err := os.Stat(identifyPath); os.IsNotExist(err) {
			fmt.Printf("❌ 错误: 找不到识别文件 %s\n", identifyPath)
			os.Exit(1)
		}
		fmt.Printf("✅ 找到识别音频文件: %s\n", identifyPath)

		// 计算步骤编号
		stepNum := 1
		if listSpeakers {
			stepNum++
		}
		if deleteSpeakerID != "" {
			stepNum++
		}
		if registerFile != "" {
			stepNum++
		}

		// HTTP 识别声纹
		fmt.Printf("\n步骤 %d: HTTP 识别声纹 (使用文件: %s", stepNum, filepath.Base(identifyPath))
		if threshold > 0 {
			fmt.Printf(", 阈值: %.4f", threshold)
		}
		fmt.Println(")...")
		result, err := identifySpeaker(identifyPath, customUID, customAgentID, threshold)
		if err != nil {
			fmt.Printf("❌ 识别失败: %v\n", err)
			os.Exit(1)
		}

		// 显示 HTTP 识别结果
		fmt.Println("\nHTTP 识别结果:")
		fmt.Println("========================================")
		fmt.Printf("识别状态: %v\n", result.Identified)
		if result.Identified {
			fmt.Printf("说话人ID: %s\n", result.SpeakerID)
			fmt.Printf("说话人名称: %s\n", result.SpeakerName)
			fmt.Printf("相似度: %.4f\n", result.Confidence)
			fmt.Printf("阈值: %.4f\n", result.Threshold)
			if result.Confidence >= result.Threshold {
				fmt.Println("✅ 识别成功，相似度超过阈值")
			} else {
				fmt.Println("⚠️  识别成功，但相似度低于阈值")
			}
		} else {
			fmt.Println("❌ 未识别到匹配的说话人")
		}
		fmt.Println("========================================")

		// WebSocket 流式识别
		stepNum++
		fmt.Printf("\n步骤 %d: WebSocket 流式识别 (使用文件: %s", stepNum, filepath.Base(identifyPath))
		if maxFrames > 0 {
			fmt.Printf(", 发送前 %d 帧", maxFrames)
		}
		if threshold > 0 {
			fmt.Printf(", 阈值: %.4f", threshold)
		}
		fmt.Println(")...")
		wsResult, err := identifySpeakerWebSocket(identifyPath, customUID, customAgentID, maxFrames, threshold)
		if err != nil {
			fmt.Printf("❌ WebSocket 识别失败: %v\n", err)
			os.Exit(1)
		}

		// 显示 WebSocket 识别结果
		fmt.Println("\nWebSocket 流式识别结果:")
		fmt.Println("========================================")
		fmt.Printf("识别状态: %v\n", wsResult.Identified)
		if wsResult.Identified {
			fmt.Printf("说话人ID: %s\n", wsResult.SpeakerID)
			fmt.Printf("说话人名称: %s\n", wsResult.SpeakerName)
			fmt.Printf("相似度: %.4f\n", wsResult.Confidence)
			fmt.Printf("阈值: %.4f\n", wsResult.Threshold)
			if wsResult.Confidence >= wsResult.Threshold {
				fmt.Println("✅ 识别成功，相似度超过阈值")
			} else {
				fmt.Println("⚠️  识别成功，但相似度低于阈值")
			}
		} else {
			fmt.Println("❌ 未识别到匹配的说话人")
		}
		fmt.Println("========================================")
	}
}

// registerSpeaker 注册声纹
func registerSpeaker(wavPath string, sid string, sname string, uid string, agentID string, uuid string) error {
	// 打开文件
	file, err := os.Open(wavPath)
	if err != nil {
		return fmt.Errorf("打开文件失败: %v", err)
	}
	defer file.Close()

	// 创建 multipart writer
	var requestBody bytes.Buffer
	writer := multipart.NewWriter(&requestBody)

	// 添加表单字段
	if err := writer.WriteField("uid", uid); err != nil {
		return fmt.Errorf("写入 uid 失败: %v", err)
	}

	if agentID != "" {
		if err := writer.WriteField("agent_id", agentID); err != nil {
			return fmt.Errorf("写入 agent_id 失败: %v", err)
		}
	}

	if uuid != "" {
		if err := writer.WriteField("uuid", uuid); err != nil {
			return fmt.Errorf("写入 uuid 失败: %v", err)
		}
	}

	if err := writer.WriteField("speaker_id", sid); err != nil {
		return fmt.Errorf("写入 speaker_id 失败: %v", err)
	}

	if err := writer.WriteField("speaker_name", sname); err != nil {
		return fmt.Errorf("写入 speaker_name 失败: %v", err)
	}

	// 添加文件
	part, err := writer.CreateFormFile("audio", filepath.Base(wavPath))
	if err != nil {
		return fmt.Errorf("创建文件字段失败: %v", err)
	}

	if _, err := io.Copy(part, file); err != nil {
		return fmt.Errorf("复制文件内容失败: %v", err)
	}

	// 关闭 writer
	if err := writer.Close(); err != nil {
		return fmt.Errorf("关闭 writer 失败: %v", err)
	}

	// 创建 HTTP 请求
	req, err := http.NewRequest("POST", speakerAPI+"/register", &requestBody)
	if err != nil {
		return fmt.Errorf("创建请求失败: %v", err)
	}

	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("X-User-ID", uid) // 同时通过请求头传递 uid
	if agentID != "" {
		req.Header.Set("X-Agent-ID", agentID) // 同时通过请求头传递 agent_id
	}

	// 发送请求
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("发送请求失败: %v", err)
	}
	defer resp.Body.Close()

	// 读取响应
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("读取响应失败: %v", err)
	}

	// 检查状态码
	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if err := json.Unmarshal(body, &errResp); err == nil {
			return fmt.Errorf("HTTP %d: %s", resp.StatusCode, errResp.Error)
		}
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	// 解析响应
	var registerResp RegisterResponse
	if err := json.Unmarshal(body, &registerResp); err != nil {
		return fmt.Errorf("解析响应失败: %v", err)
	}

	fmt.Printf("   用户ID: %s\n", uid)
	fmt.Printf("   注册ID: %s\n", registerResp.SpeakerID)
	fmt.Printf("   注册名称: %s\n", registerResp.SpeakerName)

	return nil
}

// identifySpeaker 识别声纹
// threshold: 识别阈值，如果 <= 0 则使用服务端默认值
func identifySpeaker(wavPath string, uid string, agentID string, threshold float64) (*IdentifyResult, error) {
	// 打开文件
	file, err := os.Open(wavPath)
	if err != nil {
		return nil, fmt.Errorf("打开文件失败: %v", err)
	}
	defer file.Close()

	// 创建 multipart writer
	var requestBody bytes.Buffer
	writer := multipart.NewWriter(&requestBody)

	// 添加表单字段 uid
	if err := writer.WriteField("uid", uid); err != nil {
		return nil, fmt.Errorf("写入 uid 失败: %v", err)
	}

	// 添加表单字段 agent_id（如果提供）
	if agentID != "" {
		if err := writer.WriteField("agent_id", agentID); err != nil {
			return nil, fmt.Errorf("写入 agent_id 失败: %v", err)
		}
	}

	// 添加表单字段 threshold（如果提供且 > 0）
	if threshold > 0 {
		if err := writer.WriteField("threshold", fmt.Sprintf("%.6f", threshold)); err != nil {
			return nil, fmt.Errorf("写入 threshold 失败: %v", err)
		}
	}

	// 添加文件
	part, err := writer.CreateFormFile("audio", filepath.Base(wavPath))
	if err != nil {
		return nil, fmt.Errorf("创建文件字段失败: %v", err)
	}

	if _, err := io.Copy(part, file); err != nil {
		return nil, fmt.Errorf("复制文件内容失败: %v", err)
	}

	// 关闭 writer
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("关闭 writer 失败: %v", err)
	}

	// 创建 HTTP 请求
	req, err := http.NewRequest("POST", speakerAPI+"/identify", &requestBody)
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %v", err)
	}

	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("X-User-ID", uid) // 同时通过请求头传递 uid
	if agentID != "" {
		req.Header.Set("X-Agent-ID", agentID) // 同时通过请求头传递 agent_id
	}

	// 发送请求
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("发送请求失败: %v", err)
	}
	defer resp.Body.Close()

	// 读取响应
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %v", err)
	}

	// 检查状态码
	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if err := json.Unmarshal(body, &errResp); err == nil {
			return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, errResp.Error)
		}
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	// 解析响应
	var result IdentifyResult
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("解析响应失败: %v", err)
	}

	return &result, nil
}

// readWavToFloat32 读取WAV文件并转换为float32数组
func readWavToFloat32(wavPath string) ([]float32, int, error) {
	// 打开文件
	file, err := os.Open(wavPath)
	if err != nil {
		return nil, 0, fmt.Errorf("打开文件失败: %v", err)
	}
	defer file.Close()

	// 创建WAV解码器
	decoder := wav.NewDecoder(file)
	if !decoder.IsValidFile() {
		return nil, 0, fmt.Errorf("无效的WAV文件")
	}

	// 读取WAV文件信息
	decoder.ReadInfo()
	format := decoder.Format()
	sampleRate := int(format.SampleRate)
	numChannels := int(format.NumChannels)

	// 读取所有PCM数据
	var allSamples []float32

	// 使用缓冲区读取
	frameSize := sampleRate * 20 / 1000 // 20ms帧
	audioBuf := &audio.IntBuffer{
		Format:         format,
		SourceBitDepth: 16,
		Data:           make([]int, frameSize*numChannels),
	}

	for {
		n, err := decoder.PCMBuffer(audioBuf)
		if err == io.EOF || n == 0 {
			break
		}
		if err != nil {
			return nil, 0, fmt.Errorf("读取WAV数据失败: %v", err)
		}

		// 转换为float32格式（范围[-1.0, 1.0]）
		for i := 0; i < n; i++ {
			sample := float32(audioBuf.Data[i]) / 32767.0
			allSamples = append(allSamples, sample)
		}
	}

	// 如果是立体声，转换为单声道（取平均值）
	if numChannels == 2 {
		monoSamples := make([]float32, len(allSamples)/2)
		for i := 0; i < len(monoSamples); i++ {
			monoSamples[i] = (allSamples[i*2] + allSamples[i*2+1]) / 2.0
		}
		allSamples = monoSamples
	}

	return allSamples, sampleRate, nil
}

// float32ToBytes 将float32数组转换为二进制字节（小端序）
func float32ToBytes(samples []float32) []byte {
	buf := make([]byte, len(samples)*4)
	for i, sample := range samples {
		// 将float32转换为字节（使用math.Float32bits）
		bits := math.Float32bits(sample)
		binary.LittleEndian.PutUint32(buf[i*4:], bits)
	}
	return buf
}

// identifySpeakerWebSocket 通过WebSocket流式识别声纹
// maxFrames: 要发送的最大帧数，0表示发送所有帧
// threshold: 识别阈值，如果 <= 0 则使用服务端默认值
func identifySpeakerWebSocket(wavPath string, uid string, agentID string, maxFrames int, threshold float64) (*IdentifyResult, error) {
	// 读取WAV文件
	audioData, sampleRate, err := readWavToFloat32(wavPath)
	if err != nil {
		return nil, fmt.Errorf("读取音频文件失败: %v", err)
	}

	fmt.Printf("   音频采样率: %d Hz\n", sampleRate)
	fmt.Printf("   音频样本数: %d\n", len(audioData))
	fmt.Printf("   音频时长: %.2f 秒\n", float64(len(audioData))/float64(sampleRate))
	fmt.Printf("   注意: 客户端不进行重采样，服务端将自动重采样到模型期望的采样率\n")

	// 连接WebSocket，传入原始采样率和uid
	// 服务端会根据传入的采样率自动重采样到模型期望的采样率（通常是16000Hz）
	wsURL := fmt.Sprintf("ws://192.168.208.214:8080/api/v1/speaker/identify_ws?sample_rate=%d&uid=%s", sampleRate, uid)
	if agentID != "" {
		wsURL += fmt.Sprintf("&agent_id=%s", url.QueryEscape(agentID))
	}
	if threshold > 0 {
		wsURL += fmt.Sprintf("&threshold=%.6f", threshold)
	}

	// 创建请求头，同时通过请求头传递 uid 和 agent_id
	header := http.Header{}
	header.Set("X-User-ID", uid)
	if agentID != "" {
		header.Set("X-Agent-ID", agentID)
	}

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		return nil, fmt.Errorf("WebSocket连接失败: %v", err)
	}
	defer conn.Close()

	// 设置读取超时
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))

	// 接收连接确认消息
	var connectionMsg map[string]interface{}
	if err := conn.ReadJSON(&connectionMsg); err != nil {
		return nil, fmt.Errorf("读取连接确认消息失败: %v", err)
	}
	if msgType, ok := connectionMsg["type"].(string); !ok || msgType != "connection" {
		return nil, fmt.Errorf("意外的连接消息: %v", connectionMsg)
	}
	fmt.Printf("   ✅ WebSocket连接成功\n")

	// 分块发送音频数据（每块约20ms）
	chunkSize := sampleRate * 20 / 1000 // 20ms的样本数
	totalChunks := (len(audioData) + chunkSize - 1) / chunkSize

	// 如果指定了最大帧数，限制要发送的块数
	chunksToSend := totalChunks
	if maxFrames > 0 && maxFrames < totalChunks {
		chunksToSend = maxFrames
		fmt.Printf("   开始发送音频数据（发送前 %d/%d 块，每块约 %d 样本）...\n", chunksToSend, totalChunks, chunkSize)
	} else {
		fmt.Printf("   开始发送音频数据（分 %d 块，每块约 %d 样本）...\n", totalChunks, chunkSize)
	}

	// 启动goroutine接收消息
	resultChan := make(chan *IdentifyResult, 1)
	errorChan := make(chan error, 1)

	go func() {
		for {
			messageType, message, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					errorChan <- fmt.Errorf("WebSocket读取错误: %v", err)
				}
				return
			}

			if messageType == websocket.TextMessage {
				var msg map[string]interface{}
				if err := json.Unmarshal(message, &msg); err != nil {
					fmt.Printf("   ⚠️  无法解析消息: %v\n", err)
					continue
				}

				if msgType, ok := msg["type"].(string); ok {
					switch msgType {
					case "audio_received":
						// 音频接收确认
						if samples, ok := msg["samples"].(float64); ok {
							fmt.Printf("   📦 服务器确认收到 %d 样本\n", int(samples))
						}
						continue
					case "result":
						if resultData, ok := msg["result"].(map[string]interface{}); ok {
							result := &IdentifyResult{
								Identified:  getBool(resultData, "identified"),
								SpeakerID:   getString(resultData, "speaker_id"),
								SpeakerName: getString(resultData, "speaker_name"),
								Confidence:  getFloat32(resultData, "confidence"),
								Threshold:   getFloat32(resultData, "threshold"),
							}
							resultChan <- result
							return
						}
					case "error":
						if errMsg, ok := msg["message"].(string); ok {
							errorChan <- fmt.Errorf("服务器错误: %s", errMsg)
							return
						}
					default:
						fmt.Printf("   ⚠️  收到未知消息类型: %s, 内容: %v\n", msgType, msg)
					}
				} else {
					fmt.Printf("   ⚠️  消息格式异常: %v\n", msg)
				}
			} else {
				fmt.Printf("   ⚠️  收到非文本消息，类型: %d\n", messageType)
			}
		}
	}()

	// 发送音频数据块
	totalSamplesSent := 0
	currentChunk := 0
	for i := 0; i < len(audioData); i += chunkSize {
		// 如果指定了最大帧数，检查是否已达到限制（在发送前检查）
		if maxFrames > 0 && currentChunk >= maxFrames {
			fmt.Printf("   ⚠️  已达到最大帧数限制 (%d 帧)，停止发送\n", maxFrames)
			break
		}

		end := i + chunkSize
		if end > len(audioData) {
			end = len(audioData)
		}

		chunk := audioData[i:end]
		chunkBytes := float32ToBytes(chunk)
		totalSamplesSent += len(chunk)
		currentChunk++

		if err := conn.WriteMessage(websocket.BinaryMessage, chunkBytes); err != nil {
			return nil, fmt.Errorf("发送音频数据失败: %v", err)
		}

		// 显示发送进度
		shouldPrint := currentChunk%10 == 0 || end == len(audioData) || (maxFrames > 0 && currentChunk >= maxFrames)
		if shouldPrint {
			fmt.Printf("   已发送 %d/%d 块 (共 %d 样本)\n", currentChunk, chunksToSend, totalSamplesSent)
		}
	}

	if totalSamplesSent != len(audioData) {
		fmt.Printf("   ⚠️  警告: 发送的样本数 (%d) 与总样本数 (%d) 不匹配\n", totalSamplesSent, len(audioData))
	}

	fmt.Printf("   ✅ 音频数据发送完成\n")

	// 发送完成命令
	finishCmd := map[string]interface{}{
		"action": "finish",
	}
	if err := conn.WriteJSON(finishCmd); err != nil {
		return nil, fmt.Errorf("发送完成命令失败: %v", err)
	}
	fmt.Printf("   ✅ 已发送完成命令，等待识别结果...\n")

	// 等待结果
	select {
	case result := <-resultChan:
		// 显示识别详情
		if !result.Identified {
			fmt.Printf("   ⚠️  识别失败: 相似度 %.4f < 阈值 %.4f\n", result.Confidence, result.Threshold)
		}
		return result, nil
	case err := <-errorChan:
		return nil, err
	case <-time.After(15 * time.Second):
		return nil, fmt.Errorf("等待识别结果超时（15秒）")
	}
}

// 辅助函数：从map中安全获取值
func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func getBool(m map[string]interface{}, key string) bool {
	if v, ok := m[key].(bool); ok {
		return v
	}
	return false
}

func getFloat32(m map[string]interface{}, key string) float32 {
	if v, ok := m[key].(float64); ok {
		return float32(v)
	}
	return 0.0
}

// listSpeakersFunc 获取声纹列表
func listSpeakersFunc(uid string, agentID string) error {
	// 构建 URL，安全编码参数
	apiURL, err := url.Parse(speakerAPI + "/list")
	if err != nil {
		return fmt.Errorf("解析URL失败: %v", err)
	}
	params := url.Values{}
	params.Set("uid", uid)
	if agentID != "" {
		params.Set("agent_id", agentID)
	}
	apiURL.RawQuery = params.Encode()

	// 创建 HTTP 请求
	req, err := http.NewRequest("GET", apiURL.String(), nil)
	if err != nil {
		return fmt.Errorf("创建请求失败: %v", err)
	}

	req.Header.Set("X-User-ID", uid) // 同时通过请求头传递 uid
	if agentID != "" {
		req.Header.Set("X-Agent-ID", agentID) // 同时通过请求头传递 agent_id
	}

	// 发送请求
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("发送请求失败: %v", err)
	}
	defer resp.Body.Close()

	// 读取响应
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("读取响应失败: %v", err)
	}

	// 检查状态码
	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if err := json.Unmarshal(body, &errResp); err == nil {
			return fmt.Errorf("HTTP %d: %s", resp.StatusCode, errResp.Error)
		}
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	// 解析响应
	var listResp ListResponse
	if err := json.Unmarshal(body, &listResp); err != nil {
		return fmt.Errorf("解析响应失败: %v", err)
	}

	// 显示结果
	fmt.Println("\n声纹列表:")
	fmt.Println("========================================")
	fmt.Printf("用户ID: %s\n", listResp.UID)
	fmt.Printf("总数: %d\n", listResp.Total)

	if len(listResp.Speakers) == 0 {
		fmt.Println("\n暂无已注册的声纹")
	} else {
		fmt.Println("\n说话人列表:")
		fmt.Println("----------------------------------------")
		for i, speaker := range listResp.Speakers {
			fmt.Printf("%d. 说话人ID: %s\n", i+1, speaker.ID)
			fmt.Printf("   说话人名称: %s\n", speaker.Name)
			fmt.Printf("   样本数量: %d\n", speaker.SampleCount)
			fmt.Printf("   创建时间: %s\n", speaker.CreatedAt)
			fmt.Printf("   更新时间: %s\n", speaker.UpdatedAt)
			if i < len(listResp.Speakers)-1 {
				fmt.Println()
			}
		}
	}
	fmt.Println("========================================")

	return nil
}

// deleteSpeaker 删除声纹
func deleteSpeaker(speakerID string, uid string, agentID string) error {
	// 构建 URL，安全编码路径参数
	apiURL, err := url.Parse(speakerAPI)
	if err != nil {
		return fmt.Errorf("解析URL失败: %v", err)
	}
	// 使用 PathEscape 编码 speakerID，确保特殊字符正确处理
	apiURL.Path += "/" + url.PathEscape(speakerID)
	params := url.Values{}
	params.Set("uid", uid)
	if agentID != "" {
		params.Set("agent_id", agentID)
	}
	apiURL.RawQuery = params.Encode()

	// 创建 HTTP DELETE 请求
	req, err := http.NewRequest("DELETE", apiURL.String(), nil)
	if err != nil {
		return fmt.Errorf("创建请求失败: %v", err)
	}

	req.Header.Set("X-User-ID", uid) // 同时通过请求头传递 uid
	if agentID != "" {
		req.Header.Set("X-Agent-ID", agentID) // 同时通过请求头传递 agent_id
	}

	// 发送请求
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("发送请求失败: %v", err)
	}
	defer resp.Body.Close()

	// 读取响应
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("读取响应失败: %v", err)
	}

	// 检查状态码
	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if err := json.Unmarshal(body, &errResp); err == nil {
			return fmt.Errorf("HTTP %d: %s", resp.StatusCode, errResp.Error)
		}
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	// 解析响应
	var deleteResp DeleteResponse
	if err := json.Unmarshal(body, &deleteResp); err != nil {
		return fmt.Errorf("解析响应失败: %v", err)
	}

	fmt.Printf("   用户ID: %s\n", deleteResp.UID)
	fmt.Printf("   说话人ID: %s\n", deleteResp.SpeakerID)
	fmt.Printf("   消息: %s\n", deleteResp.Message)

	return nil
}
