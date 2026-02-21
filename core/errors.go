package core

import "errors"

var (
	// ErrRecognitionFailed 表示解码阶段未拿到识别结果对象。
	ErrRecognitionFailed = errors.New("recognition failed")
)
