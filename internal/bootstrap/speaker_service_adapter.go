package bootstrap

import (
	"fmt"

	"voice_server/core"
	internalSpeaker "voice_server/internal/speaker"
)

// speakerServiceAdapter 将 internal/speaker.Manager 适配为 core.SpeakerService。
type speakerServiceAdapter struct {
	manager *internalSpeaker.Manager
}

type speakerStreamingSessionAdapter struct {
	session *internalSpeaker.StreamingIdentifier
}

func newSpeakerServiceAdapter(manager *internalSpeaker.Manager) core.SpeakerService {
	if manager == nil {
		return nil
	}
	return &speakerServiceAdapter{manager: manager}
}

func (a *speakerServiceAdapter) RegisterSpeaker(uid, agentID, speakerID, speakerName, uuid string, audioData []float32, sampleRate int) error {
	filteredAudio, err := a.manager.FilterSilenceWithVADKeepEdges(audioData, sampleRate)
	if err != nil {
		return fmt.Errorf("failed to filter silence: %w", err)
	}
	return a.manager.RegisterSpeaker(uid, agentID, speakerID, speakerName, uuid, filteredAudio, sampleRate)
}

func (a *speakerServiceAdapter) IdentifySpeaker(uid, agentID, speakerID, speakerName string, audioData []float32, sampleRate int, threshold ...float32) (*core.SpeakerIdentifyResult, error) {
	result, err := a.manager.IdentifySpeaker(uid, agentID, speakerID, speakerName, audioData, sampleRate, threshold...)
	if err != nil {
		return nil, err
	}
	return toCoreIdentifyResult(result), nil
}

func (a *speakerServiceAdapter) VerifySpeaker(uid, agentID, speakerID string, audioData []float32, sampleRate int) (*core.SpeakerVerifyResult, error) {
	result, err := a.manager.VerifySpeaker(uid, agentID, speakerID, audioData, sampleRate)
	if err != nil {
		return nil, err
	}
	return toCoreVerifyResult(result), nil
}

func (a *speakerServiceAdapter) DeleteSpeaker(uid, agentID, speakerID string) error {
	return a.manager.DeleteSpeaker(uid, agentID, speakerID)
}

func (a *speakerServiceAdapter) DeleteSpeakerByUUID(uid, agentID, uuid string) error {
	return a.manager.DeleteSpeakerByUUID(uid, agentID, uuid)
}

func (a *speakerServiceAdapter) GetAllSpeakers(uid, agentID string) []*core.SpeakerInfo {
	speakers := a.manager.GetAllSpeakers(uid, agentID)
	results := make([]*core.SpeakerInfo, 0, len(speakers))
	for _, speaker := range speakers {
		if speaker == nil {
			continue
		}
		results = append(results, &core.SpeakerInfo{
			ID:          speaker.ID,
			Name:        speaker.Name,
			UUID:        speaker.UUID,
			AgentID:     speaker.AgentID,
			SampleCount: speaker.SampleCount,
			CreatedAt:   speaker.CreatedAt,
			UpdatedAt:   speaker.UpdatedAt,
		})
	}
	return results
}

func (a *speakerServiceAdapter) GetStats(uid, agentID string) map[string]interface{} {
	return a.manager.GetStats(uid, agentID)
}

func (a *speakerServiceAdapter) NewStreamingSession(uid, agentID, speakerID, speakerName string, sampleRate int, threshold ...float32) (core.SpeakerStreamingSession, error) {
	session := a.manager.NewStreamingIdentifier(uid, agentID, speakerID, speakerName, sampleRate, threshold...)
	if session == nil {
		return nil, fmt.Errorf("failed to create speaker streaming session")
	}
	return &speakerStreamingSessionAdapter{session: session}, nil
}

func (a *speakerStreamingSessionAdapter) AcceptAudio(audioData []float32) error {
	if a == nil || a.session == nil {
		return fmt.Errorf("speaker streaming session is not initialized")
	}
	return a.session.AcceptAudio(audioData)
}

func (a *speakerStreamingSessionAdapter) FinishAndIdentify() (*core.SpeakerIdentifyResult, error) {
	if a == nil || a.session == nil {
		return nil, fmt.Errorf("speaker streaming session is not initialized")
	}
	result, err := a.session.FinishAndIdentify()
	if err != nil {
		return nil, err
	}
	return toCoreIdentifyResult(result), nil
}

func (a *speakerStreamingSessionAdapter) Close() {
	if a == nil || a.session == nil {
		return
	}
	a.session.Close()
	a.session = nil
}

func toCoreIdentifyResult(result *internalSpeaker.IdentifyResult) *core.SpeakerIdentifyResult {
	if result == nil {
		return nil
	}
	return &core.SpeakerIdentifyResult{
		Identified:  result.Identified,
		SpeakerID:   result.SpeakerID,
		SpeakerName: result.SpeakerName,
		Confidence:  result.Confidence,
		Threshold:   result.Threshold,
	}
}

func toCoreVerifyResult(result *internalSpeaker.VerifyResult) *core.SpeakerVerifyResult {
	if result == nil {
		return nil
	}
	return &core.SpeakerVerifyResult{
		SpeakerID:   result.SpeakerID,
		SpeakerName: result.SpeakerName,
		Verified:    result.Verified,
		Confidence:  result.Confidence,
		Threshold:   result.Threshold,
	}
}
