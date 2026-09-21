package main

import (
	"context"
	"io"
	"log"
	"reflect"
	"strings"
	"testing"

	rlmcore "github.com/XiaoConstantine/rlm-go/pkg/core"
	rlmgo "github.com/XiaoConstantine/rlm-go/pkg/rlm"
	extension "github.com/esengine/DeepSeek-Reasonix/sdk/go"

	"github.com/esengine/DeepSeek-Reasonix/extensions/specptc/internal/engine"
	"github.com/esengine/DeepSeek-Reasonix/extensions/specptc/internal/ptc"
)

type fakeRLMCompleter struct {
	contextPayload any
	query          string
	scope          engine.Scope
	controls       ptc.RLMRunControls
	response       string
}

func (f *fakeRLMCompleter) CompleteControlled(
	_ context.Context,
	scope engine.Scope,
	contextPayload any,
	query string,
	stream rlmgo.StreamHandler,
	controls ptc.RLMRunControls,
) (ptc.RLMRunResult, error) {
	f.contextPayload, f.query, f.scope, f.controls = contextPayload, query, scope, controls
	if err := stream("```go\n", false); err != nil {
		return ptc.RLMRunResult{}, err
	}
	return ptc.RLMRunResult{Completion: &rlmcore.CompletionResult{
		Response: f.response, Usage: rlmcore.UsageStats{PromptTokens: 4, CompletionTokens: 2, TotalTokens: 6},
	}}, nil
}

func TestSpecPTCProviderStreamsReasoningFinalUsageAndDone(t *testing.T) {
	completer := &fakeRLMCompleter{response: "answer"}
	provider := &specPTCProvider{model: "root-model", runtime: completer, log: log.New(io.Discard, "", 0)}
	chunks, err := provider.Stream(context.Background(), extension.StreamRequest{
		StreamID: "stream-1", ProviderRef: rlmProviderRef,
		Request: extension.ProviderRequest{Messages: []extension.ProviderMessage{
			{Role: extension.ProviderRoleSystem, Content: "be exact"},
			{Role: extension.ProviderRoleUser, Content: "question"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var got []extension.StreamChunk
	for chunk := range chunks {
		got = append(got, chunk)
	}
	if len(got) != 5 || got[0].Type != extension.ChunkReasoning || got[1].Type != extension.ChunkReasoning ||
		got[2].Type != extension.ChunkText || got[3].Type != extension.ChunkUsage || got[4].Type != extension.ChunkDone {
		t.Fatalf("chunks = %+v", got)
	}
	if !strings.Contains(got[1].Text, "sPTC dispatched=") {
		t.Fatalf("metrics chunk = %q", got[1].Text)
	}
	if completer.query != "question" || !completer.scope.Valid() || completer.controls.MaxTokens != 0 {
		t.Fatalf("query=%q scope=%+v controls=%+v", completer.query, completer.scope, completer.controls)
	}
	payload, ok := completer.contextPayload.(rlmTranscriptContext)
	if !ok || len(payload.Messages) != 1 || payload.Messages[0].Role != extension.ProviderRoleSystem {
		t.Fatalf("context payload = %#v", completer.contextPayload)
	}
}

func TestSpecPTCProviderEmitsUsageForEmptyCompletion(t *testing.T) {
	provider := &specPTCProvider{runtime: &fakeRLMCompleter{}, log: log.New(io.Discard, "", 0)}
	chunks, err := provider.Stream(context.Background(), extension.StreamRequest{
		StreamID: "empty", ProviderRef: rlmProviderRef,
		Request: extension.ProviderRequest{Messages: []extension.ProviderMessage{{
			Role: extension.ProviderRoleUser, Content: "question",
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var types []extension.ProviderChunkType
	for chunk := range chunks {
		types = append(types, chunk.Type)
	}
	want := []extension.ProviderChunkType{
		extension.ChunkReasoning, extension.ChunkReasoning, extension.ChunkUsage, extension.ChunkDone,
	}
	if !reflect.DeepEqual(types, want) {
		t.Fatalf("chunk types = %v, want %v", types, want)
	}
}

func TestSplitRLMRequestRejectsNonUserContinuation(t *testing.T) {
	_, _, err := splitRLMRequest(extension.ProviderRequest{Messages: []extension.ProviderMessage{
		{Role: extension.ProviderRoleUser, Content: "old"},
		{Role: extension.ProviderRoleAssistant, Content: "reply"},
		{Role: extension.ProviderRoleTool, Content: "late tool output"},
	}})
	if err == nil {
		t.Fatal("trailing non-user continuation was accepted")
	}
}

func TestSpecPTCProviderRejectsUnknownRefAndMissingUser(t *testing.T) {
	provider := &specPTCProvider{runtime: &fakeRLMCompleter{}}
	if _, err := provider.Stream(context.Background(), extension.StreamRequest{ProviderRef: "other"}); err == nil {
		t.Fatal("unknown provider ref was accepted")
	}
	if _, err := provider.Stream(context.Background(), extension.StreamRequest{ProviderRef: rlmProviderRef}); err == nil {
		t.Fatal("request without user message was accepted")
	}
	temperature := 0.5
	if _, err := provider.Stream(context.Background(), extension.StreamRequest{
		ProviderRef: rlmProviderRef,
		Request: extension.ProviderRequest{
			Temperature: &temperature,
			Messages:    []extension.ProviderMessage{{Role: extension.ProviderRoleUser, Content: "question"}},
		},
	}); err == nil {
		t.Fatal("unsupported request temperature was accepted")
	}
	for name, request := range map[string]extension.ProviderRequest{
		"MaxTokens": {
			MaxTokens: 10,
			Messages:  []extension.ProviderMessage{{Role: extension.ProviderRoleUser, Content: "question"}},
		},
		"ResponseFormat": {
			ResponseFormat: &extension.ProviderResponseFormat{Type: "json_object"},
			Messages:       []extension.ProviderMessage{{Role: extension.ProviderRoleUser, Content: "question"}},
		},
	} {
		if _, err := provider.Stream(context.Background(), extension.StreamRequest{
			ProviderRef: rlmProviderRef, Request: request,
		}); err == nil {
			t.Fatalf("unsupported %s was accepted", name)
		}
	}
}
