package ptc

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	rlmcore "github.com/XiaoConstantine/rlm-go/pkg/core"
	rlmgo "github.com/XiaoConstantine/rlm-go/pkg/rlm"
	"github.com/XiaoConstantine/rlm-go/pkg/sandbox"
	"github.com/esengine/DeepSeek-Reasonix/extensions/specptc/internal/engine"
)

type stagedRootClient struct {
	started <-chan string
	release chan struct{}
}

func (f *stagedRootClient) Complete(ctx context.Context, messages []rlmcore.Message) (rlmcore.LLMResponse, error) {
	return rlmcore.LLMResponse{}, nil
}

func (f *stagedRootClient) CompleteStream(
	ctx context.Context,
	messages []rlmcore.Message,
	handler rlmgo.StreamHandler,
) (rlmcore.LLMResponse, error) {
	first := "```go\nanswer := QueryRaw(\"sub prompt\")\n"
	last := "FINAL(answer)\n```\n"
	if err := handler(first, false); err != nil {
		return rlmcore.LLMResponse{}, err
	}
	select {
	case <-ctx.Done():
		return rlmcore.LLMResponse{}, ctx.Err()
	case <-f.started:
	}
	close(f.release)
	if err := handler(last, false); err != nil {
		return rlmcore.LLMResponse{}, err
	}
	if err := handler("", true); err != nil {
		return rlmcore.LLMResponse{}, err
	}
	return rlmcore.LLMResponse{Content: first + last, PromptTokens: 10, CompletionTokens: 8}, nil
}

type stagedDependentRootClient struct {
	started <-chan string
	release chan struct{}
}

func (f *stagedDependentRootClient) Complete(context.Context, []rlmcore.Message) (rlmcore.LLMResponse, error) {
	return rlmcore.LLMResponse{}, nil
}

func (f *stagedDependentRootClient) CompleteStream(
	ctx context.Context,
	_ []rlmcore.Message,
	handler rlmgo.StreamHandler,
) (rlmcore.LLMResponse, error) {
	first := "```go\ndraft := QueryRaw(\"draft\")\njudge := QueryRaw(\"judge:\" + draft)\n"
	last := "FINAL(judge)\n```\n"
	if err := handler(first, false); err != nil {
		return rlmcore.LLMResponse{}, err
	}
	select {
	case <-ctx.Done():
		return rlmcore.LLMResponse{}, ctx.Err()
	case <-f.started:
	}
	close(f.release)
	select {
	case <-ctx.Done():
		return rlmcore.LLMResponse{}, ctx.Err()
	case prompt := <-f.started:
		if prompt != "judge:response:draft" {
			return rlmcore.LLMResponse{}, errors.New("unexpected dependent prompt: " + prompt)
		}
	}
	if err := handler(last, false); err != nil {
		return rlmcore.LLMResponse{}, err
	}
	_ = handler("", true)
	return rlmcore.LLMResponse{Content: first + last}, nil
}

func TestRLMRuntimeResumesDependentSpeculationBeforeRootStreamCloses(t *testing.T) {
	release := make(chan struct{})
	sub := &fakeRLMClient{started: make(chan string, 8), release: release}
	runtime := &RLMRuntime{
		Root: &stagedDependentRootClient{started: sub.started, release: release},
		Sub:  sub,
		Config: RLMRuntimeConfig{
			Scheduler:        engine.Config{Workers: 2},
			RLM:              []rlmgo.Option{rlmgo.WithMaxIterations(2)},
			TrustedInProcess: true,
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	run, err := runtime.Complete(ctx, runtimeScope("dependent-runtime"), nil, "query", nil)
	if err != nil {
		t.Fatal(err)
	}
	if run.Completion == nil || run.Completion.Response != "response:judge:response:draft" {
		t.Fatalf("completion = %+v", run.Completion)
	}
	if sub.count() != 2 || run.Metrics.Hits != 2 {
		t.Fatalf("calls=%d metrics=%+v", sub.count(), run.Metrics)
	}
}

func TestConstrainedSandboxRejectsUnsafeProfiles(t *testing.T) {
	_, err := constrainedSandboxConfig(&sandbox.Config{
		Backend: sandbox.BackendLocal, NetworkMode: sandbox.NetworkNone, EnableIPC: true,
	})
	if err == nil || !strings.Contains(err.Error(), "unsafe sandbox backend") {
		t.Fatalf("local backend error = %v", err)
	}
	_, err = constrainedSandboxConfig(&sandbox.Config{
		Backend: sandbox.BackendAuto, NetworkMode: sandbox.NetworkHost, EnableIPC: true,
	})
	if err == nil || !strings.Contains(err.Error(), "unsafe sandbox network mode") {
		t.Fatalf("host network error = %v", err)
	}
	_, err = constrainedSandboxConfig(&sandbox.Config{
		Backend: sandbox.BackendAuto, NetworkMode: sandbox.NetworkNone, EnableIPC: false,
	})
	if err == nil || !strings.Contains(err.Error(), "IPC is required") {
		t.Fatalf("disabled IPC error = %v", err)
	}
}

func TestRLMRuntimeOverlapsRootStreamingAndClaimsInRealREPL(t *testing.T) {
	release := make(chan struct{})
	sub := &fakeRLMClient{started: make(chan string, 8), release: release}
	root := &stagedRootClient{started: sub.started, release: release}
	runtime := &RLMRuntime{
		Root: root,
		Sub:  sub,
		Config: RLMRuntimeConfig{
			Scheduler:        engine.Config{Workers: 2},
			RLM:              []rlmgo.Option{rlmgo.WithMaxIterations(2)},
			TrustedInProcess: true,
		},
	}
	var streamed strings.Builder
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	run, err := runtime.Complete(ctx, runtimeScope("full-runtime"), "context", "answer the question", func(chunk string, done bool) error {
		streamed.WriteString(chunk)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if run.Completion == nil || run.Completion.Response != "response:sub prompt" {
		t.Fatalf("completion = %+v", run.Completion)
	}
	if !strings.Contains(streamed.String(), "QueryRaw") || !strings.Contains(streamed.String(), "FINAL") {
		t.Fatalf("forwarded stream = %q", streamed.String())
	}
	if sub.count() != 1 {
		t.Fatalf("sub-model calls = %d, want one", sub.count())
	}
	if run.Metrics.Hits != 1 || run.Metrics.Misses != 0 {
		t.Fatalf("metrics = %+v", run.Metrics)
	}
}

func TestRLMContextStringMatchesStructuredREPLContext(t *testing.T) {
	got, err := rlmContextString(map[string]any{"b": 2.0, "a": "x"})
	if err != nil {
		t.Fatal(err)
	}
	if got != `{"a":"x","b":2}` {
		t.Fatalf("context string = %q", got)
	}
}
