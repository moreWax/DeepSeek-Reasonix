package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync/atomic"

	rlmgo "github.com/XiaoConstantine/rlm-go/pkg/rlm"
	extension "github.com/esengine/DeepSeek-Reasonix/sdk/go"

	"github.com/esengine/DeepSeek-Reasonix/extensions/specptc/internal/engine"
	"github.com/esengine/DeepSeek-Reasonix/extensions/specptc/internal/ptc"
)

const rlmProviderRef = "plugin/spec-ptc/rlm"

type rlmCompleter interface {
	CompleteControlled(context.Context, engine.Scope, any, string, rlmgo.StreamHandler, ptc.RLMRunControls) (ptc.RLMRunResult, error)
}

type specPTCProvider struct {
	model     string
	runtime   rlmCompleter
	log       *log.Logger
	next      atomic.Uint64
	uiBinding atomic.Pointer[providerUIBinding]
}

func (p *specPTCProvider) bindUI(params extension.InitializeParams) {
	if params.Capabilities.UIHost == extension.UIHostHeadless {
		p.uiBinding.Store(nil)
		return
	}
	p.uiBinding.Store(&providerUIBinding{
		sessionID: params.Session.SessionID, generation: params.Session.Generation,
		host: params.Capabilities.UIHost,
	})
}

func (p *specPTCProvider) Catalog(context.Context) ([]extension.ProviderDescriptor, error) {
	if p.runtime == nil {
		return []extension.ProviderDescriptor{}, nil
	}
	return []extension.ProviderDescriptor{rlmProviderDescriptor(p.model)}, nil
}

func rlmProviderDescriptor(model string) extension.ProviderDescriptor {
	return extension.ProviderDescriptor{
		Ref: rlmProviderRef, DisplayName: "RLM + speculative PTC", Model: model,
		InputModalities: []string{"text"}, Reasoning: true,
	}
}

func (p *specPTCProvider) Stream(ctx context.Context, req extension.StreamRequest) (<-chan extension.StreamChunk, error) {
	if req.ProviderRef != rlmProviderRef {
		return nil, fmt.Errorf("specptc: unknown provider ref %q", req.ProviderRef)
	}
	if p.runtime == nil {
		return nil, errors.New("specptc: RLM provider is not configured")
	}
	if req.Request.Temperature != nil {
		return nil, errors.New("specptc: RLM provider does not support request temperature")
	}
	if req.Request.MaxTokens != 0 {
		return nil, errors.New("specptc: RLM provider does not support per-completion MaxTokens")
	}
	if req.Request.ResponseFormat != nil {
		return nil, errors.New("specptc: RLM provider does not support ResponseFormat")
	}
	contextPayload, query, err := splitRLMRequest(req.Request)
	if err != nil {
		return nil, err
	}
	out := make(chan extension.StreamChunk, 16)
	generation := p.next.Add(1)
	go func() {
		defer close(out)
		uiTrace := newRLMUITrace(ctx, p.uiBinding.Load(), req.StreamID, p.log)
		scope := engine.Scope{
			Generation: generation,
			SessionID:  "provider:" + req.StreamID,
			TurnID:     req.StreamID,
			AttemptID:  "rlm",
		}
		run, runErr := p.runtime.CompleteControlled(
			ctx, scope, contextPayload, query,
			func(chunk string, done bool) error {
				if chunk == "" {
					return nil
				}
				return sendProviderChunk(ctx, out, extension.ReasoningChunk(chunk, ""))
			},
			ptc.RLMRunControls{Trace: func(event ptc.QueryTraceEvent) {
				if uiTrace != nil {
					uiTrace.observe(event)
				}
			}},
		)
		if uiTrace != nil {
			uiTrace.finish(run.Metrics)
		}
		if runErr != nil {
			if p.log != nil {
				p.log.Printf("RLM stream %s failed: %v", req.StreamID, runErr)
			}
			_ = sendProviderChunk(ctx, out, extension.ErrorChunk("RLM provider execution failed"))
			return
		}
		metrics := run.Metrics
		status := fmt.Sprintf("\n[sPTC dispatched=%d hits=%d misses=%d wasted=%d evictions=%d cancelled=%d saved_ms=%d actual_wait_ms=%d]\n",
			metrics.Dispatched, metrics.Hits, metrics.Misses, metrics.Wasted, metrics.Evictions, metrics.Cancelled,
			metrics.Saved.Milliseconds(), metrics.ActualWait.Milliseconds())
		if err := sendProviderChunk(ctx, out, extension.ReasoningChunk(status, "")); err != nil {
			return
		}
		if run.Completion != nil {
			if run.Completion.Response != "" {
				if err := sendProviderChunk(ctx, out, extension.TextChunk(run.Completion.Response)); err != nil {
					return
				}
			}
			usage := run.Completion.Usage
			if err := sendProviderChunk(ctx, out, extension.UsageChunk(extension.ProviderUsage{
				PromptTokens: usage.PromptTokens, CompletionTokens: usage.CompletionTokens,
				TotalTokens: usage.TotalTokens, FinishReason: "stop",
			})); err != nil {
				return
			}
		}
		_ = sendProviderChunk(ctx, out, extension.DoneChunk())
	}()
	return out, nil
}

func sendProviderChunk(ctx context.Context, out chan<- extension.StreamChunk, chunk extension.StreamChunk) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case out <- chunk:
		return nil
	}
}

type rlmTranscriptContext struct {
	Messages []extension.ProviderMessage    `json:"messages,omitempty"`
	Tools    []extension.ProviderToolSchema `json:"tools,omitempty"`
}

func splitRLMRequest(req extension.ProviderRequest) (any, string, error) {
	if len(req.Messages) == 0 {
		return nil, "", errors.New("specptc: RLM provider requires a non-empty user message")
	}
	last := req.Messages[len(req.Messages)-1]
	if last.Role != extension.ProviderRoleUser || last.Content == "" {
		return nil, "", errors.New("specptc: RLM provider requires the final message to be a non-empty user turn")
	}
	messages := append([]extension.ProviderMessage(nil), req.Messages[:len(req.Messages)-1]...)
	contextPayload := rlmTranscriptContext{Messages: messages, Tools: req.Tools}
	// Verify early that the payload can be loaded by both the real and shadow
	// runtimes before opening an asynchronous provider stream.
	if _, err := json.Marshal(contextPayload); err != nil {
		return nil, "", fmt.Errorf("specptc: encode RLM transcript: %w", err)
	}
	return contextPayload, last.Content, nil
}
