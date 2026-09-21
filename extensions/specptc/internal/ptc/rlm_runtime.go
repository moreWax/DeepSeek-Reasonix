package ptc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	rlmcore "github.com/XiaoConstantine/rlm-go/pkg/core"
	rlmgo "github.com/XiaoConstantine/rlm-go/pkg/rlm"
	"github.com/XiaoConstantine/rlm-go/pkg/sandbox"
	"github.com/esengine/DeepSeek-Reasonix/extensions/specptc/internal/engine"
)

// RLMRuntimeConfig controls one combined rlm-go and streamed-sPTC runtime.
type RLMRuntimeConfig struct {
	Scheduler engine.Config
	Queries   QueryLimits
	RLM       []rlmgo.Option

	// Sandbox optionally overrides rlm-go's constrained worker settings. When
	// nil, rlm-go's auto-selected Podman/Docker sandbox is required.
	Sandbox *sandbox.Config
	// TrustedInProcess is intended only for tests and explicitly trusted code.
	// Model-generated production code must leave this false.
	TrustedInProcess bool
}

// RLMRuntime owns the root model loop, authoritative REPL, speculative shadow
// planning, and sub-model claims for one extension implementation.
type RLMRuntime struct {
	Root   rlmgo.LLMClient
	Sub    RLMClient
	Config RLMRuntimeConfig

	queryOnce sync.Once
	querySem  chan struct{}
}

func (r *RLMRuntime) processQuerySemaphore() chan struct{} {
	r.queryOnce.Do(func() {
		limits := r.Config.Queries.normalized()
		r.querySem = make(chan struct{}, limits.MaxConcurrent)
	})
	return r.querySem
}

// RLMRunResult combines the authoritative RLM result with turn-level sPTC
// observability.
type RLMRunResult struct {
	Completion *rlmcore.CompletionResult
	Metrics    engine.Metrics
}

// RLMRunControls are request-scoped limits that must not be shared across
// concurrent provider streams.
type RLMRunControls struct {
	MaxTokens int
}

// Complete executes one RLM turn. The streamed root response is forwarded
// unchanged while complete speculative calls are launched from partial code.
func (r *RLMRuntime) Complete(
	ctx context.Context,
	scope engine.Scope,
	contextPayload any,
	query string,
	downstream rlmgo.StreamHandler,
) (RLMRunResult, error) {
	return r.CompleteControlled(ctx, scope, contextPayload, query, downstream, RLMRunControls{})
}

// CompleteControlled is Complete with request-local limits.
func (r *RLMRuntime) CompleteControlled(
	ctx context.Context,
	scope engine.Scope,
	contextPayload any,
	query string,
	downstream rlmgo.StreamHandler,
	controls RLMRunControls,
) (run RLMRunResult, err error) {
	if r == nil || r.Root == nil || r.Sub == nil {
		return run, errors.New("ptc: RLM runtime requires root and sub-model clients")
	}
	if !scope.Valid() {
		return run, errors.New("ptc: invalid RLM runtime scope")
	}
	contextValue, err := rlmContextString(contextPayload)
	if err != nil {
		return run, err
	}
	guard := newQueryGuardWithGlobal(r.Config.Queries, r.processQuerySemaphore())
	guardedSub := guardedRLMClient{client: r.Sub, guard: guard}
	scheduler, backend, err := NewQueryRuntime(RLMQueryExecutor{Client: guardedSub}, r.Config.Scheduler)
	if err != nil {
		return run, err
	}
	defer scheduler.Close()
	if err := scheduler.Begin(ctx, scope); err != nil {
		return run, err
	}
	defer func() {
		metrics, endErr := scheduler.End(context.Background(), scope)
		backend.ReleaseScope(scope)
		run.Metrics = metrics
		if err == nil && endErr != nil {
			err = endErr
		}
	}()

	bridge := NewStreamBridge(
		Planner{Tools: RLMToolPolicies(contextValue)},
		nil,
		EngineSink{Engine: scheduler, Backend: backend, Scope: scope},
	)
	backend.SetResultHandler(func(id string, arguments []byte, value any) {
		if response, ok := value.(rlmcore.QueryResponse); ok {
			bridge.Resolve(id, arguments, response.Response)
		}
	})
	claiming := ClaimingRLMClient{
		Scheduler: scheduler, Backend: backend, Scope: scope,
		Guard: guard,
	}
	opts := append([]rlmgo.Option{}, r.Config.RLM...)
	if controls.MaxTokens > 0 {
		opts = append(opts, rlmgo.WithMaxTokens(controls.MaxTokens))
	}
	if !r.Config.TrustedInProcess {
		sandboxConfig, sandboxErr := constrainedSandboxConfig(r.Config.Sandbox)
		if sandboxErr != nil {
			return run, sandboxErr
		}
		opts = append(opts, rlmgo.WithSandboxConfig(sandboxConfig))
	}
	// These options are appended last so callers cannot bypass streamed planning.
	opts = append(opts, rlmgo.WithStreaming(true), rlmgo.WithStreamHandler(bridge.Handler(downstream)))
	machine := rlmgo.New(constrainedRootClient{client: r.Root}, claiming, opts...)
	run.Completion, err = machine.Complete(ctx, contextPayload, query)
	return run, err
}

func constrainedSandboxConfig(configured *sandbox.Config) (sandbox.Config, error) {
	config := sandbox.DefaultConfig()
	if configured != nil {
		config = *configured
	}
	if config.NetworkMode != sandbox.NetworkNone {
		return sandbox.Config{}, fmt.Errorf("ptc: unsafe sandbox network mode %q is not allowed", config.NetworkMode)
	}
	if !config.EnableIPC {
		return sandbox.Config{}, errors.New("ptc: sandbox IPC is required for authoritative Query calls")
	}
	switch config.Backend {
	case sandbox.BackendPodman:
		if !sandbox.IsRuntimeAvailable("podman") {
			return sandbox.Config{}, errors.New("ptc: Podman sandbox is not available")
		}
	case sandbox.BackendDocker:
		if !sandbox.IsRuntimeAvailable("docker") {
			return sandbox.Config{}, errors.New("ptc: Docker sandbox is not available")
		}
	case sandbox.BackendAuto, "":
		switch {
		case sandbox.IsRuntimeAvailable("podman"):
			config.Backend = sandbox.BackendPodman
		case sandbox.IsRuntimeAvailable("docker"):
			config.Backend = sandbox.BackendDocker
		default:
			return sandbox.Config{}, errors.New("ptc: no container sandbox available; install Podman or Docker")
		}
	default:
		return sandbox.Config{}, fmt.Errorf("ptc: unsafe sandbox backend %q is not allowed", config.Backend)
	}
	config.Memory = "256m"
	config.CPUs = 1
	if config.Timeout <= 0 || config.Timeout > 60*time.Second {
		config.Timeout = 60 * time.Second
	}
	return config, nil
}

func rlmContextString(payload any) (string, error) {
	if payload == nil {
		return "", nil
	}
	if value, ok := payload.(string); ok {
		return value, nil
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("ptc: marshal RLM context: %w", err)
	}
	return string(encoded), nil
}
