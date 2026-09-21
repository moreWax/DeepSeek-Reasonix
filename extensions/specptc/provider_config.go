package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/XiaoConstantine/rlm-go/pkg/providers"
	rlmgo "github.com/XiaoConstantine/rlm-go/pkg/rlm"
	"github.com/XiaoConstantine/rlm-go/pkg/sandbox"

	"github.com/esengine/DeepSeek-Reasonix/extensions/specptc/internal/codex"
	"github.com/esengine/DeepSeek-Reasonix/extensions/specptc/internal/engine"
	"github.com/esengine/DeepSeek-Reasonix/extensions/specptc/internal/ptc"
)

const codexProvider providers.Provider = "codex"

func newRLMProvider(logger *log.Logger, scheduler engine.Config) *specPTCProvider {
	rootModel := envString("REASONIX_RLM_ROOT_MODEL", defaultRLMRootModel())
	provider := &specPTCProvider{model: rootModel, log: logger}
	subModel, err := configuredRLMSubModel(rootModel)
	if err != nil {
		logger.Printf("RLM provider unavailable: %v", err)
		return provider
	}
	rootProvider := os.Getenv("REASONIX_RLM_ROOT_PROVIDER")
	subProvider := configuredRLMSubProvider(rootProvider)
	codexManager := codex.NewManager(codex.AuthConfig{})
	root, err := newRLMModelClient(rootModel, rootProvider, codexManager)
	if err != nil {
		logger.Printf("RLM provider unavailable: %v", err)
		return provider
	}
	sub, err := newRLMModelClient(subModel, subProvider, codexManager)
	if err != nil {
		logger.Printf("RLM provider unavailable: %v", err)
		return provider
	}
	containerAvailable := sandbox.IsRuntimeAvailable("podman") || sandbox.IsRuntimeAvailable("docker")
	trusted := useTrustedInProcess(containerAvailable)
	switch {
	case !containerAvailable:
		logger.Printf("warning: no Podman/Docker runtime found; model-generated Go will execute locally in the extension process without OS isolation")
	case trusted:
		logger.Printf("warning: trusted in-process RLM execution forced; model-generated Go will execute without OS isolation")
	}
	runtimeOptions := []rlmgo.Option{
		rlmgo.WithMaxIterations(envInt("REASONIX_RLM_MAX_ITERATIONS", 30)),
		rlmgo.WithMaxTimeout(time.Duration(envInt("REASONIX_RLM_TIMEOUT_SECONDS", 300)) * time.Second),
	}
	if maxTokens := envInt("REASONIX_RLM_MAX_TOKENS", 0); maxTokens > 0 {
		runtimeOptions = append(runtimeOptions, rlmgo.WithMaxTokens(maxTokens))
	}
	provider.runtime = &ptc.RLMRuntime{
		Root: root,
		Sub:  sub,
		Config: ptc.RLMRuntimeConfig{
			Scheduler: scheduler,
			Queries: ptc.QueryLimits{
				MaxBatch:       envInt("REASONIX_RLM_MAX_BATCH", 64),
				MaxPromptBytes: envInt("REASONIX_RLM_MAX_PROMPT_BYTES", 1<<20),
				MaxConcurrent:  envInt("REASONIX_RLM_MAX_CONCURRENT_QUERIES", 8),
				MaxCalls:       int64(envInt("REASONIX_RLM_MAX_QUERIES", 256)),
			},
			RLM:              runtimeOptions,
			TrustedInProcess: trusted,
		},
	}
	return provider
}

func defaultRLMRootModel() string {
	explicit := strings.ToLower(strings.TrimSpace(os.Getenv("REASONIX_RLM_ROOT_PROVIDER")))
	if (explicit == "" || explicit == string(codexProvider)) && codex.Available(codex.AuthConfig{}) {
		return codex.DefaultModel
	}
	return "gpt-5-mini"
}

func configuredRLMSubProvider(rootProvider string) string {
	if subProvider := strings.TrimSpace(os.Getenv("REASONIX_RLM_SUB_PROVIDER")); subProvider != "" {
		return subProvider
	}
	return rootProvider
}

func configuredRLMSubModel(rootModel string) (string, error) {
	if model := strings.TrimSpace(os.Getenv("REASONIX_RLM_SUB_MODEL")); model != "" {
		return model, nil
	}
	subProvider := strings.TrimSpace(os.Getenv("REASONIX_RLM_SUB_PROVIDER"))
	if subProvider == "" {
		return rootModel, nil
	}
	rootKind, err := resolveRLMProvider(rootModel, os.Getenv("REASONIX_RLM_ROOT_PROVIDER"))
	if err != nil {
		return "", err
	}
	subKind, err := resolveRLMProvider(rootModel, subProvider)
	if err != nil {
		return "", err
	}
	if rootKind != subKind {
		return "", errors.New("REASONIX_RLM_SUB_MODEL is required when root and sub providers differ")
	}
	return rootModel, nil
}

func newRLMModelClient(model, explicitProvider string, codexManager *codex.Manager) (providers.Client, error) {
	kind, err := resolveRLMProvider(model, explicitProvider)
	if err != nil {
		return nil, err
	}
	if kind == codexProvider {
		if !codex.Available(codex.AuthConfig{}) {
			return nil, errors.New("Codex subscription credentials are required; run Reasonix or Codex login first")
		}
		return codex.NewClient(codex.Config{
			Manager: codexManager,
			Model:   model,
			Effort:  envString("REASONIX_RLM_CODEX_EFFORT", "high"),
		}), nil
	}
	key := strings.TrimSpace(os.Getenv(kind.EnvKey()))
	if key == "" {
		return nil, fmt.Errorf("%s is required for %s model %q", kind.EnvKey(), kind, model)
	}
	switch kind {
	case providers.OpenAI:
		return providers.NewOpenAIClient(key, model, false), nil
	case providers.Gemini:
		return providers.NewGeminiClient(key, model, false), nil
	case providers.Anthropic:
		return providers.NewAnthropicClient(key, model, false), nil
	default:
		return nil, fmt.Errorf("unsupported RLM provider %q", kind)
	}
}

func resolveRLMProvider(model, explicit string) (providers.Provider, error) {
	if explicit == "" {
		if isCodexSubscriptionModel(model) {
			if codex.Available(codex.AuthConfig{}) {
				return codexProvider, nil
			}
			return providers.OpenAI, nil
		}
		return providers.GetProvider(model), nil
	}
	switch strings.ToLower(strings.TrimSpace(explicit)) {
	case string(codexProvider):
		return codexProvider, nil
	case string(providers.OpenAI):
		return providers.OpenAI, nil
	case string(providers.Gemini):
		return providers.Gemini, nil
	case string(providers.Anthropic):
		return providers.Anthropic, nil
	default:
		return "", errors.New("REASONIX_RLM_*_PROVIDER must be anthropic, codex, gemini, or openai")
	}
}

func isCodexSubscriptionModel(model string) bool {
	switch strings.ToLower(strings.TrimSpace(model)) {
	case "gpt-5.3-codex-spark", "gpt-5.4", "gpt-5.4-mini", "gpt-5.5", "gpt-5.6-luna", "gpt-5.6-sol", "gpt-5.6-terra":
		return true
	default:
		return false
	}
}

func useTrustedInProcess(containerAvailable bool) bool {
	return envBool("REASONIX_RLM_TRUSTED_IN_PROCESS") || !containerAvailable
}

func envString(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func envBool(name string) bool {
	value, err := strconv.ParseBool(strings.TrimSpace(os.Getenv(name)))
	return err == nil && value
}
