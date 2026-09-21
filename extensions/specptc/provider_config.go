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

	"github.com/esengine/DeepSeek-Reasonix/extensions/specptc/internal/engine"
	"github.com/esengine/DeepSeek-Reasonix/extensions/specptc/internal/ptc"
)

func newRLMProvider(logger *log.Logger, scheduler engine.Config) *specPTCProvider {
	rootModel := envString("REASONIX_RLM_ROOT_MODEL", "gpt-5-mini")
	subModel := envString("REASONIX_RLM_SUB_MODEL", rootModel)
	provider := &specPTCProvider{model: rootModel, log: logger}
	root, err := newRLMModelClient(rootModel, os.Getenv("REASONIX_RLM_ROOT_PROVIDER"))
	if err != nil {
		logger.Printf("RLM provider unavailable: %v", err)
		return provider
	}
	sub, err := newRLMModelClient(subModel, os.Getenv("REASONIX_RLM_SUB_PROVIDER"))
	if err != nil {
		logger.Printf("RLM provider unavailable: %v", err)
		return provider
	}
	trusted := envBool("REASONIX_RLM_TRUSTED_IN_PROCESS")
	if trusted {
		logger.Printf("warning: trusted in-process RLM execution enabled; do not use for untrusted model output")
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

func newRLMModelClient(model, explicitProvider string) (providers.Client, error) {
	kind, err := resolveRLMProvider(model, explicitProvider)
	if err != nil {
		return nil, err
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
		return providers.GetProvider(model), nil
	}
	switch strings.ToLower(strings.TrimSpace(explicit)) {
	case string(providers.OpenAI):
		return providers.OpenAI, nil
	case string(providers.Gemini):
		return providers.Gemini, nil
	case string(providers.Anthropic):
		return providers.Anthropic, nil
	default:
		return "", errors.New("REASONIX_RLM_*_PROVIDER must be anthropic, gemini, or openai")
	}
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
