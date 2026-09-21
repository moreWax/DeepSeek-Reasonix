package main

import (
	"io"
	"log"
	"testing"

	"github.com/XiaoConstantine/rlm-go/pkg/providers"
	"github.com/esengine/DeepSeek-Reasonix/extensions/specptc/internal/engine"
)

func TestResolveRLMProviderExplicitAndDetected(t *testing.T) {
	kind, err := resolveRLMProvider("gpt-5-mini", "")
	if err != nil || kind != providers.OpenAI {
		t.Fatalf("detected kind=%q err=%v", kind, err)
	}
	kind, err = resolveRLMProvider("private-model", "gemini")
	if err != nil || kind != providers.Gemini {
		t.Fatalf("explicit kind=%q err=%v", kind, err)
	}
	if _, err := resolveRLMProvider("model", "unknown"); err == nil {
		t.Fatal("unknown explicit provider was accepted")
	}
}

func TestNewRLMProviderFailsClosedWithoutCredentials(t *testing.T) {
	t.Setenv("REASONIX_RLM_ROOT_MODEL", "gpt-5-mini")
	t.Setenv("REASONIX_RLM_ROOT_PROVIDER", "openai")
	t.Setenv("OPENAI_API_KEY", "")
	provider := newRLMProvider(log.New(io.Discard, "", 0), engine.Config{})
	if provider.runtime != nil {
		t.Fatal("provider runtime initialized without credentials")
	}
	catalog, err := provider.Catalog(t.Context())
	if err != nil || len(catalog) != 0 {
		t.Fatalf("unavailable catalog=%+v err=%v", catalog, err)
	}
}
