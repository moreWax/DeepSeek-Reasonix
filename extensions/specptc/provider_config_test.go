package main

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/XiaoConstantine/rlm-go/pkg/providers"
	"github.com/esengine/DeepSeek-Reasonix/extensions/specptc/internal/codex"
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

func TestCodexSubscriptionModelDetection(t *testing.T) {
	t.Setenv("REASONIX_HOME", t.TempDir())
	t.Setenv("REASONIX_CODEX_CLI_AUTH_FILE", filepath.Join(t.TempDir(), "missing"))
	kind, err := resolveRLMProvider(codex.DefaultModel, "")
	if err != nil || kind != providers.OpenAI {
		t.Fatalf("API-key fallback kind=%q err=%v", kind, err)
	}
	kind, err = resolveRLMProvider("private-model", "codex")
	if err != nil || kind != codexProvider {
		t.Fatalf("explicit kind=%q err=%v", kind, err)
	}
}

func TestConfiguredRLMSubModelRequiresModelAcrossProviders(t *testing.T) {
	t.Setenv("REASONIX_RLM_ROOT_PROVIDER", "codex")
	t.Setenv("REASONIX_RLM_SUB_PROVIDER", "openai")
	t.Setenv("REASONIX_RLM_SUB_MODEL", "")
	if _, err := configuredRLMSubModel(codex.DefaultModel); err == nil {
		t.Fatal("cross-provider inherited model was accepted")
	}
	t.Setenv("REASONIX_RLM_SUB_MODEL", "gpt-5-mini")
	if model, err := configuredRLMSubModel(codex.DefaultModel); err != nil || model != "gpt-5-mini" {
		t.Fatalf("explicit sub-model = %q, %v", model, err)
	}
}

func TestConfiguredRLMSubProviderInheritsExplicitRootProvider(t *testing.T) {
	t.Setenv("REASONIX_RLM_SUB_PROVIDER", "")
	if provider := configuredRLMSubProvider("openai"); provider != "openai" {
		t.Fatalf("inherited sub-provider = %q", provider)
	}
	t.Setenv("REASONIX_RLM_SUB_PROVIDER", "gemini")
	if provider := configuredRLMSubProvider("openai"); provider != "gemini" {
		t.Fatalf("explicit sub-provider = %q", provider)
	}
}

func TestDefaultRLMRootModelUsesExistingCodexSubscription(t *testing.T) {
	home := t.TempDir()
	t.Setenv("REASONIX_HOME", home)
	t.Setenv("REASONIX_CODEX_CLI_AUTH_FILE", filepath.Join(t.TempDir(), "missing"))
	t.Setenv("REASONIX_RLM_ROOT_PROVIDER", "")
	credential := map[string]any{
		"access_token": "access", "refresh_token": "refresh",
		"expires_at": time.Now().Add(time.Hour), "account_id": "account",
	}
	data, err := json.Marshal(credential)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, "codex-auth.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if model := defaultRLMRootModel(); model != codex.DefaultModel {
		t.Fatalf("default model = %q", model)
	}
	if kind, err := resolveRLMProvider(codex.DefaultModel, ""); err != nil || kind != codexProvider {
		t.Fatalf("subscription kind = %q, %v", kind, err)
	}
}

func TestDefaultRLMRootModelFallsBackWithoutCodexSubscription(t *testing.T) {
	t.Setenv("REASONIX_HOME", t.TempDir())
	t.Setenv("REASONIX_CODEX_CLI_AUTH_FILE", filepath.Join(t.TempDir(), "missing"))
	t.Setenv("REASONIX_RLM_ROOT_PROVIDER", "")
	if model := defaultRLMRootModel(); model != "gpt-5-mini" {
		t.Fatalf("default model = %q", model)
	}
}

func TestLocalExecutionAutomaticallyFollowsContainerAvailability(t *testing.T) {
	t.Setenv("REASONIX_RLM_TRUSTED_IN_PROCESS", "")
	if !useTrustedInProcess(false) {
		t.Fatal("missing container runtime did not enable local execution")
	}
	if useTrustedInProcess(true) {
		t.Fatal("available container runtime unexpectedly enabled local execution")
	}
	t.Setenv("REASONIX_RLM_TRUSTED_IN_PROCESS", "true")
	if !useTrustedInProcess(true) {
		t.Fatal("explicit local execution override was ignored")
	}
}
