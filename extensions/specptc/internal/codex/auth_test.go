package codex

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func testJWT(t *testing.T, accountID string, expires time.Time) string {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"exp":        expires.Unix(),
		jwtClaimPath: map[string]string{"chatgpt_account_id": accountID},
	})
	if err != nil {
		t.Fatal(err)
	}
	return "x." + base64.RawURLEncoding.EncodeToString(payload) + ".x"
}

func TestAvailableFindsReasonixAndCodexCredentials(t *testing.T) {
	directory := t.TempDir()
	reasonixPath := filepath.Join(directory, "reasonix.json")
	codexPath := filepath.Join(directory, "codex.json")
	config := AuthConfig{ReasonixPath: reasonixPath, CodexPath: codexPath}
	if Available(config) {
		t.Fatal("credentials unexpectedly available")
	}
	access := testJWT(t, "account", time.Now().Add(time.Hour))
	writeJSON(t, codexPath, map[string]any{"tokens": map[string]string{
		"access_token": access, "refresh_token": "refresh",
	}})
	if !Available(config) {
		t.Fatal("Codex CLI credential was not detected")
	}
	if err := os.Remove(codexPath); err != nil {
		t.Fatal(err)
	}
	writeJSON(t, reasonixPath, Credential{
		AccessToken: access, RefreshToken: "refresh", ExpiresAt: time.Now().Add(time.Hour), AccountID: "account",
	})
	if !Available(config) {
		t.Fatal("Reasonix credential was not detected")
	}
}

func TestAvailableDoesNotMaskMalformedReasonixCredential(t *testing.T) {
	directory := t.TempDir()
	reasonixPath := filepath.Join(directory, "reasonix.json")
	codexPath := filepath.Join(directory, "codex.json")
	if err := os.WriteFile(reasonixPath, []byte("{not-json}"), 0o600); err != nil {
		t.Fatal(err)
	}
	access := testJWT(t, "account", time.Now().Add(time.Hour))
	writeJSON(t, codexPath, map[string]any{"tokens": map[string]string{
		"access_token": access, "refresh_token": "refresh",
	}})
	if Available(AuthConfig{ReasonixPath: reasonixPath, CodexPath: codexPath}) {
		t.Fatal("availability masked malformed Reasonix credential")
	}
}

func TestManagerImportsCodexCredentialPrivately(t *testing.T) {
	directory := t.TempDir()
	reasonixPath := filepath.Join(directory, "reasonix", "codex-auth.json")
	codexPath := filepath.Join(directory, "codex", "auth.json")
	access := testJWT(t, "account-import", time.Now().Add(time.Hour))
	writeJSON(t, codexPath, map[string]any{"tokens": map[string]string{
		"access_token": access, "refresh_token": "refresh-import",
	}})
	manager := NewManager(AuthConfig{ReasonixPath: reasonixPath, CodexPath: codexPath})
	credential, err := manager.Credentials(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if credential.AccountID != "account-import" {
		t.Fatalf("account ID = %q", credential.AccountID)
	}
	info, err := os.Stat(reasonixPath)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
		t.Fatalf("credential mode = %o, want 600", info.Mode().Perm())
	}
}

func TestManagerRefreshesExpiredCredential(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	path := filepath.Join(t.TempDir(), "codex-auth.json")
	writeJSON(t, path, Credential{
		AccessToken:  testJWT(t, "old-account", now.Add(-time.Hour)),
		RefreshToken: "old-refresh",
		ExpiresAt:    now.Add(-time.Hour),
		AccountID:    "old-account",
	})
	newAccess := testJWT(t, "new-account", now.Add(time.Hour))
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		if err := request.ParseForm(); err != nil {
			t.Error(err)
		}
		if request.Form.Get("grant_type") != "refresh_token" || request.Form.Get("refresh_token") != "old-refresh" || request.Form.Get("client_id") != clientID {
			t.Errorf("refresh form = %v", request.Form)
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"access_token": newAccess, "refresh_token": "new-refresh", "expires_in": 3600,
		})
	}))
	defer server.Close()
	manager := NewManager(AuthConfig{
		HTTPClient: server.Client(), AuthBaseURL: server.URL, ReasonixPath: path,
		CodexPath: filepath.Join(t.TempDir(), "missing"), Now: func() time.Time { return now },
	})
	credential, err := manager.Credentials(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if credential.AccountID != "new-account" || credential.RefreshToken != "new-refresh" || requests.Load() != 1 {
		t.Fatalf("refreshed credential = %+v, requests = %d", credential, requests.Load())
	}
	stored, err := manager.loadReasonix()
	if err != nil {
		t.Fatal(err)
	}
	if stored.AccessToken != newAccess || stored.RefreshToken != "new-refresh" {
		t.Fatal("refreshed credential was not persisted")
	}
}

func TestManagerCredentialGateHonorsCancellation(t *testing.T) {
	manager := NewManager(AuthConfig{})
	if err := manager.lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := manager.lock(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("lock error = %v", err)
	}
	manager.unlock()
}

func TestDefaultPathsHonorReasonixEnvironment(t *testing.T) {
	home := t.TempDir()
	customCLI := filepath.Join(t.TempDir(), "auth.json")
	t.Setenv("REASONIX_HOME", home)
	t.Setenv("REASONIX_CODEX_AUTH_FILE", "")
	t.Setenv("REASONIX_CODEX_CLI_AUTH_FILE", customCLI)
	reasonixPath, err := defaultReasonixPath()
	if err != nil {
		t.Fatal(err)
	}
	codexPath, err := defaultCodexPath()
	if err != nil {
		t.Fatal(err)
	}
	if reasonixPath != filepath.Join(home, "codex-auth.json") || codexPath != customCLI {
		t.Fatalf("paths = %q, %q", reasonixPath, codexPath)
	}
}

func TestReasonixCredentialPathMatchesPlatformConvention(t *testing.T) {
	if got := reasonixCredentialPath("darwin", "/home/user", "/config"); got != filepath.Join("/home/user", ".reasonix", "codex-auth.json") {
		t.Fatalf("Unix path = %q", got)
	}
	windowsHome := `C:\Users\user`
	windowsConfig := `C:\Users\user\AppData\Roaming`
	if got := reasonixCredentialPath("windows", windowsHome, windowsConfig); got != filepath.Join(windowsConfig, "reasonix", "codex-auth.json") {
		t.Fatalf("Windows path = %q", got)
	}
}

func writeJSON(t *testing.T, path string, value any) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}
