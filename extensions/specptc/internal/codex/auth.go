// Package codex implements the extension-local ChatGPT Codex subscription transport.
package codex

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	clientID           = "app_EMoamEEZ73f0CkXaXp7hrann"
	defaultAuthBaseURL = "https://auth.openai.com"
	jwtClaimPath       = "https://api.openai.com/auth"
	maxCredentialBytes = 1 << 20
)

// Credential is the shared Reasonix/Codex OAuth credential representation.
type Credential struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token"`
	ExpiresAt    time.Time `json:"expires_at"`
	AccountID    string    `json:"account_id"`
}

func (c Credential) valid() bool {
	return strings.TrimSpace(c.AccessToken) != "" && strings.TrimSpace(c.RefreshToken) != "" && strings.TrimSpace(c.AccountID) != ""
}

// AuthConfig allows tests to substitute paths, time, and OAuth transport.
type AuthConfig struct {
	HTTPClient    *http.Client
	AuthBaseURL   string
	ReasonixPath  string
	CodexPath     string
	Now           func() time.Time
	RefreshBefore time.Duration
}

// Manager loads the existing Reasonix credential, imports Codex CLI credentials
// when necessary, and serializes token refreshes within this extension process.
type Manager struct {
	config AuthConfig
	once   sync.Once
	gate   chan struct{}
}

func NewManager(config AuthConfig) *Manager {
	if config.HTTPClient == nil {
		config.HTTPClient = http.DefaultClient
	}
	if strings.TrimSpace(config.AuthBaseURL) == "" {
		config.AuthBaseURL = defaultAuthBaseURL
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.RefreshBefore == 0 {
		config.RefreshBefore = time.Minute
	}
	return &Manager{config: config}
}

func (m *Manager) lock(ctx context.Context) error {
	m.once.Do(func() {
		m.gate = make(chan struct{}, 1)
		m.gate <- struct{}{}
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.gate:
		return nil
	}
}

func (m *Manager) unlock() {
	m.gate <- struct{}{}
}

// Available reports whether an existing Reasonix or Codex CLI OAuth credential
// is structurally usable. It does not refresh, import, or write credentials.
func Available(config AuthConfig) bool {
	manager := NewManager(config)
	credential, err := manager.loadReasonix()
	if err == nil {
		return credential.valid()
	}
	if !os.IsNotExist(err) {
		return false
	}
	credential, err = manager.loadCodexCLI()
	return err == nil && credential.valid()
}

func defaultReasonixPath() (string, error) {
	if path := strings.TrimSpace(os.Getenv("REASONIX_CODEX_AUTH_FILE")); path != "" {
		return filepath.Clean(path), nil
	}
	if home := strings.TrimSpace(os.Getenv("REASONIX_HOME")); home != "" {
		return filepath.Join(filepath.Clean(home), "codex-auth.json"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return "", errors.New("codex: cannot resolve Reasonix credential path")
	}
	configDirectory := ""
	if runtime.GOOS == "windows" {
		configDirectory, _ = os.UserConfigDir()
	}
	return reasonixCredentialPath(runtime.GOOS, home, configDirectory), nil
}

func reasonixCredentialPath(goos, home, configDirectory string) string {
	if goos == "windows" && strings.TrimSpace(configDirectory) != "" {
		return filepath.Join(configDirectory, "reasonix", "codex-auth.json")
	}
	return filepath.Join(home, ".reasonix", "codex-auth.json")
}

func defaultCodexPath() (string, error) {
	if path := strings.TrimSpace(os.Getenv("REASONIX_CODEX_CLI_AUTH_FILE")); path != "" {
		return filepath.Clean(path), nil
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return "", errors.New("codex: cannot resolve Codex CLI credential path")
	}
	return filepath.Join(home, ".codex", "auth.json"), nil
}

func (m *Manager) reasonixPath() (string, error) {
	if path := strings.TrimSpace(m.config.ReasonixPath); path != "" {
		return filepath.Clean(path), nil
	}
	return defaultReasonixPath()
}

func (m *Manager) codexPath() (string, error) {
	if path := strings.TrimSpace(m.config.CodexPath); path != "" {
		return filepath.Clean(path), nil
	}
	return defaultCodexPath()
}

func readJSONFile(path string, dst any) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, maxCredentialBytes+1))
	if err := decoder.Decode(dst); err != nil {
		return errors.New("codex: invalid credential file")
	}
	if decoder.Decode(new(any)) != io.EOF {
		return errors.New("codex: invalid credential file")
	}
	return nil
}

func (m *Manager) loadReasonix() (Credential, error) {
	path, err := m.reasonixPath()
	if err != nil {
		return Credential{}, err
	}
	var credential Credential
	if err := readJSONFile(path, &credential); err != nil {
		return Credential{}, err
	}
	if !credential.valid() {
		return Credential{}, errors.New("codex: Reasonix credential is incomplete")
	}
	return credential, nil
}

func (m *Manager) loadCodexCLI() (Credential, error) {
	path, err := m.codexPath()
	if err != nil {
		return Credential{}, err
	}
	var document struct {
		Tokens struct {
			AccessToken  string `json:"access_token"`
			RefreshToken string `json:"refresh_token"`
			AccountID    string `json:"account_id"`
		} `json:"tokens"`
	}
	if err := readJSONFile(path, &document); err != nil {
		return Credential{}, err
	}
	credential := Credential{
		AccessToken:  document.Tokens.AccessToken,
		RefreshToken: document.Tokens.RefreshToken,
		AccountID:    document.Tokens.AccountID,
		ExpiresAt:    jwtExpiry(document.Tokens.AccessToken),
	}
	if credential.AccountID == "" {
		credential.AccountID, _ = accountIDFromJWT(credential.AccessToken)
	}
	if !credential.valid() {
		return Credential{}, errors.New("codex: Codex CLI credential is incomplete")
	}
	return credential, nil
}

func accountIDFromJWT(token string) (string, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", errors.New("codex: malformed access token")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", errors.New("codex: malformed access token")
	}
	var claims map[string]json.RawMessage
	if json.Unmarshal(payload, &claims) != nil {
		return "", errors.New("codex: malformed access token")
	}
	var auth struct {
		AccountID string `json:"chatgpt_account_id"`
	}
	if json.Unmarshal(claims[jwtClaimPath], &auth) != nil || strings.TrimSpace(auth.AccountID) == "" {
		return "", errors.New("codex: account ID is absent from access token")
	}
	return strings.TrimSpace(auth.AccountID), nil
}

func jwtExpiry(token string) time.Time {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}
	}
	var claims struct {
		Expires int64 `json:"exp"`
	}
	if json.Unmarshal(payload, &claims) != nil || claims.Expires <= 0 {
		return time.Time{}
	}
	return time.Unix(claims.Expires, 0)
}

func (m *Manager) save(credential Credential) error {
	if !credential.valid() {
		return errors.New("codex: refusing to save incomplete credential")
	}
	path, err := m.reasonixPath()
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(credential, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("codex: create credential directory: %w", err)
	}
	_ = os.Chmod(directory, 0o700)
	file, err := os.CreateTemp(directory, "."+filepath.Base(path)+"-*")
	if err != nil {
		return fmt.Errorf("codex: create credential file: %w", err)
	}
	temporary := file.Name()
	published := false
	defer func() {
		if !published {
			_ = os.Remove(temporary)
		}
	}()
	if err = file.Chmod(0o600); err == nil {
		_, err = file.Write(data)
	}
	if err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("codex: write credential: %w", err)
	}
	if err = os.Rename(temporary, path); err != nil {
		return fmt.Errorf("codex: publish credential: %w", err)
	}
	_ = os.Chmod(path, 0o600)
	published = true
	return nil
}

// Credentials returns a current OAuth credential, refreshing and privately
// persisting it when its access token is near expiry.
func (m *Manager) Credentials(ctx context.Context) (Credential, error) {
	if ctx == nil {
		return Credential{}, errors.New("codex: nil context")
	}
	if err := m.lock(ctx); err != nil {
		return Credential{}, err
	}
	defer m.unlock()
	credential, err := m.loadReasonix()
	if os.IsNotExist(err) {
		credential, err = m.loadCodexCLI()
		if err == nil {
			err = m.save(credential)
		}
	}
	if err != nil {
		return Credential{}, err
	}
	before := m.config.RefreshBefore
	if before < 0 {
		before = 0
	}
	if !credential.ExpiresAt.IsZero() && m.config.Now().Add(before).Before(credential.ExpiresAt) {
		return credential, nil
	}
	credential, err = m.refresh(ctx, credential)
	if err != nil {
		return Credential{}, err
	}
	if err := m.save(credential); err != nil {
		return Credential{}, err
	}
	return credential, nil
}

func (m *Manager) refresh(ctx context.Context, previous Credential) (Credential, error) {
	values := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {previous.RefreshToken},
		"client_id":     {clientID},
	}
	endpoint := strings.TrimRight(strings.TrimSpace(m.config.AuthBaseURL), "/") + "/oauth/token"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(values.Encode()))
	if err != nil {
		return Credential{}, errors.New("codex: create refresh request")
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := m.config.HTTPClient.Do(request)
	if err != nil {
		return Credential{}, fmt.Errorf("codex: refresh request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
		return Credential{}, fmt.Errorf("codex: refresh failed with status %d", response.StatusCode)
	}
	var wire struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int64  `json:"expires_in"`
	}
	if json.NewDecoder(io.LimitReader(response.Body, maxCredentialBytes)).Decode(&wire) != nil || strings.TrimSpace(wire.AccessToken) == "" || wire.ExpiresIn <= 0 {
		return Credential{}, errors.New("codex: invalid refresh response")
	}
	if wire.RefreshToken == "" {
		wire.RefreshToken = previous.RefreshToken
	}
	accountID, err := accountIDFromJWT(wire.AccessToken)
	if err != nil {
		return Credential{}, err
	}
	return Credential{
		AccessToken:  wire.AccessToken,
		RefreshToken: wire.RefreshToken,
		ExpiresAt:    m.config.Now().Add(time.Duration(wire.ExpiresIn) * time.Second),
		AccountID:    accountID,
	}, nil
}
