package codex

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/XiaoConstantine/rlm-go/pkg/core"
	"github.com/XiaoConstantine/rlm-go/pkg/providers"
	"github.com/XiaoConstantine/rlm-go/pkg/rlm"
)

const (
	DefaultBaseURL        = "https://chatgpt.com/backend-api"
	DefaultModel          = "gpt-5.6-sol"
	maxStreamEventBytes   = 4 << 20
	maxCompletionBytes    = 64 << 20
	maxStreamEvents       = 65_536
	maxTrackedOutputParts = 1_024
)

// Config configures the extension-local Codex Responses client.
type Config struct {
	HTTPClient *http.Client
	Manager    *Manager
	BaseURL    string
	Model      string
	Effort     string
	Verbosity  string
}

// Client implements the rlm-go root and sub-model client interfaces.
type Client struct {
	httpClient *http.Client
	manager    *Manager
	endpoint   string
	model      string
	effort     string
	verbosity  string
}

func NewClient(config Config) *Client {
	if config.HTTPClient == nil {
		config.HTTPClient = http.DefaultClient
	}
	if config.Manager == nil {
		config.Manager = NewManager(AuthConfig{HTTPClient: config.HTTPClient})
	}
	baseURL := strings.TrimRight(strings.TrimSpace(config.BaseURL), "/")
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	endpoint := baseURL + "/codex/responses"
	if strings.HasSuffix(baseURL, "/codex") {
		endpoint = baseURL + "/responses"
	} else if strings.HasSuffix(baseURL, "/codex/responses") {
		endpoint = baseURL
	}
	model := strings.TrimSpace(config.Model)
	if model == "" {
		model = DefaultModel
	}
	effort := strings.TrimSpace(config.Effort)
	if effort == "" {
		effort = "high"
	}
	verbosity := strings.TrimSpace(config.Verbosity)
	if verbosity == "" {
		verbosity = "low"
	}
	return &Client{
		httpClient: config.HTTPClient,
		manager:    config.Manager,
		endpoint:   endpoint,
		model:      model,
		effort:     effort,
		verbosity:  verbosity,
	}
}

func (c *Client) Complete(ctx context.Context, messages []core.Message) (core.LLMResponse, error) {
	return c.complete(ctx, messages, nil)
}

func (c *Client) CompleteStream(ctx context.Context, messages []core.Message, handler rlm.StreamHandler) (core.LLMResponse, error) {
	return c.complete(ctx, messages, handler)
}

func (c *Client) Query(ctx context.Context, prompt string) (core.QueryResponse, error) {
	response, err := c.complete(ctx, []core.Message{{Role: "user", Content: prompt}}, nil)
	return core.QueryResponse{
		Response:         response.Content,
		PromptTokens:     response.PromptTokens,
		CompletionTokens: response.CompletionTokens,
	}, err
}

func (c *Client) QueryBatched(ctx context.Context, prompts []string) ([]core.QueryResponse, error) {
	return providers.QueryBatchedConcurrent(ctx, prompts, c.Query)
}

type requestBody struct {
	Model             string           `json:"model"`
	Instructions      string           `json:"instructions"`
	Input             []map[string]any `json:"input"`
	Store             bool             `json:"store"`
	Stream            bool             `json:"stream"`
	Text              map[string]any   `json:"text"`
	Include           []string         `json:"include"`
	ToolChoice        string           `json:"tool_choice"`
	ParallelToolCalls bool             `json:"parallel_tool_calls"`
	Reasoning         map[string]any   `json:"reasoning,omitempty"`
}

func (c *Client) buildRequest(messages []core.Message) requestBody {
	instructions := "You are a helpful assistant."
	start := 0
	if len(messages) > 0 && strings.EqualFold(messages[0].Role, "system") {
		if value := strings.TrimSpace(messages[0].Content); value != "" {
			instructions = value
		}
		start = 1
	}
	input := make([]map[string]any, 0, len(messages)-start)
	for _, message := range messages[start:] {
		role := strings.ToLower(strings.TrimSpace(message.Role))
		switch role {
		case "assistant", "developer", "system", "user":
		default:
			role = "user"
		}
		input = append(input, map[string]any{"role": role, "content": message.Content})
	}
	return requestBody{
		Model:             c.model,
		Instructions:      instructions,
		Input:             input,
		Store:             false,
		Stream:            true,
		Text:              map[string]any{"verbosity": c.verbosity},
		Include:           []string{"reasoning.encrypted_content"},
		ToolChoice:        "auto",
		ParallelToolCalls: true,
		Reasoning:         map[string]any{"effort": c.effort, "summary": "auto"},
	}
}

func (c *Client) complete(ctx context.Context, messages []core.Message, handler rlm.StreamHandler) (core.LLMResponse, error) {
	credential, err := c.manager.Credentials(ctx)
	if err != nil {
		return core.LLMResponse{}, fmt.Errorf("codex subscription credentials unavailable: %w", err)
	}
	payload, err := json.Marshal(c.buildRequest(messages))
	if err != nil {
		return core.LLMResponse{}, fmt.Errorf("codex: encode request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(payload))
	if err != nil {
		return core.LLMResponse{}, fmt.Errorf("codex: create request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+credential.AccessToken)
	request.Header.Set("chatgpt-account-id", credential.AccountID)
	request.Header.Set("originator", "pi")
	request.Header.Set("OpenAI-Beta", "responses=experimental")
	request.Header.Set("Accept", "text/event-stream")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "reasonix")
	response, err := c.httpClient.Do(request)
	if err != nil {
		return core.LLMResponse{}, fmt.Errorf("codex: request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
		return core.LLMResponse{}, fmt.Errorf("codex: request failed with status %d", response.StatusCode)
	}
	return readStream(ctx, response.Body, handler)
}

type streamEvent struct {
	Type         string          `json:"type"`
	Delta        string          `json:"delta"`
	Text         string          `json:"text"`
	ItemID       string          `json:"item_id"`
	ContentIndex int             `json:"content_index"`
	Response     *streamResponse `json:"response"`
}

type streamResponse struct {
	Usage *streamUsage `json:"usage"`
}

type streamUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

func readStream(ctx context.Context, reader io.Reader, handler rlm.StreamHandler) (core.LLMResponse, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 64<<10), maxStreamEventBytes)
	var content strings.Builder
	var promptTokens, completionTokens int
	hadDelta := make(map[string]bool)
	eventCount := 0
	terminal := false
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return core.LLMResponse{}, err
		}
		line := scanner.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "" || data == "[DONE]" {
			continue
		}
		eventCount++
		if eventCount > maxStreamEvents {
			return core.LLMResponse{}, errors.New("codex: stream event limit exceeded")
		}
		var event streamEvent
		if err := json.Unmarshal([]byte(data), &event); err != nil {
			return core.LLMResponse{}, errors.New("codex: malformed stream event")
		}
		key := fmt.Sprintf("%s:%d", event.ItemID, event.ContentIndex)
		switch event.Type {
		case "response.output_text.delta":
			if event.Delta != "" && !hadDelta[key] {
				if len(hadDelta) >= maxTrackedOutputParts {
					return core.LLMResponse{}, errors.New("codex: output part limit exceeded")
				}
				hadDelta[key] = true
			}
			if err := appendOutput(&content, event.Delta, handler); err != nil {
				return core.LLMResponse{}, err
			}
		case "response.output_text.done":
			partHadDelta := hadDelta[key]
			delete(hadDelta, key)
			if !partHadDelta {
				if err := appendOutput(&content, event.Text, handler); err != nil {
					return core.LLMResponse{}, err
				}
			}
		case "response.completed", "response.done":
			terminal = true
			if event.Response != nil && event.Response.Usage != nil {
				promptTokens = event.Response.Usage.InputTokens
				completionTokens = event.Response.Usage.OutputTokens
			}
		case "response.incomplete":
			return core.LLMResponse{}, errors.New("codex: response incomplete")
		case "response.failed":
			return core.LLMResponse{}, errors.New("codex: response failed")
		}
		if terminal {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		if ctx.Err() != nil {
			return core.LLMResponse{}, ctx.Err()
		}
		return core.LLMResponse{}, fmt.Errorf("codex: read stream: %w", err)
	}
	if !terminal {
		return core.LLMResponse{}, io.ErrUnexpectedEOF
	}
	if handler != nil {
		if err := handler("", true); err != nil {
			return core.LLMResponse{}, fmt.Errorf("codex: stream handler: %w", err)
		}
	}
	return core.LLMResponse{
		Content:          content.String(),
		PromptTokens:     promptTokens,
		CompletionTokens: completionTokens,
	}, nil
}

func appendOutput(content *strings.Builder, text string, handler rlm.StreamHandler) error {
	if text == "" {
		return nil
	}
	if content.Len()+len(text) > maxCompletionBytes {
		return errors.New("codex: completion exceeds byte limit")
	}
	content.WriteString(text)
	if handler != nil {
		if err := handler(text, false); err != nil {
			return fmt.Errorf("codex: stream handler: %w", err)
		}
	}
	return nil
}

var _ providers.Client = (*Client)(nil)
var _ rlm.StreamingLLMClient = (*Client)(nil)
