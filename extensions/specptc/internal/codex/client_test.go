package codex

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/XiaoConstantine/rlm-go/pkg/core"
)

func TestClientStreamsCodexResponseWithSubscriptionHeaders(t *testing.T) {
	manager := testManager(t)
	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/codex/responses" {
			t.Errorf("path = %q", request.URL.Path)
		}
		if request.Header.Get("Authorization") != "Bearer access-token" || request.Header.Get("chatgpt-account-id") != "account-id" {
			t.Errorf("authentication headers = %q, %q", request.Header.Get("Authorization"), request.Header.Get("chatgpt-account-id"))
		}
		if request.Header.Get("originator") != "pi" || request.Header.Get("OpenAI-Beta") != "responses=experimental" || request.Header.Get("User-Agent") != "reasonix" {
			t.Errorf("Codex headers = %v", request.Header)
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"private\"}\n\n")
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello \"}\n\n")
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"world\"}\n\n")
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":12,\"output_tokens\":7,\"total_tokens\":19}}}\n\n")
	}))
	defer server.Close()
	client := NewClient(Config{HTTPClient: server.Client(), Manager: manager, BaseURL: server.URL, Model: "gpt-test"})
	var chunks []string
	response, err := client.CompleteStream(t.Context(), []core.Message{
		{Role: "system", Content: "system instructions"},
		{Role: "user", Content: "question"},
	}, func(chunk string, done bool) error {
		if done {
			chunks = append(chunks, "<done>")
		} else {
			chunks = append(chunks, chunk)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Content != "hello world" || response.PromptTokens != 12 || response.CompletionTokens != 7 {
		t.Fatalf("response = %+v", response)
	}
	if !reflect.DeepEqual(chunks, []string{"hello ", "world", "<done>"}) {
		t.Fatalf("chunks = %#v", chunks)
	}
	if body["model"] != "gpt-test" || body["instructions"] != "system instructions" || body["store"] != false || body["stream"] != true {
		t.Fatalf("request body = %#v", body)
	}
	if body["max_output_tokens"] != nil {
		t.Fatalf("unsupported max_output_tokens sent: %#v", body["max_output_tokens"])
	}
	reasoning, _ := body["reasoning"].(map[string]any)
	if reasoning["effort"] != "high" || reasoning["summary"] != "auto" {
		t.Fatalf("reasoning = %#v", reasoning)
	}
}

func TestClientQueryUsesOutputDoneFallback(t *testing.T) {
	manager := testManager(t)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.output_text.done\",\"text\":\"fallback\"}\n\n")
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.done\",\"response\":{\"usage\":{\"input_tokens\":2,\"output_tokens\":1}}}\n\n")
	}))
	defer server.Close()
	client := NewClient(Config{HTTPClient: server.Client(), Manager: manager, BaseURL: server.URL})
	response, err := client.Query(t.Context(), "question")
	if err != nil {
		t.Fatal(err)
	}
	if response.Response != "fallback" || response.PromptTokens != 2 || response.CompletionTokens != 1 {
		t.Fatalf("response = %+v", response)
	}
}

func TestClientPreservesDoneFallbackForSeparateOutputItem(t *testing.T) {
	manager := testManager(t)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"first\",\"content_index\":0,\"delta\":\"first\"}\n\n")
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.output_text.done\",\"item_id\":\"second\",\"content_index\":0,\"text\":\" second\"}\n\n")
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.completed\"}\n\n")
	}))
	defer server.Close()
	client := NewClient(Config{HTTPClient: server.Client(), Manager: manager, BaseURL: server.URL})
	response, err := client.Query(t.Context(), "question")
	if err != nil {
		t.Fatal(err)
	}
	if response.Response != "first second" {
		t.Fatalf("response = %q", response.Response)
	}
}

func TestReadStreamBoundsTrackedOutputParts(t *testing.T) {
	var stream strings.Builder
	for i := 0; i <= maxTrackedOutputParts; i++ {
		fmt.Fprintf(&stream, "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"item-%d\",\"delta\":\"x\"}\n\n", i)
	}
	_, err := readStream(t.Context(), strings.NewReader(stream.String()), nil)
	if err == nil || !strings.Contains(err.Error(), "output part limit") {
		t.Fatalf("error = %v", err)
	}
}

func TestClientRejectsPrematureAndFailedStreams(t *testing.T) {
	for name, stream := range map[string]string{
		"premature": "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n",
		"malformed": "data: {not-json}\n\ndata: {\"type\":\"response.completed\"}\n\n",
		"failed":    "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":\"secret body\"}}}\n\n",
	} {
		t.Run(name, func(t *testing.T) {
			manager := testManager(t)
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(writer, stream)
			}))
			defer server.Close()
			client := NewClient(Config{HTTPClient: server.Client(), Manager: manager, BaseURL: server.URL})
			_, err := client.Query(t.Context(), "question")
			if err == nil {
				t.Fatal("invalid stream was accepted")
			}
			if strings.Contains(err.Error(), "secret body") {
				t.Fatalf("server body leaked in error: %v", err)
			}
		})
	}
}

func TestClientPropagatesStreamHandlerError(t *testing.T) {
	manager := testManager(t)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"text\"}\n\n")
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.completed\"}\n\n")
	}))
	defer server.Close()
	client := NewClient(Config{HTTPClient: server.Client(), Manager: manager, BaseURL: server.URL})
	want := errors.New("stop")
	_, err := client.CompleteStream(t.Context(), []core.Message{{Role: "user", Content: "question"}}, func(string, bool) error { return want })
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}

func testManager(t *testing.T) *Manager {
	t.Helper()
	path := filepath.Join(t.TempDir(), "codex-auth.json")
	writeJSON(t, path, Credential{
		AccessToken: "access-token", RefreshToken: "refresh-token",
		ExpiresAt: time.Now().Add(time.Hour), AccountID: "account-id",
	})
	return NewManager(AuthConfig{ReasonixPath: path, CodexPath: filepath.Join(t.TempDir(), "missing")})
}
