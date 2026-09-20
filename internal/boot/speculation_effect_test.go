package boot

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"reasonix/internal/config"
	"reasonix/internal/event"
	"reasonix/internal/provider"
)

type speculationEffectProvider struct {
	logPath  string
	target   string
	expected string
	mutate   bool
	round    atomic.Int32
	early    atomic.Bool
	observed atomic.Bool
}

func (*speculationEffectProvider) Name() string { return "boot-speculation-effect" }

func (p *speculationEffectProvider) Stream(ctx context.Context, req provider.Request) (<-chan provider.Chunk, error) {
	chunks := make(chan provider.Chunk, 2)
	if p.round.Add(1) > 1 {
		for _, message := range req.Messages {
			if message.Role == provider.RoleTool && strings.Contains(message.Content, p.expected) {
				p.observed.Store(true)
			}
		}
		chunks <- provider.Chunk{Type: provider.ChunkText, Text: "done"}
		close(chunks)
		return chunks, nil
	}
	go func() {
		defer close(chunks)
		select {
		case chunks <- provider.Chunk{Type: provider.ChunkToolCall, ToolCall: &provider.ToolCall{
			ID: "read-early", Name: "read_file", Arguments: `{"path":"speculation.txt","offset":0,"limit":20}`,
		}}:
		case <-ctx.Done():
			return
		}
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			raw, _ := os.ReadFile(p.logPath)
			if strings.Contains(string(raw), `"accepted":true`) {
				p.early.Store(true)
				// The host returns acceptance before Execute completes. Give the
				// tiny read ample time to finish. The mutation case then verifies
				// that freshness validation rejects the now-stale result.
				time.Sleep(200 * time.Millisecond)
				if p.mutate {
					_ = os.WriteFile(p.target, []byte("after-stream\n"), 0o644)
				}
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	}()
	return chunks, nil
}

// TestEffectSpeculationReadFreshness crosses the real BuildRuntime, sidecar
// reverse-RPC, provider stream, host policy, execution, claim, and tool-result
// boundaries. An unchanged source is adopted, while a post-execution mutation
// makes the speculative result stale and forces a fresh ordinary read.
func TestEffectSpeculationReadFreshness(t *testing.T) {
	cases := []struct {
		name, expected string
		mutate         bool
	}{
		{name: "unchanged source", expected: "before-speculation"},
		{name: "stale source", expected: "after-stream", mutate: true},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isolateConfigHome(t)
			dir := robustTempDir(t)
			t.Chdir(dir)
			target := dir + "/speculation.txt"
			if err := os.WriteFile(target, []byte("before-speculation\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			logPath := dir + "/speculation-start.json"
			kind := fmt.Sprintf("boot-speculation-effect-provider-%d", i)
			recorder := &speculationEffectProvider{
				logPath: logPath, target: target, expected: tc.expected, mutate: tc.mutate,
			}
			provider.Register(kind, func(provider.Config) (provider.Provider, error) { return recorder, nil })
			writeFile(t, dir, "reasonix.toml", `
default_model = "test-model"

[agent]
system_prompt = "BASE"

[environment]
enabled = false

[[providers]]
name = "test-model"
kind = "`+kind+`"
model = "x"
`)
			installBootFakePlugin(t, config.ReasonixHomeDir(), "speculation-effect", map[string]any{
				"replaces":     []string{"speculation"},
				"capabilities": []string{"strategies"},
				"env": map[string]string{
					bootFakeEnvSpeculation:    "1",
					bootFakeEnvSpeculationLog: logPath,
				},
			})
			res, err := BuildRuntime(context.Background(), Options{Sink: event.Discard})
			if err != nil {
				t.Fatalf("BuildRuntime: %v", err)
			}
			defer res.Controller.Close()
			if res.Dispatcher == nil || res.Dispatcher.Speculation() == nil {
				t.Fatal("real assembly did not bind the speculation slot owner")
			}
			if err := res.Controller.Run(context.Background(), "read the file"); err != nil {
				t.Fatalf("Run: %v", err)
			}
			if !recorder.early.Load() {
				t.Fatal("host speculative execution did not start before the provider stream closed")
			}
			if !recorder.observed.Load() {
				t.Fatalf("tool result did not contain fresh value %q", tc.expected)
			}
		})
	}
}
