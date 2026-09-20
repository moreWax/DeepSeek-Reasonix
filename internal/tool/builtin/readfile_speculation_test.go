package builtin

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestReadFileSpeculationRejectsContinuationCursor(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "x"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	reader := readFile{workDir: dir}
	policy, ok := reader.SpeculationPolicy([]byte(`{"path":"x"}`))
	if !ok || !policy.Pure || policy.Deterministic {
		t.Fatalf("ordinary read policy = %+v, %v", policy, ok)
	}
	if _, ok := reader.SpeculationPolicy([]byte(`{"cursor":"cursor-1"}`)); ok {
		t.Fatal("continuation cursor was eligible for raw speculative Execute")
	}
}

func TestReadFileSpeculationValidatesDiskFreshness(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "source.txt")
	if err := os.WriteFile(path, []byte("before\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	reader := readFile{workDir: dir}
	args := json.RawMessage(`{"path":"source.txt"}`)
	_, envelope, err := reader.ExecuteRead(context.Background(), args)
	if err != nil {
		t.Fatalf("ExecuteRead: %v", err)
	}
	if !reader.ValidateSpeculativeRead(context.Background(), args, envelope) {
		t.Fatal("unchanged disk source was rejected")
	}
	replacement := filepath.Join(dir, "replacement.txt")
	if err := os.WriteFile(replacement, []byte("after\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
	if reader.ValidateSpeculativeRead(context.Background(), args, envelope) {
		t.Fatal("replaced disk source was accepted")
	}
}

func TestReadFileSpeculationValidatesOverlayFreshness(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "source.txt")
	overlay := &fakeOverlay{files: map[string]string{path: "before\n"}}
	reader := readFile{workDir: dir, overlay: overlay}
	args := json.RawMessage(`{"path":"source.txt"}`)
	_, envelope, err := reader.ExecuteRead(context.Background(), args)
	if err != nil {
		t.Fatalf("ExecuteRead: %v", err)
	}
	if !reader.ValidateSpeculativeRead(context.Background(), args, envelope) {
		t.Fatal("unchanged overlay source was rejected")
	}
	overlay.files[path] = "after\n"
	if reader.ValidateSpeculativeRead(context.Background(), args, envelope) {
		t.Fatal("changed overlay source was accepted")
	}
	delete(overlay.files, path)
	if reader.ValidateSpeculativeRead(context.Background(), args, envelope) {
		t.Fatal("overlay-to-disk transition was accepted")
	}
}
