//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package builtin

import (
	"context"
	"encoding/json"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestReadFileSpeculationDoesNotBlockOnFIFO(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pipe")
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	reader := readFile{workDir: dir}
	args := json.RawMessage(`{"path":"pipe"}`)
	done := make(chan error, 1)
	go func() {
		_, _, err := reader.ExecuteRead(context.Background(), args)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("FIFO read unexpectedly succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("FIFO read blocked despite non-blocking open")
	}
}
