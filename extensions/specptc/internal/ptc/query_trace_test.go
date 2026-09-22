package ptc

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestQueryTracePreviewIsBoundedAndOmitsOpaqueValues(t *testing.T) {
	preview := queryTracePreview([]any{strings.Repeat("界", 1000), map[string]any{"secret": "value"}, "ignored"})
	if utf8.RuneCountInString(preview) > 64 {
		t.Fatalf("preview rune count = %d: %q", utf8.RuneCountInString(preview), preview)
	}
	if strings.Contains(preview, "secret") || !strings.Contains(preview, "…") {
		t.Fatalf("preview = %q", preview)
	}
}
