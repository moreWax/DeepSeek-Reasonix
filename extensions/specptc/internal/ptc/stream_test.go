package ptc

import (
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

func TestStreamSegmenterStreamsOnlyCompleteStatements(t *testing.T) {
	source := "outside\n```repl\nresults := []string{}\nfor _, value := range []string{\"a\", \"b\"} {\n\tresults = append(results, value)\n}\n\nprintln(len(results))\n```\nafter\n"
	var segmenter StreamSegmenter
	var got []Segment
	for _, r := range source {
		got = append(got, segmenter.Feed(string(r))...)
		for _, segment := range got {
			assertValidREPLStatement(t, segment.Source)
		}
	}
	got = append(got, segmenter.Finish()...)
	if len(got) != 3 {
		t.Fatalf("segments = %d, want 3: %#v", len(got), got)
	}
	if !strings.Contains(got[1].Source, "for _, value") || !strings.Contains(got[1].Source, "append") {
		t.Fatalf("loop was split or lost: %q", got[1].Source)
	}
}

func TestStreamSegmenterPreservesMultilineConstructs(t *testing.T) {
	source := "```go\n" +
		"x := (1 +\n  2)\n" +
		"text := `multi\nline`\n" +
		"fn := func(a,\n b int) int {\n return a + b\n}\n" +
		"_ = fn(x, len(text))\n```\n"
	var segmenter StreamSegmenter
	var got []Segment
	for i := 0; i < len(source); i += 3 {
		end := min(i+3, len(source))
		got = append(got, segmenter.Feed(source[i:end])...)
	}
	got = append(got, segmenter.Finish()...)
	if len(got) != 4 {
		t.Fatalf("segments = %d, want 4: %#v", len(got), got)
	}
	for _, segment := range got {
		assertValidREPLStatement(t, segment.Source)
	}
	if !strings.Contains(got[1].Source, "multi\nline") {
		t.Fatalf("raw string was split: %q", got[1].Source)
	}
}

func TestStreamSegmenterKeepsElseWithIf(t *testing.T) {
	source := "```go\nif flag {\n answer = \"yes\"\n}\nelse {\n answer = \"no\"\n}\nnext := 1\n```\n"
	var segmenter StreamSegmenter
	got := segmenter.Feed(source)
	got = append(got, segmenter.Finish()...)
	if len(got) != 2 {
		t.Fatalf("segments = %d, want 2: %#v", len(got), got)
	}
	if !strings.Contains(got[0].Source, "else") {
		t.Fatalf("else detached from if: %q", got[0].Source)
	}
	assertValidREPLStatement(t, got[0].Source)
}

func TestStreamSegmenterExposesOpenTailForPeek(t *testing.T) {
	var segmenter StreamSegmenter
	got := segmenter.Feed("```repl\nchunks := []string{\"a\", \"b\"}\nresults := []string{}\nfor _, chunk := range chunks {\n results = append(results, Query(\"sum: \"+chunk))\n")
	if len(got) != 2 {
		t.Fatalf("closed prefix segments = %d, want 2", len(got))
	}
	tail := segmenter.PendingTail()
	if !strings.Contains(tail, "for _, chunk") || !strings.Contains(tail, "Query") {
		t.Fatalf("pending tail = %q", tail)
	}
	if completeStatement(tail) {
		t.Fatalf("open loop reported complete: %q", tail)
	}
}

func TestStreamSegmenterIgnoresUnfencedAndIncompleteCode(t *testing.T) {
	var segmenter StreamSegmenter
	got := segmenter.Feed("x := Query(\"outside\")\n```go\ny := Query(\"unfinished\"")
	got = append(got, segmenter.Finish()...)
	if len(got) != 0 {
		t.Fatalf("segments = %#v, want none", got)
	}
}

func assertValidREPLStatement(t *testing.T, source string) {
	t.Helper()
	wrapped := "package p\nfunc _(){\n" + source + "\n}\n"
	if _, err := parser.ParseFile(token.NewFileSet(), "segment.go", wrapped, parser.AllErrors); err != nil {
		t.Fatalf("invalid segment %q: %v", source, err)
	}
}
