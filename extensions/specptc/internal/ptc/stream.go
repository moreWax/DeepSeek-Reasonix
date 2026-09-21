// Package ptc implements the streamed programmatic-tool-call frontend used by
// the spec-ptc extension. It is intentionally independent from Reasonix host
// packages so the extension owns the complete RLM execution path.
package ptc

import (
	"go/parser"
	"go/token"
	"strings"
)

// Segment is one complete Go REPL statement recovered from a streamed fenced
// code block. Source is preserved exactly as generated apart from surrounding
// Markdown fences.
type Segment struct {
	Source string
}

// StreamSegmenter incrementally extracts complete statements from ```go and
// ```repl Markdown blocks. A complete statement is retained for one additional
// line so an immediately following else clause cannot be split from its if.
type StreamSegmenter struct {
	line      strings.Builder
	statement strings.Builder
	candidate string
	block     strings.Builder
	blockID   uint64
	inCode    bool
}

// Feed consumes one arbitrary stream delta and returns statements that became
// unambiguously complete while processing it.
func (s *StreamSegmenter) Feed(delta string) []Segment {
	var out []Segment
	for _, r := range delta {
		s.line.WriteRune(r)
		if r == '\n' {
			out = append(out, s.consumeLine(s.line.String())...)
			s.line.Reset()
		}
	}
	return out
}

// CurrentBlock returns the complete source received for the current Markdown
// code block, including an unterminated stream line when it is code rather
// than the closing fence. BlockID increases whenever a new block starts.
func (s *StreamSegmenter) CurrentBlock() (source string, blockID uint64, active bool) {
	if s.blockID == 0 {
		return "", 0, false
	}
	source = s.block.String()
	if s.inCode && !strings.HasPrefix(strings.TrimSpace(s.line.String()), "```") {
		source += s.line.String()
	}
	return source, s.blockID, s.inCode
}

// PendingTail returns code accepted from the active block but not emitted yet.
// It is used by the speculative planner to inspect an open loop or expression.
func (s *StreamSegmenter) PendingTail() string {
	if !s.inCode {
		return ""
	}
	return s.candidate + s.statement.String() + s.line.String()
}

// Finish drains a final unterminated line and any complete pending statement.
// Incomplete generated code is deliberately ignored; authoritative execution
// will report its syntax error through the ordinary RLM path.
func (s *StreamSegmenter) Finish() []Segment {
	var out []Segment
	if s.line.Len() > 0 {
		out = append(out, s.consumeLine(s.line.String())...)
		s.line.Reset()
	}
	if s.inCode {
		out = append(out, s.flushBlock()...)
		s.inCode = false
	}
	return out
}

func (s *StreamSegmenter) consumeLine(line string) []Segment {
	trimmed := strings.TrimSpace(line)
	if !s.inCode {
		if startsCodeFence(trimmed) {
			s.inCode = true
			s.block.Reset()
			s.blockID++
		}
		return nil
	}
	if trimmed == "```" && !lexicallyOpen(s.candidate+s.statement.String()) {
		out := s.flushBlock()
		s.inCode = false
		return out
	}
	s.block.WriteString(line)

	var out []Segment
	if s.candidate != "" {
		if continuesCandidate(trimmed) {
			s.statement.WriteString(strings.TrimRight(s.candidate, "\r\n"))
			s.statement.WriteByte(' ')
			s.candidate = ""
		} else {
			out = append(out, Segment{Source: s.candidate})
			s.candidate = ""
		}
	}
	if trimmed == "" && s.statement.Len() == 0 {
		return out
	}
	s.statement.WriteString(line)
	if completeStatement(s.statement.String()) {
		s.candidate = s.statement.String()
		s.statement.Reset()
	}
	return out
}

func (s *StreamSegmenter) flushBlock() []Segment {
	out := make([]Segment, 0, 2)
	if s.candidate != "" {
		out = append(out, Segment{Source: s.candidate})
		s.candidate = ""
	}
	if source := s.statement.String(); completeStatement(source) {
		out = append(out, Segment{Source: source})
	}
	s.statement.Reset()
	return out
}

func startsCodeFence(line string) bool {
	if !strings.HasPrefix(line, "```") {
		return false
	}
	language := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(line, "```")))
	return language == "go" || language == "repl"
}

func continuesCandidate(line string) bool {
	return line == "else" || strings.HasPrefix(line, "else ") || strings.HasPrefix(line, "else{") ||
		strings.HasPrefix(line, "else {")
}

func completeStatement(source string) bool {
	if strings.TrimSpace(source) == "" || lexicallyOpen(source) {
		return false
	}
	fset := token.NewFileSet()
	wrapped := "package p\nfunc _(){\n" + source + "\n}\n"
	if _, err := parser.ParseFile(fset, "stream.go", wrapped, parser.AllErrors); err == nil {
		return true
	}
	_, err := parser.ParseFile(fset, "stream.go", "package p\n"+source, parser.AllErrors)
	return err == nil
}

// lexicallyOpen reports constructs for which a zero punctuation balance would
// still be misleading, notably quoted strings and block comments.
func lexicallyOpen(source string) bool {
	var paren, bracket, brace int
	var quote rune
	escaped := false
	lineComment := false
	blockComment := false
	runes := []rune(source)
	for i, r := range runes {
		if lineComment {
			if r == '\n' {
				lineComment = false
			}
			continue
		}
		if blockComment {
			if r == '*' && i+1 < len(runes) && runes[i+1] == '/' {
				blockComment = false
			}
			continue
		}
		if quote != 0 {
			if quote == '`' {
				if r == '`' {
					quote = 0
				}
				continue
			}
			if escaped {
				escaped = false
				continue
			}
			if r == '\\' {
				escaped = true
				continue
			}
			if r == quote {
				quote = 0
			}
			continue
		}
		if r == '/' && i+1 < len(runes) {
			switch runes[i+1] {
			case '/':
				lineComment = true
				continue
			case '*':
				blockComment = true
				continue
			}
		}
		switch r {
		case '\'', '"', '`':
			quote = r
		case '(':
			paren++
		case ')':
			paren--
		case '[':
			bracket++
		case ']':
			bracket--
		case '{':
			brace++
		case '}':
			brace--
		}
	}
	return quote != 0 || blockComment || paren > 0 || bracket > 0 || brace > 0
}
