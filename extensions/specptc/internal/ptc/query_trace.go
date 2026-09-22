package ptc

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// QueryTraceKind is one observable lifecycle transition for an RLM Query call.
type QueryTraceKind string

const (
	QueryTraceDispatch  QueryTraceKind = "dispatch"
	QueryTraceReady     QueryTraceKind = "ready"
	QueryTraceClaimHit  QueryTraceKind = "claim_hit"
	QueryTraceClaimMiss QueryTraceKind = "claim_miss"
	QueryTraceDone      QueryTraceKind = "done"
	QueryTraceFailed    QueryTraceKind = "failed"
	QueryTraceEvicted   QueryTraceKind = "evicted"
)

// QueryTraceEvent contains bounded, extension-local data used to render the
// speculative cache. Errors, model output, and full prompt values are omitted.
type QueryTraceEvent struct {
	Kind        QueryTraceKind
	ID          string
	Name        string
	Preview     string
	Speculative bool
	Hit         bool
	Duration    time.Duration
	Wait        time.Duration
	HeadStart   time.Duration
	Saved       time.Duration
}

// QueryTraceFunc observes Query lifecycle events. Implementations must return
// promptly because calls can originate on scheduler and model goroutines.
type QueryTraceFunc func(QueryTraceEvent)

func emitQueryTrace(trace QueryTraceFunc, event QueryTraceEvent) {
	if trace != nil {
		trace(event)
	}
}

func queryTracePreview(values []any) string {
	const (
		maxValues = 2
		maxRunes  = 42
	)
	parts := make([]string, 0, min(len(values), maxValues))
	for i, value := range values {
		if i == maxValues {
			parts = append(parts, "…")
			break
		}
		var part string
		switch typed := value.(type) {
		case string:
			part = strconv.Quote(truncateTraceRunes(typed, maxRunes))
		case nil:
			part = "nil"
		case bool, float32, float64, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			part = fmt.Sprint(typed)
		default:
			part = "…"
		}
		parts = append(parts, part)
	}
	return strings.Join(parts, ", ")
}

func truncateTraceRunes(value string, limit int) string {
	if utf8.RuneCountInString(value) <= limit {
		return value
	}
	runes := []rune(value)
	return string(runes[:max(limit-1, 0)]) + "…"
}
