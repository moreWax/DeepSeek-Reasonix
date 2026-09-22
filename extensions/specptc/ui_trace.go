package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	extension "github.com/esengine/DeepSeek-Reasonix/sdk/go"

	"github.com/esengine/DeepSeek-Reasonix/extensions/specptc/internal/engine"
	"github.com/esengine/DeepSeek-Reasonix/extensions/specptc/internal/ptc"
)

const (
	uiTraceInterval     = 200 * time.Millisecond
	uiTraceFinalTimeout = 750 * time.Millisecond
	uiTraceRows         = 24
)

type providerUIBinding struct {
	sessionID  string
	generation uint64
	host       extension.UIHostKind
}

type rlmUITrace struct {
	ctx       context.Context
	ui        extension.HostUI
	binding   providerUIBinding
	surfaceID string
	log       *log.Logger
	wake      chan struct{}
	finishCh  chan engine.Metrics
	done      chan struct{}
	closed    atomic.Bool
	mu        sync.Mutex
	state     uiTraceState
}

type uiCallRow struct {
	id        string
	name      string
	preview   string
	state     ptc.QueryTraceKind
	started   time.Time
	duration  time.Duration
	wait      time.Duration
	headStart time.Duration
	saved     time.Duration
	hit       bool
}

type uiTraceState struct {
	cache       []*uiCallRow
	cacheByID   map[string]*uiCallRow
	actual      []*uiCallRow
	actualByID  map[string]*uiCallRow
	metrics     *engine.Metrics
	spinnerStep int
}

func (s *uiTraceState) clone() uiTraceState {
	clone := uiTraceState{
		cache: make([]*uiCallRow, 0, len(s.cache)), cacheByID: make(map[string]*uiCallRow, len(s.cacheByID)),
		actual: make([]*uiCallRow, 0, len(s.actual)), actualByID: make(map[string]*uiCallRow, len(s.actualByID)),
		spinnerStep: s.spinnerStep,
	}
	for _, row := range s.cache {
		copyRow := *row
		clone.cache = append(clone.cache, &copyRow)
		clone.cacheByID[copyRow.id] = &copyRow
	}
	for _, row := range s.actual {
		copyRow := *row
		clone.actual = append(clone.actual, &copyRow)
		clone.actualByID[copyRow.id] = &copyRow
	}
	if s.metrics != nil {
		metrics := *s.metrics
		clone.metrics = &metrics
	}
	return clone
}

func (s *uiTraceState) finalizeRunning() {
	for _, row := range s.cache {
		switch row.state {
		case ptc.QueryTraceDispatch:
			row.state = ptc.QueryTraceEvicted
		case ptc.QueryTraceClaimHit:
			row.state = ptc.QueryTraceFailed
		}
	}
	for _, row := range s.actual {
		switch row.state {
		case ptc.QueryTraceClaimHit, ptc.QueryTraceClaimMiss:
			row.state = ptc.QueryTraceFailed
		}
	}
}

func newRLMUITrace(ctx context.Context, binding *providerUIBinding, streamID string, logger *log.Logger) *rlmUITrace {
	if binding == nil || binding.sessionID == "" || binding.generation == 0 || binding.host == "" || binding.host == extension.UIHostHeadless {
		return nil
	}
	digest := sha256.Sum256([]byte(streamID))
	trace := &rlmUITrace{
		ctx: ctx, binding: *binding, surfaceID: "sptc-calls-" + hex.EncodeToString(digest[:8]),
		log: logger, wake: make(chan struct{}, 1), finishCh: make(chan engine.Metrics, 1), done: make(chan struct{}),
		state: uiTraceState{cacheByID: make(map[string]*uiCallRow), actualByID: make(map[string]*uiCallRow)},
	}
	go trace.run()
	return trace
}

func (t *rlmUITrace) observe(event ptc.QueryTraceEvent) {
	if t == nil || t.closed.Load() {
		return
	}
	t.mu.Lock()
	if t.closed.Load() {
		t.mu.Unlock()
		return
	}
	t.state.apply(event)
	t.mu.Unlock()
	select {
	case t.wake <- struct{}{}:
	default:
	}
}

func (t *rlmUITrace) finish(metrics engine.Metrics) {
	if t == nil || !t.closed.CompareAndSwap(false, true) {
		return
	}
	t.finishCh <- metrics
	timer := time.NewTimer(uiTraceFinalTimeout)
	defer timer.Stop()
	select {
	case <-t.done:
	case <-timer.C:
	}
}

func (t *rlmUITrace) snapshot(tick bool) uiTraceState {
	t.mu.Lock()
	defer t.mu.Unlock()
	if tick {
		t.state.spinnerStep++
	}
	return t.state.clone()
}

func (t *rlmUITrace) finalSnapshot(metrics *engine.Metrics) uiTraceState {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.state.finalizeRunning()
	if metrics != nil {
		copyMetrics := *metrics
		t.state.metrics = &copyMetrics
	}
	return t.state.clone()
}

func (t *rlmUITrace) run() {
	defer close(t.done)
	ticker := time.NewTicker(uiTraceInterval)
	defer ticker.Stop()
	published := false
	for {
		select {
		case <-t.ctx.Done():
			timer := time.NewTimer(uiTraceFinalTimeout)
			select {
			case metrics := <-t.finishCh:
				timer.Stop()
				t.publishFinal(&metrics)
			case <-timer.C:
				t.publishFinal(nil)
			}
			return
		case metrics := <-t.finishCh:
			t.publishFinal(&metrics)
			return
		case <-t.wake:
			state := t.snapshot(false)
			if t.publish(t.ctx, &state) == nil && t.ctx.Err() == nil {
				published = true
			}
		case <-ticker.C:
			state := t.snapshot(true)
			if !published || state.running() {
				if t.publish(t.ctx, &state) == nil && t.ctx.Err() == nil {
					published = true
				}
			}
		}
	}
}

func (t *rlmUITrace) publishFinal(metrics *engine.Metrics) {
	state := t.finalSnapshot(metrics)
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.ctx), uiTraceFinalTimeout)
	defer cancel()
	_ = t.publish(ctx, &state)
}

func (t *rlmUITrace) publish(ctx context.Context, state *uiTraceState) error {
	if len(state.cache) == 0 && len(state.actual) == 0 {
		return nil
	}
	payload := extension.UICardPayload{
		Title:    "sPTC · tool calls",
		Markdown: state.markdown(time.Now()),
	}
	if state.metrics != nil {
		metrics := state.metrics
		payload.Fields = []extension.UIKeyValue{
			{Key: "cache", Value: fmt.Sprintf("%d hit · %d miss · %d wasted", metrics.Hits, metrics.Misses, metrics.Wasted)},
			{Key: "latency", Value: fmt.Sprintf("%s saved · %s aggregate wait", formatDuration(metrics.Saved), formatDuration(metrics.ActualWait))},
		}
	}
	if err := t.ui.PublishCard(ctx, t.binding.sessionID, t.binding.generation, t.surfaceID, payload); err != nil {
		if t.log != nil && ctx.Err() == nil {
			t.log.Printf("publish sPTC call surface: %v", err)
		}
		return err
	}
	return nil
}

func (s *uiTraceState) apply(event ptc.QueryTraceEvent) {
	now := time.Now()
	switch event.Kind {
	case ptc.QueryTraceDispatch:
		row := &uiCallRow{id: event.ID, name: event.Name, preview: event.Preview, state: event.Kind, started: now}
		s.cache = append(s.cache, row)
		s.cacheByID[event.ID] = row
	case ptc.QueryTraceReady, ptc.QueryTraceEvicted:
		row := s.cacheByID[event.ID]
		if row == nil {
			row = &uiCallRow{id: event.ID, name: event.Name, preview: event.Preview, started: now}
			s.cache = append(s.cache, row)
			s.cacheByID[event.ID] = row
		}
		if event.Kind != ptc.QueryTraceReady || row.state == ptc.QueryTraceDispatch || row.state == "" {
			if row.state != ptc.QueryTraceDone {
				row.state = event.Kind
			}
		}
		row.duration = event.Duration
	case ptc.QueryTraceFailed:
		if event.Speculative {
			row := s.cacheByID[event.ID]
			if row == nil {
				row = &uiCallRow{id: event.ID, name: event.Name, preview: event.Preview, started: now}
				s.cache = append(s.cache, row)
				s.cacheByID[event.ID] = row
			}
			if row.state != ptc.QueryTraceDone {
				row.state = event.Kind
			}
			row.duration = event.Duration
			return
		}
		if claimed := s.actualByID["claim:"+event.ID]; claimed != nil {
			claimed.state = event.Kind
			claimed.duration, claimed.wait = event.Duration, event.Wait
		}
		key := "inline:" + event.ID
		row := s.actualByID[key]
		if row == nil {
			row = &uiCallRow{id: key, name: event.Name, preview: event.Preview, started: now}
			s.actual = append(s.actual, row)
			s.actualByID[key] = row
		}
		row.state, row.duration, row.wait = event.Kind, event.Duration, event.Wait
	case ptc.QueryTraceClaimHit:
		row := s.cacheByID[event.ID]
		if row == nil {
			row = &uiCallRow{id: event.ID, name: event.Name, preview: event.Preview, started: now}
			s.cache = append(s.cache, row)
			s.cacheByID[event.ID] = row
		}
		row.state, row.headStart, row.hit = event.Kind, event.HeadStart, true
		key := "claim:" + event.ID
		actual := s.actualByID[key]
		if actual == nil {
			actual = &uiCallRow{id: key, name: event.Name, preview: event.Preview, started: now}
			s.actual = append(s.actual, actual)
			s.actualByID[key] = actual
		}
		actual.state, actual.headStart, actual.hit = event.Kind, event.HeadStart, true
	case ptc.QueryTraceClaimMiss:
		if claimed := s.actualByID["claim:"+event.ID]; claimed != nil {
			claimed.state = ptc.QueryTraceFailed
		}
		key := "inline:" + event.ID
		row := s.actualByID[key]
		if row == nil {
			row = &uiCallRow{id: key, name: event.Name, preview: event.Preview, state: event.Kind, started: now}
			s.actual = append(s.actual, row)
			s.actualByID[key] = row
		}
	case ptc.QueryTraceDone:
		if event.Hit {
			row := s.cacheByID[event.ID]
			if row != nil {
				row.state, row.hit = event.Kind, true
				row.duration, row.wait, row.saved = event.Duration, event.Wait, event.Saved
			}
			key := "claim:" + event.ID
			actual := s.actualByID[key]
			if actual == nil {
				actual = &uiCallRow{id: key, name: event.Name, preview: event.Preview, started: now}
				s.actual = append(s.actual, actual)
				s.actualByID[key] = actual
			}
			actual.state, actual.hit = event.Kind, true
			actual.duration, actual.wait, actual.saved = event.Duration, event.Wait, event.Saved
			return
		}
		key := "inline:" + event.ID
		row := s.actualByID[key]
		if row == nil {
			row = &uiCallRow{id: key, name: event.Name, preview: event.Preview, started: now}
			s.actual = append(s.actual, row)
			s.actualByID[key] = row
		}
		row.state, row.duration, row.wait = event.Kind, event.Duration, event.Wait
	}
}

func (s *uiTraceState) running() bool {
	for _, rows := range [][]*uiCallRow{s.cache, s.actual} {
		for _, row := range rows {
			if row.state == ptc.QueryTraceDispatch || row.state == ptc.QueryTraceClaimMiss || row.state == ptc.QueryTraceClaimHit {
				return true
			}
		}
	}
	return false
}

func (s *uiTraceState) markdown(now time.Time) string {
	cache, cacheOmitted := visibleTraceRows(s.cache)
	actual, actualOmitted := visibleTraceRows(s.actual)
	count := max(len(cache), len(actual))
	var out strings.Builder
	out.WriteString("| speculation cache | actually running |\n|---|---|\n")
	for i := range count {
		left, right := "", ""
		if i < len(cache) {
			left = cache[i].render(now, true, s.spinnerStep)
		}
		if i < len(actual) {
			right = actual[i].render(now, false, s.spinnerStep)
		}
		fmt.Fprintf(&out, "| %s | %s |\n", escapeTable(left), escapeTable(right))
	}
	if cacheOmitted > 0 || actualOmitted > 0 {
		fmt.Fprintf(&out, "| %s | %s |\n", omittedLabel(cacheOmitted), omittedLabel(actualOmitted))
	}
	return strings.TrimRight(out.String(), "\n")
}

func (r *uiCallRow) render(now time.Time, speculative bool, spinnerStep int) string {
	label := strings.TrimSpace(r.name)
	if label == "" {
		label = "Query"
	}
	call := label + "(" + r.preview + ")"
	switch r.state {
	case ptc.QueryTraceDispatch:
		return fmt.Sprintf("%s %s — speculating · %s", traceSpinner(spinnerStep), call, formatDuration(now.Sub(r.started)))
	case ptc.QueryTraceReady:
		return fmt.Sprintf("✓ %s — cached · took %s", call, formatDuration(r.duration))
	case ptc.QueryTraceClaimHit:
		return fmt.Sprintf("✚ %s — cache hit · +%s head start", call, formatDuration(r.headStart))
	case ptc.QueryTraceClaimMiss:
		return fmt.Sprintf("○ %s — cache miss · running inline · %s", call, formatDuration(now.Sub(r.started)))
	case ptc.QueryTraceDone:
		if r.hit {
			if speculative {
				return fmt.Sprintf("✚ %s — cache hit · took %s · waited %s · +%s saved", call, formatDuration(r.duration), formatDuration(r.wait), formatDuration(r.saved))
			}
			return fmt.Sprintf("✚ %s — cache hit · waited %s", call, formatDuration(r.wait))
		}
		return fmt.Sprintf("✓ %s — inline · took %s", call, formatDuration(r.duration))
	case ptc.QueryTraceFailed:
		if speculative {
			return "✕ " + call + " — failed"
		}
		return fmt.Sprintf("✕ %s — failed · after %s", call, formatDuration(r.wait))
	case ptc.QueryTraceEvicted:
		return "✕ " + call + " — evicted"
	default:
		if speculative {
			return "… " + call
		}
		return "○ " + call
	}
}

func visibleTraceRows(rows []*uiCallRow) ([]*uiCallRow, int) {
	if len(rows) <= uiTraceRows {
		return rows, 0
	}
	return rows[len(rows)-uiTraceRows:], len(rows) - uiTraceRows
}

func omittedLabel(count int) string {
	if count == 0 {
		return ""
	}
	return fmt.Sprintf("… %d earlier calls", count)
}

func escapeTable(value string) string {
	value = strings.ReplaceAll(value, "|", "\\|")
	return strings.ReplaceAll(value, "\n", " ")
}

func traceSpinner(step int) string {
	frames := [...]string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
	return frames[step%len(frames)]
}

func formatDuration(value time.Duration) string {
	if value < 0 {
		value = 0
	}
	return fmt.Sprintf("%.1fs", value.Seconds())
}
