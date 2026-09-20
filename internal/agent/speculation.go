package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"reasonix/internal/extension"
	"reasonix/internal/extension/dispatch"
	"reasonix/internal/extension/protocol"
	"reasonix/internal/fileops"
	"reasonix/internal/provider"
	"reasonix/internal/runtimepolicy"
	"reasonix/internal/tool"
)

const (
	speculationRPCTimeout     = 5 * time.Second
	speculationObserveDepth   = 256
	speculationObserveWorkers = 8
	speculationHostWorkers    = 64
)

var hostSpeculationSlots = make(chan struct{}, speculationHostWorkers)

type speculationObservation struct {
	seq  uint64
	call protocol.SpeculationCall
}

type streamSpeculation struct {
	agent *Agent
	round *speculationRound
}

func (a *Agent) openStreamSpeculation(ctx context.Context, turn int, attemptID string) *streamSpeculation {
	return &streamSpeculation{agent: a, round: a.beginSpeculationRound(ctx, turn, attemptID)}
}

func (s *streamSpeculation) close() {
	if s != nil && s.round != nil {
		s.agent.endSpeculationRound(s.round)
	}
}

func (s *streamSpeculation) retain(hasCalls bool) *speculationRound {
	if s == nil || !hasCalls {
		return nil
	}
	round := s.round
	s.round = nil
	return round
}

func (s *streamSpeculation) observe(a *Agent, call provider.ToolCall) {
	if s != nil {
		a.observeSpeculation(s.round, call)
	}
}

func (s *streamSpeculation) completeCall(a *Agent, chunk provider.Chunk, calls, partial []provider.ToolCall, maxArgChars int) ([]provider.ToolCall, []provider.ToolCall, int) {
	if chunk.ToolCall == nil {
		return calls, partial, maxArgChars
	}
	call := *chunk.ToolCall
	calls = append(calls, call)
	s.observe(a, call)
	partial = upsertPartialToolCall(partial, call)
	return calls, partial, max(maxArgChars, len(call.Arguments))
}

type speculationRound struct {
	scope        protocol.SpeculationScope
	client       dispatch.SpeculationClient
	owner        *dispatch.Dispatcher
	seq          atomic.Uint64
	failed       atomic.Bool
	observations chan speculationObservation
	closeOnce    sync.Once
	wg           sync.WaitGroup
}

type hostSpeculationRuntime struct {
	mu        sync.Mutex
	next      uint64
	nextRound atomic.Uint64
	scopes    map[protocol.SpeculationScope]*hostSpeculationScope
}

type hostSpeculationScope struct {
	ctx     context.Context
	cancel  context.CancelFunc
	turn    *turnRuntime
	client  dispatch.SpeculationClient
	closing bool
	allowed map[string]protocol.SpeculationCall
	started map[string]struct{}
	tokens  map[string]*hostSpeculationToken
}

type hostSpeculationExecution struct {
	result       string
	err          error
	readEnvelope *tool.ReadResultEnvelope
	observations *fileops.Store
	activeMillis int64
}

type hostSpeculationLaunch struct {
	agent                    *Agent
	candidate                tool.Tool
	turn                     *turnRuntime
	client                   dispatch.SpeculationClient
	scope                    protocol.SpeculationScope
	handle                   string
	token                    *hostSpeculationToken
	execCtx                  context.Context
	releaseGate, releaseSlot func()
}

func (l hostSpeculationLaunch) run() {
	results := make(chan hostSpeculationExecution, 1)
	executionDone := make(chan struct{})
	started := time.Now()
	go l.execute(results, executionDone, started)
	go func() {
		defer l.releaseSlot()
		if err := l.execCtx.Err(); err != nil {
			l.token.err = err
		} else {
			select {
			case result := <-results:
				if err := l.execCtx.Err(); err != nil {
					l.token.err = err
				} else {
					l.token.result, l.token.err = result.result, result.err
					l.token.readEnvelope, l.token.observations = result.readEnvelope, result.observations
					l.token.activeMillis = result.activeMillis
				}
			case <-l.execCtx.Done():
				l.token.err = l.execCtx.Err()
			}
		}
		if l.token.activeMillis == 0 {
			l.token.activeMillis = max(1, time.Since(started).Milliseconds())
		}
		close(l.token.done)
		completion := protocol.SpeculationReady
		if l.token.err != nil {
			completion = protocol.SpeculationFailed
		}
		completionCtx, cancelCompletion := context.WithTimeout(context.Background(), speculationRPCTimeout)
		_, _ = l.client.CompleteSpeculation(completionCtx, protocol.SpeculationCompleteParams{
			Scope: l.scope, Handle: l.handle, Completion: completion,
		})
		cancelCompletion()
		// Keep the bounded host slot until both execution and terminal delivery
		// finish. A stuck tool or sidecar can suppress speculation, but cannot
		// create unbounded detached work or lose completion state.
		<-executionDone
	}()
}

func (l hostSpeculationLaunch) execute(results chan<- hostSpeculationExecution, executionDone chan<- struct{}, started time.Time) {
	defer close(executionDone)
	defer l.releaseGate()
	result := hostSpeculationExecution{observations: fileops.NewStore()}
	callCtx := fileops.WithStore(withTurnState(l.agent.withAgentContext(l.execCtx), l.turn), result.observations)
	if reader, ok := l.candidate.(tool.ReadExecutor); ok {
		var envelope tool.ReadResultEnvelope
		result.result, envelope, result.err = reader.ExecuteRead(callCtx, l.token.call.Arguments)
		if result.err == nil {
			result.readEnvelope = &envelope
		}
	} else {
		result.result, result.err = l.candidate.Execute(callCtx, l.token.call.Arguments)
	}
	result.activeMillis = max(1, time.Since(started).Milliseconds())
	results <- result
}

type hostSpeculationToken struct {
	call         protocol.SpeculationCall
	reusable     bool
	cancel       context.CancelFunc
	done         chan struct{}
	result       string
	err          error
	readEnvelope *tool.ReadResultEnvelope
	observations *fileops.Store
	activeMillis int64
	claims       int
}

func (a *Agent) beginSpeculationRound(ctx context.Context, turn int, attemptID string) *speculationRound {
	if a == nil || a.svc.extensions == nil {
		return nil
	}
	owner := a.svc.extensions
	client := owner.Speculation()
	if client == nil {
		return nil
	}
	base := client.SpeculationContext()
	if attemptID == "" {
		attemptID = fmt.Sprintf("run-%d-spec-%d", a.protocolRunSeq.Load(), a.speculation.nextRound.Add(1))
	}
	scope := protocol.SpeculationScope{
		Generation: base.Generation,
		SessionID:  base.SessionID,
		TurnID:     fmt.Sprintf("%d", turn),
		AttemptID:  attemptID,
	}
	if !owner.RegisterSpeculationScope(scope, a) {
		return nil
	}
	if !a.speculation.begin(ctx, scope, &a.turn, client) {
		owner.UnregisterSpeculationScope(scope)
		return nil
	}
	rpcCtx, cancel := context.WithTimeout(ctx, speculationRPCTimeout)
	result, err := client.BeginSpeculation(rpcCtx, protocol.SpeculationBeginParams{Scope: scope})
	cancel()
	if err != nil || !result.Accepted {
		a.speculation.end(scope)
		owner.UnregisterSpeculationScope(scope)
		return nil
	}
	round := &speculationRound{
		scope: scope, client: client, owner: owner,
		observations: make(chan speculationObservation, speculationObserveDepth),
	}
	for range speculationObserveWorkers {
		round.wg.Add(1)
		go round.runObserver()
	}
	return round
}

func (round *speculationRound) runObserver() {
	defer round.wg.Done()
	for observation := range round.observations {
		if round.failed.Load() {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), speculationRPCTimeout)
		result, err := round.client.ObserveSpeculation(ctx, protocol.SpeculationObserveParams{
			Scope: round.scope, Seq: observation.seq, Call: observation.call,
		})
		cancel()
		if err != nil || !result.Accepted {
			round.failed.Store(true)
		}
	}
}

func (a *Agent) observeSpeculation(round *speculationRound, call provider.ToolCall) {
	if round == nil || call.ID == "" || call.Name == "" || !json.Valid([]byte(call.Arguments)) {
		return
	}
	wireCall := protocol.SpeculationCall{ID: call.ID, Name: call.Name, Arguments: json.RawMessage(call.Arguments)}
	if !a.speculation.allow(round.scope, wireCall) {
		round.failed.Store(true)
		return
	}
	seq := round.seq.Add(1)
	select {
	case round.observations <- speculationObservation{seq: seq, call: wireCall}:
	default:
		round.failed.Store(true)
	}
}

type speculationAdoption struct {
	result       string
	err          error
	readEnvelope *tool.ReadResultEnvelope
	observations *fileops.Store
	activeMillis int64
}

func (a *Agent) dispatchOrAdoptSpeculation(ctx, callCtx context.Context, plan *toolCallPlan) (string, []string, *tool.ShellExecution, error) {
	if result, err, adopted := a.tryAdoptSpeculation(ctx, callCtx, plan); adopted {
		return result, nil, nil, err
	}
	return a.dispatchResolvedTool(callCtx, plan)
}

func (a *Agent) tryAdoptSpeculation(ctx, callCtx context.Context, plan *toolCallPlan) (string, error, bool) {
	call, runTool := plan.call, plan.runTool
	if runTool == nil || runTool.Name() != call.Name || string(plan.runArgs) != call.Arguments {
		return "", nil, false
	}
	adoption, claimed := a.claimSpeculation(ctx, plan.speculation, call)
	if !claimed || !a.validateSpeculativeRead(callCtx, plan, adoption) {
		return "", nil, false
	}
	return adoption.result, adoption.err, true
}

func (a *Agent) validateSpeculativeRead(ctx context.Context, plan *toolCallPlan, adoption speculationAdoption) bool {
	if err := ctx.Err(); err != nil || adoption.err != nil {
		return false
	}
	if _, isRead := plan.runTool.(tool.ReadExecutor); !isRead {
		return true
	}
	validator, validates := plan.runTool.(tool.SpeculativeReadValidator)
	if adoption.readEnvelope == nil || !validates ||
		!validator.ValidateSpeculativeRead(ctx, plan.runArgs, *adoption.readEnvelope) || ctx.Err() != nil {
		return false
	}
	plan.readEnvelope = adoption.readEnvelope
	plan.readActiveMillis += adoption.activeMillis
	a.fileObservations.MergeFrom(adoption.observations)
	return true
}

func (a *Agent) claimSpeculation(ctx context.Context, round *speculationRound, call provider.ToolCall) (speculationAdoption, bool) {
	if round == nil || round.failed.Load() {
		return speculationAdoption{}, false
	}
	wireCall := protocol.SpeculationCall{ID: call.ID, Name: call.Name, Arguments: json.RawMessage(call.Arguments)}
	rpcCtx, cancel := context.WithTimeout(ctx, speculationRPCTimeout)
	claimed, err := round.client.ClaimSpeculation(rpcCtx, protocol.SpeculationClaimParams{
		Scope: round.scope, BarrierSeq: round.seq.Load(), Call: wireCall,
	})
	cancel()
	if err != nil || !claimed.Hit {
		return speculationAdoption{}, false
	}
	return a.speculation.adoptDetailed(ctx, round.scope, claimed.Handle, wireCall)
}

func (a *Agent) activateSpeculativeToolLoop(turn *turnRuntime) func() {
	releaseMCPListObserver := a.activateMCPListObserver()
	return func() {
		releaseMCPListObserver()
		a.clearTurnSpeculation(turn)
	}
}

func (a *Agent) sealTurnSpeculation(turn *turnRuntime) {
	if turn != nil && turn.speculation != nil {
		turn.speculation.seal()
	}
}

func (a *Agent) clearTurnSpeculation(turn *turnRuntime) {
	if turn == nil || turn.speculation == nil {
		return
	}
	round := turn.speculation
	turn.speculation = nil
	a.endSpeculationRound(round)
}

func (round *speculationRound) seal() {
	if round == nil {
		return
	}
	round.closeOnce.Do(func() { close(round.observations) })
	round.wg.Wait()
}

func (a *Agent) endSpeculationRound(round *speculationRound) {
	if round == nil {
		return
	}
	round.seal()
	a.speculation.close(round.scope)
	ctx, cancel := context.WithTimeout(context.Background(), speculationRPCTimeout)
	barrierSeq := round.seq.Load()
	if round.failed.Load() {
		barrierSeq = 0
	}
	_, _ = round.client.EndSpeculation(ctx, protocol.SpeculationEndParams{
		Scope: round.scope, BarrierSeq: barrierSeq,
	})
	cancel()
	a.speculation.end(round.scope)
	round.owner.UnregisterSpeculationScope(round.scope)
}

func (r *hostSpeculationRuntime) begin(ctx context.Context, scope protocol.SpeculationScope, turn *turnRuntime, client dispatch.SpeculationClient) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.scopes == nil {
		r.scopes = make(map[protocol.SpeculationScope]*hostSpeculationScope)
	}
	if _, exists := r.scopes[scope]; exists {
		return false
	}
	scopeCtx, cancel := context.WithCancel(ctx)
	r.scopes[scope] = &hostSpeculationScope{
		ctx: scopeCtx, cancel: cancel, turn: turn, client: client,
		allowed: make(map[string]protocol.SpeculationCall),
		started: make(map[string]struct{}),
		tokens:  make(map[string]*hostSpeculationToken),
	}
	return true
}

func (r *hostSpeculationRuntime) allow(scope protocol.SpeculationScope, call protocol.SpeculationCall) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	active := r.scopes[scope]
	if active == nil {
		return false
	}
	if _, started := active.started[call.ID]; started {
		return false
	}
	call.Arguments = append(json.RawMessage(nil), call.Arguments...)
	active.allowed[call.ID] = call
	return true
}

type toolHookEnabledReporter interface {
	Enabled() bool
}

func toolHooksEnabled(hooks ToolHooks) bool {
	if hooks == nil {
		return false
	}
	if reporter, ok := hooks.(toolHookEnabledReporter); ok {
		return reporter.Enabled()
	}
	return true
}

func (a *Agent) speculationPreauthorized(ctx context.Context, turn *turnRuntime, candidate tool.Tool, call protocol.SpeculationCall, gate Gate) bool {
	if toolHooksEnabled(a.svc.hooks) {
		return false
	}
	if a.svc.extensions != nil && (a.svc.extensions.HasInterceptors(extension.PointToolBefore) ||
		a.svc.extensions.HasInterceptors(extension.PointPermissionDecision) ||
		a.svc.extensions.HasReplacement(extension.SlotPermission)) {
		return false
	}
	arguments := append(json.RawMessage(nil), call.Arguments...)
	plan := &toolCallPlan{
		call: provider.ToolCall{ID: call.ID, Name: call.Name, Arguments: string(arguments)},
		tool: candidate, canonicalName: call.Name, permName: call.Name, permArgs: arguments,
		execTool: candidate, execArgs: arguments, evidenceName: call.Name, evidenceArgs: arguments,
		readOnly: candidate.ReadOnly(), runTool: candidate, runArgs: arguments,
	}
	if _, blocked := a.applyPlanModeAndProxy(ctx, plan); blocked {
		return false
	}
	if _, blocked := a.applyResolvedTargetGates(plan); blocked {
		return false
	}
	if _, blocked := a.applyContextualToolGate(ctx, plan); blocked {
		return false
	}
	plan.classifyEffects()
	switch a.pipelineDecision(plan).Action {
	case runtimepolicy.GuardAsk, runtimepolicy.GuardDeny:
		return false
	}
	if gate == nil {
		return true
	}
	speculationGate, ok := gate.(SpeculationGate)
	return ok && speculationGate.SpeculationAllowed(plan.permName, plan.permArgs, plan.readOnly)
}

func (r *hostSpeculationRuntime) start(a *Agent, params protocol.HostSpeculationStartParams) protocol.HostSpeculationStartResult {
	r.mu.Lock()
	active := r.scopes[params.Scope]
	if active == nil || active.closing || !sameObservedSpeculationCall(active.allowed[params.Call.ID], params.Call) {
		r.mu.Unlock()
		return protocol.HostSpeculationStartResult{Reason: "stale, closing, or unobserved speculation call"}
	}
	if _, alreadyStarted := active.started[params.Call.ID]; alreadyStarted {
		r.mu.Unlock()
		return protocol.HostSpeculationStartResult{Reason: "speculation call was already started"}
	}
	// Consume the observation before any extensible policy work. Duplicate
	// reverse requests then fail without serializing unrelated scopes behind it.
	active.started[params.Call.ID] = struct{}{}
	scopeCtx, turn, client := active.ctx, active.turn, active.client
	r.mu.Unlock()

	if a.svc.tools == nil {
		return protocol.HostSpeculationStartResult{Reason: "tool registry is unavailable"}
	}
	candidate, ok := a.svc.tools.Get(params.Call.Name)
	if !ok {
		return protocol.HostSpeculationStartResult{Reason: "tool is unavailable"}
	}
	select {
	case hostSpeculationSlots <- struct{}{}:
	default:
		return protocol.HostSpeculationStartResult{Reason: "host speculation execution limit reached"}
	}
	releaseSlot := func() { <-hostSpeculationSlots }
	if !candidate.ReadOnly() {
		releaseSlot()
		return protocol.HostSpeculationStartResult{Reason: "tool is not read-only"}
	}
	speculative, ok := candidate.(tool.SpeculativeTool)
	if !ok {
		releaseSlot()
		return protocol.HostSpeculationStartResult{Reason: "tool has not opted into speculative execution"}
	}
	policy, eligible := speculative.SpeculationPolicy(params.Call.Arguments)
	if !eligible || !policy.Pure || policy.Deterministic != params.Reusable {
		releaseSlot()
		return protocol.HostSpeculationStartResult{Reason: "tool speculation policy rejected this call"}
	}
	gate, releaseGate := a.svc.acquireGateSnapshot()
	if !a.speculationPreauthorized(scopeCtx, turn, candidate, params.Call, gate) {
		releaseGate()
		releaseSlot()
		return protocol.HostSpeculationStartResult{Reason: "tool call is not preauthorized for speculation"}
	}

	r.mu.Lock()
	activeNow := r.scopes[params.Scope]
	if activeNow != active || active.closing {
		r.mu.Unlock()
		releaseGate()
		releaseSlot()
		return protocol.HostSpeculationStartResult{Reason: "speculation scope closed during admission"}
	}
	r.next++
	scopeDigest := sha256.Sum256([]byte(fmt.Sprintf("%d\x00%s\x00%s\x00%s", params.Scope.Generation,
		params.Scope.SessionID, params.Scope.TurnID, params.Scope.AttemptID)))
	handle := fmt.Sprintf("spec-%x-%d", scopeDigest[:12], r.next)
	execCtx, cancel := context.WithCancel(scopeCtx)
	token := &hostSpeculationToken{
		call: protocol.SpeculationCall{
			ID: params.Call.ID, Name: params.Call.Name,
			Arguments: append(json.RawMessage(nil), params.Call.Arguments...),
		},
		reusable: params.Reusable, cancel: cancel, done: make(chan struct{}),
	}
	active.tokens[handle] = token
	r.mu.Unlock()

	hostSpeculationLaunch{
		agent: a, candidate: candidate, turn: turn, client: client,
		scope: params.Scope, handle: handle, token: token, execCtx: execCtx,
		releaseGate: releaseGate, releaseSlot: releaseSlot,
	}.run()
	return protocol.HostSpeculationStartResult{Accepted: true, Handle: handle}
}

func (r *hostSpeculationRuntime) cancel(scope protocol.SpeculationScope, handle string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	active := r.scopes[scope]
	if active == nil {
		return false
	}
	token := active.tokens[handle]
	if token == nil || token.claims > 0 {
		return false
	}
	delete(active.tokens, handle)
	token.cancel()
	return true
}

func (r *hostSpeculationRuntime) adopt(ctx context.Context, scope protocol.SpeculationScope, handle string, call protocol.SpeculationCall) (string, error, bool) {
	adoption, ok := r.adoptDetailed(ctx, scope, handle, call)
	return adoption.result, adoption.err, ok
}

func (r *hostSpeculationRuntime) adoptDetailed(ctx context.Context, scope protocol.SpeculationScope, handle string, call protocol.SpeculationCall) (speculationAdoption, bool) {
	r.mu.Lock()
	active := r.scopes[scope]
	if active == nil {
		r.mu.Unlock()
		return speculationAdoption{}, false
	}
	token := active.tokens[handle]
	if token == nil || !sameCanonicalSpeculationCall(token.call, call) ||
		(!token.reusable && (token.call.ID != call.ID || token.claims > 0)) {
		r.mu.Unlock()
		return speculationAdoption{}, false
	}
	token.claims++
	r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return speculationAdoption{err: err}, true
	}
	select {
	case <-ctx.Done():
		return speculationAdoption{err: ctx.Err()}, true
	case <-token.done:
		if err := ctx.Err(); err != nil {
			return speculationAdoption{err: err}, true
		}
		return speculationAdoption{
			result: token.result, err: token.err, readEnvelope: token.readEnvelope,
			observations: token.observations, activeMillis: token.activeMillis,
		}, true
	}
}

func (r *hostSpeculationRuntime) close(scope protocol.SpeculationScope) {
	r.mu.Lock()
	active := r.scopes[scope]
	if active != nil && !active.closing {
		active.closing = true
		for handle, token := range active.tokens {
			if token.claims == 0 {
				delete(active.tokens, handle)
				token.cancel()
			}
		}
	}
	r.mu.Unlock()
}

func (r *hostSpeculationRuntime) end(scope protocol.SpeculationScope) {
	r.mu.Lock()
	active := r.scopes[scope]
	delete(r.scopes, scope)
	if active != nil {
		for _, token := range active.tokens {
			if token.claims == 0 {
				token.cancel()
			}
		}
		active.cancel()
	}
	r.mu.Unlock()
}

func sameObservedSpeculationCall(a, b protocol.SpeculationCall) bool {
	return a.ID != "" && a.ID == b.ID && sameCanonicalSpeculationCall(a, b)
}

func sameCanonicalSpeculationCall(a, b protocol.SpeculationCall) bool {
	if a.Name != b.Name {
		return false
	}
	left, leftErr := canonicalSpeculationArguments(a.Arguments)
	right, rightErr := canonicalSpeculationArguments(b.Arguments)
	return leftErr == nil && rightErr == nil && bytes.Equal(left, right)
}

func canonicalSpeculationArguments(raw json.RawMessage) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value map[string]any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("multiple JSON values")
		}
		return nil, err
	}
	return json.Marshal(value)
}

// StartSpeculation implements extension.SpeculationHost for the bound sidecar.
func (a *Agent) StartSpeculation(_ context.Context, params protocol.HostSpeculationStartParams) (protocol.HostSpeculationStartResult, error) {
	if a == nil {
		return protocol.HostSpeculationStartResult{Reason: "agent unavailable"}, nil
	}
	return a.speculation.start(a, params), nil
}

// CancelSpeculation implements extension.SpeculationHost for the bound sidecar.
func (a *Agent) CancelSpeculation(_ context.Context, params protocol.HostSpeculationCancelParams) (protocol.HostSpeculationCancelResult, error) {
	if a == nil {
		return protocol.HostSpeculationCancelResult{}, nil
	}
	return protocol.HostSpeculationCancelResult{Cancelled: a.speculation.cancel(params.Scope, params.Handle)}, nil
}
