# Behavioral parity evidence

Reference implementation: [`alexzhang13/spec-ptc`](https://github.com/alexzhang13/spec-ptc) at `9b78b7d6ceeaf8afd1557c4e3a999ce653fc0e17`.

The Python reference shadows partially generated Python programs. This extension applies the same scheduling semantics to partially generated Go programs executed by `rlm-go`. The real REPL is always authoritative.

| Behavior | Extension implementation | Focused evidence |
| --- | --- | --- |
| Dispatch before code-block closure | Incremental `StreamSegmenter` and `StreamBridge` | `TestStreamBridgeDispatchesLoopBeforeCodeBlockCloses`, `TestRLMRuntimeOverlapsRootStreamingAndClaimsInRealREPL` |
| Literal, live-variable, and lexical-scope arguments | Inert AST evaluator with Go block/range/if shadow restoration | `TestPlannerResolvesLiteralAndLiveVariables`, `TestPlannerRestoresLexicallyShadowedValues` |
| Branch selection | Safely evaluated `if` conditions | `TestPlannerResolvesOnlyTakenBranch` |
| Loop unrolling from an open suffix | Bounded `range` evaluation and repaired tails | `TestPlannerUnrollsOpenRangeLoop`, `TestStreamBridgeDispatchesLoopBeforeCodeBlockCloses` |
| Changed-program retraction | Stable streamed IDs plus scheduler detach/cancel | `TestStreamBridgeRetractsLoopOvershootWhenBreakArrives`, `TestRetractCancelsInvalidatedUnclaimedBet` |
| Temporary invalid syntax | Prior bets retained until the stream proves them invalid | `TestStreamBridgePreservesBetsAcrossTemporarilyInvalidSuffix` |
| Dependent/judge calls | Completed speculative values resume shadow planning | `TestPlannerResumesDependentCallFromCompletedSpeculation`, `TestRLMRuntimeResumesDependentSpeculationBeforeRootStreamCloses` |
| Cross-iteration live namespace | Safe shadow state plus authenticated inert-value container persistence | `TestStreamBridgeCarriesShadowNamespaceAcrossBlocks`; fork `TestContainerProgramPersistsTopLevelVariables` |
| Batched calls | Source-ordered reservations, then concurrent waits; one independently claimable plan per element | `TestRLMToolPoliciesExpandSyncAndAsyncBatches`, `TestClaimingRLMClientClaimsBatchedElementsIndependently`, `TestClaimingRLMClientReservesDuplicateBatchBeforeConcurrentWaits` |
| Async calls | Source-ordered `QueryAsync` reservation before concurrent resolution in real and container REPLs | `TestRealRLMGoREPLClaimsAsyncSpeculation`, fork `TestQueryAsyncReservesInInvocationOrder`, `TestContainerPreludeExposesAsyncParity` |
| Non-deterministic multiplicity | FIFO occurrence queue | `TestNondeterministicCallsClaimIndependentFIFOExecutions`, `TestPlannerPreservesMultiplicityAndDeterministicPolicy` |
| Deterministic reuse | Explicit policy, one execution with multiple references | `TestDeterministicCallsShareOneExecution`, `TestPlannerPreservesMultiplicityAndDeterministicPolicy` |
| Speculative failure | Failed result is not adopted; authoritative call reruns | `TestFailedSpeculationFallsBackToAuthoritativeExecution` |
| Budget rejection and fail-open | Shared external-call, batch, prompt, concurrency, IPC-frame/execution/connection/async-handle, call-ledger, in-flight, byte, and dispatch bounds | `TestGuardedRLMClientLimitsEveryExternalCall`, `TestClaimingRLMClientRejectsHostileBatchAndPrompt`; fork `TestIPCFrameReaderRejectsOversizedFrames`, `TestIPCExecutionQuotaAndCallLedgerAreBounded`, `TestIPCConnectionLimitQueuesInsteadOfDropping`; engine budget tests |
| Cancellation and stale isolation | Scope-owned contexts, opaque handles, generation fencing, and call-ID-aware reservation rollback | `TestRetractDoesNotEvictReservedDeterministicClaim`, `TestRetractDuringReservationSurvivesRollbackAndReplacement`; engine cancellation, stale-scope, and concurrent cleanup tests |
| Waste accounting | Ready but unclaimed work counted separately | `TestReadyUnclaimedExecutionIsCountedAsWaste` |
| Observable status | Live structured-UI cache/inline lanes plus provider reasoning metrics for dispatch/hit/miss/waste/cancel, measured authoritative wait, and saved overlap | `TestUITraceRendersSpeculationAndAuthoritativeLanes`, `TestQueryTraceReportsLiveHitAndSavedLatency`, `TestSpecPTCProviderStreamsReasoningFinalUsageAndDone`; host `TestExtensionCardPublicationReplacesLiveSurface` |
| Execution mode | Constrained Podman/Docker worker when available; automatic policy-checked local Yaegi fallback otherwise | `TestConstrainedSandboxRejectsUnsafeProfiles`, `TestGeneratedCodePolicyRejectsDirectNetworkAndIPC`, `TestLocalExecutionAutomaticallyFollowsContainerAvailability` |
| Subscription transport | Existing Reasonix/Codex OAuth credential, private import/refresh, Codex Responses SSE, and `gpt-5.6-sol` default | `TestManagerImportsCodexCredentialPrivately`, `TestManagerRefreshesExpiredCredential`, `TestClientStreamsCodexResponseWithSubscriptionHeaders`, `TestDefaultRLMRootModelUsesExistingCodexSubscription` |

## Safety differences from the Python reference

These are intentional safety adaptations, not semantic substitutions:

- Unsupported or uncertain Go expressions are tainted instead of executed in the shadow planner.
- Model-generated authoritative code runs in the constrained container worker when Podman/Docker is available. Without either runtime, it automatically uses the local Yaegi REPL; this preserves usability but deliberately trades away OS isolation, as documented in `README.md`.
- Native Reasonix tool speculation remains host-owned, so ordinary permission, hook, sandbox, and adoption checks still apply.
- Every speculative failure, rejection, or miss falls open to the authoritative path.
