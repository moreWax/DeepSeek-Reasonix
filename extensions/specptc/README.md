# RLM + spec-PTC for Reasonix

This installable extension combines two paths:

1. `plugin/spec-ptc/rlm` is an extension-hosted [`rlm-go`](https://github.com/XiaoConstantine/rlm-go) provider. While the root model is still streaming Go REPL code, a shadow planner discovers eligible `Query` calls and starts them before the code block closes. The authoritative REPL then claims matching results instead of issuing duplicate sub-model calls.
2. The `speculation` strategy retains Reasonix's host-owned speculation path for ordinary structured tool calls. Host resolution, permissions, hooks, sandboxing, and adoption checks remain authoritative.

The scheduler semantics follow [`alexzhang13/spec-ptc`](https://github.com/alexzhang13/spec-ptc) commit `9b78b7d6ceeaf8afd1557c4e3a999ce653fc0e17`. The RLM substrate is pinned to `XiaoConstantine/rlm-go` commit `43905b967530d59be5b44f43be45b8f4d1a54146`. See [`PARITY.md`](PARITY.md) for the behavior/evidence matrix and [`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md) for licensing.

## Behavior

The RLM path supports:

- partial fenced `go` and `repl` stream parsing before fence closure;
- literal arguments and safely copied live variables;
- resolved branches and bounded `range` loop unrolling;
- synchronous, asynchronous, and batched `Query` calls;
- dependent-call resumption as speculative results arrive;
- safe shadow namespace carry-over between RLM iterations;
- FIFO multiplicity for non-deterministic calls and opt-in deterministic reuse;
- retraction and cancellation when later streamed code invalidates a bet;
- worker, in-flight, byte, dispatch, iteration, token, and wall-time budgets;
- fail-open authoritative execution after misses, rejected bets, or speculative failures;
- per-turn `dispatched`, `hits`, `misses`, `wasted`, `evictions`, and `cancelled` metrics in the provider reasoning stream.

The shadow planner only evaluates an inert Go subset. Unknown expressions taint dependent values and suppress speculation; they never suppress authoritative execution.

## Build and install

A Reasonix build with Extension Protocol v2 provider and speculation support is required.

```bash
cd extensions/specptc
make test
make race
make build
reasonix plugin install "$PWD" --link --replace --yes
```

The manifest launches `bin/reasonix-spec-ptc`. No platform-specific executable is stored in source control: `make build` produces a `-trimpath -buildvcs=false` binary for the current `GOOS/GOARCH`, and must run on each target platform before installation. Re-run it after source changes. `--link` is intended for development; omit it when packaging the locally built extension directory.

## RLM provider configuration

When a Reasonix Codex subscription credential (`$REASONIX_HOME/codex-auth.json`, normally `~/.reasonix/codex-auth.json`) or Codex CLI credential (`~/.codex/auth.json`) is present, the extension automatically uses the subscription-backed `gpt-5.6-sol` model for both root and sub-model calls. It shares the existing credential format, imports the Codex CLI credential into Reasonix storage when needed, and refreshes short-lived OAuth tokens without requiring an API key:

```bash
# No provider environment variables are needed after Reasonix or Codex login.
reasonix --model plugin/spec-ptc/rlm
```

The extension does not invoke the Codex CLI agent and does not require any Reasonix core changes; its extension-local transport calls the same Codex Responses endpoint using the existing subscription credential.

API-key providers remain available explicitly:

```bash
export REASONIX_RLM_ROOT_PROVIDER=openai
export REASONIX_RLM_SUB_PROVIDER=openai
export REASONIX_RLM_ROOT_MODEL=gpt-5-mini
export REASONIX_RLM_SUB_MODEL=gpt-5-mini
export OPENAI_API_KEY=...
```

Supported provider selectors are `codex`, `openai`, `anthropic`, and `gemini`. Model-name detection is automatic for known `rlm-go` and Codex subscription models; override it when needed:

- `REASONIX_RLM_ROOT_PROVIDER`
- `REASONIX_RLM_SUB_PROVIDER`

API-key providers use `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, or `GEMINI_API_KEY`. The `codex` selector uses the existing OAuth subscription credential instead. `REASONIX_RLM_CODEX_EFFORT` controls Codex reasoning effort and defaults to `high`.

Additional RLM settings:

- `REASONIX_RLM_MAX_ITERATIONS`: root REPL iterations; default 30.
- `REASONIX_RLM_TIMEOUT_SECONDS`: wall-time limit; default 300.
- `REASONIX_RLM_MAX_TOKENS`: optional per-completion aggregate root/sub-model accounting ceiling, checked between RLM iterations; disabled by default. Pre-call containment is provided by the query-count, prompt-size, batch, and process-wide concurrency limits below.
- `REASONIX_RLM_MAX_BATCH`: maximum prompts in one authoritative batch; default 64.
- `REASONIX_RLM_MAX_PROMPT_BYTES`: per-prompt and aggregate batch byte limit; default 1 MiB.
- `REASONIX_RLM_MAX_CONCURRENT_QUERIES`: process-wide and per-turn authoritative sub-model concurrency; default 8.
- `REASONIX_RLM_MAX_QUERIES`: authoritative calls admitted per turn; default 256.
- `REASONIX_RLM_TRUSTED_IN_PROCESS`: force local in-process execution even when Podman/Docker is available. Local execution is selected automatically when neither container runtime is installed.

Request-scoped `Temperature`, `MaxTokens`, and `ResponseFormat` are rejected because the pinned `rlm-go` provider clients cannot enforce their wire semantics per completion; they are never silently ignored. Use the completion-wide aggregate token accounting ceiling above together with the pre-call query limits for cost containment.

### Execution isolation

When Podman or Docker is available, model-generated code executes in the constrained container worker. Without either runtime, the extension now falls back automatically to the local Yaegi REPL so the provider remains usable on a standard Reasonix installation. **This fallback has no OS isolation: model-generated Go executes inside the extension process.** The extension still applies generated-code policy checks and query/iteration/time budgets, but those are not a security boundary equivalent to a container. Install Podman/Docker to restore container isolation, or set `REASONIX_RLM_TRUSTED_IN_PROCESS=true` to force the same local behavior even when a container runtime exists.

In container mode, the extension-local `rlm-go` fork preserves `--network none` and mounts a mode-restricted Unix socket as the only host transport for `Query`; it never adds a host gateway. The extension also validates every completed code block before execution and rejects direct references to the generated program's `net` package or private IPC connection. Generated source can reach a model only through the bounded `Query` APIs.

The container worker receives only its generated temporary source directory and Unix IPC directory, has 256 MiB/1 CPU/60 second execution caps, bounded stdout/stderr and IPC frames, and receives no workspace mount. Each execution is also capped at 512 IPC query messages and 16 MiB of aggregate request frames; at most 16 IPC connections are actively handled while up to 64 async handles may queue. The host retains at most 1024 truncated call records. Persisted REPL values use a token-authenticated marker and are reinserted only after host-side inert-literal validation.

## Speculation budgets

These limits apply to both the RLM scheduler and the structured-tool strategy:

- `REASONIX_SPEC_PTC_WORKERS`: start/cancel worker count; default `max(8, 4*GOMAXPROCS)`.
- `REASONIX_SPEC_PTC_MAX_INFLIGHT`: global active execution limit; default 64.
- `REASONIX_SPEC_PTC_MAX_DISPATCHES`: admission limit per turn; default 2048.
- `REASONIX_SPEC_PTC_MAX_ARGUMENT_BYTES`: global in-flight argument budget; default 8 MiB.

## Structured Reasonix tools

`REASONIX_SPEC_PTC_TOOLS` configures the separate host-owned structured-tool path. It defaults to `read_file`. Add `:deterministic` only when the host tool policy also permits deterministic reuse, or use `none` to disable this path.

The default excludes editor overlays, `glob`, `grep`, `ls`, network tools, shell tools, MCP tools, and writers because their dependencies or cancellation behavior cannot be safely validated at adoption. Failures, overload, stale scopes, rejected tools, and missing extension support all fall back to ordinary Reasonix execution.
