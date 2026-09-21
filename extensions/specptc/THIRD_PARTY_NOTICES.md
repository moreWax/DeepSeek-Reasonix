# Third-party notices

The speculation identity, FIFO multiplicity, deterministic reuse, dispatch and
claim, budget, cancellation, eviction, and fail-open semantics are derived from
[alexzhang13/spec-ptc](https://github.com/alexzhang13/spec-ptc).

Copyright (c) 2026 Alex Zhang

Licensed under the MIT License. The upstream license is reproduced in
[`LICENSE`](LICENSE).

The streamed Go RLM integration uses an audited extension-local fork of
[`XiaoConstantine/rlm-go`](https://github.com/XiaoConstantine/rlm-go), based on
commit `43905b967530d59be5b44f43be45b8f4d1a54146`. The patches under
`third_party/rlm-go` add network-disabled Unix-socket IPC, bounded frames,
execution quotas, connection/call-ledger limits, bounded concurrent container
async APIs, and authenticated inert-value persistence between container executions.

Copyright (c) 2026 Xiao Constantine

Licensed under the MIT License. The copyright and permission notice above
apply to the portions derived from or linked against `rlm-go`.

`rlm-go` uses the following transitive modules in the shipped extension binary:

- The Go runtime and standard library, Copyright 2009 The Go Authors, licensed
  under the Go BSD-style license. The license is reproduced in
  [`THIRD_PARTY_LICENSES/go-LICENSE`](THIRD_PARTY_LICENSES/go-LICENSE).
- [`traefik/yaegi`](https://github.com/traefik/yaegi) v0.16.1,
  Copyright 2019 Containous SAS and Copyright 2020 Traefik Labs SAS, licensed
  under Apache License 2.0. The license is reproduced in
  [`THIRD_PARTY_LICENSES/yaegi-LICENSE`](THIRD_PARTY_LICENSES/yaegi-LICENSE).
- [`google/uuid`](https://github.com/google/uuid) v1.6.0,
  Copyright (c) 2009,2014 Google Inc., licensed under the 3-clause BSD license.
  The license is reproduced in
  [`THIRD_PARTY_LICENSES/google-uuid-LICENSE`](THIRD_PARTY_LICENSES/google-uuid-LICENSE).
