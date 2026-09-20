package extension

import (
	"context"

	"reasonix/internal/extension/protocol"
)

// SpeculationHost is the transport-independent host-owned execution seam used
// by the single SlotSpeculation owner. The active agent implements it; sidecar
// transport only forwards the two reverse RPCs.
type SpeculationHost interface {
	StartSpeculation(context.Context, protocol.HostSpeculationStartParams) (protocol.HostSpeculationStartResult, error)
	CancelSpeculation(context.Context, protocol.HostSpeculationCancelParams) (protocol.HostSpeculationCancelResult, error)
}
