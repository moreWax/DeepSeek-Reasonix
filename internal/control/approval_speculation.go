package control

import (
	"encoding/json"

	"reasonix/internal/agent"
)

// AcquireSpeculationGate pins and returns the exact inner policy used by one
// speculative host execution. Update waits until the returned release runs.
func (g *SharedHeadlessGate) AcquireSpeculationGate() (agent.Gate, func()) {
	g.mu.RLock()
	return g.gate, g.mu.RUnlock
}

func (g *SharedHeadlessGate) SpeculationAllowed(toolName string, args json.RawMessage, readOnly bool) bool {
	g.mu.RLock()
	gate := g.gate
	g.mu.RUnlock()
	return gate.SpeculationAllowed(toolName, args, readOnly)
}

func (g *freshHumanHeadlessGate) SpeculationAllowed(toolName string, args json.RawMessage, readOnly bool) bool {
	if g == nil || RequiresFreshHumanApprovalTool(toolName) {
		return false
	}
	return g.gate.SpeculationAllowed(toolName, args, readOnly)
}
