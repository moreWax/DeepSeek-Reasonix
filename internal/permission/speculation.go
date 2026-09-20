package permission

import "encoding/json"

// SpeculationApprover exposes only already-recorded approval state. It must not
// prompt, mutate grants, or invoke user callbacks.
type SpeculationApprover interface {
	SpeculationPreapproved(toolName, subject string, args json.RawMessage) bool
}

// SpeculationAllowed reports whether Check is guaranteed to allow without
// prompting. It deliberately evaluates current immutable policy and an
// optional read-only preapproval snapshot only; ordinary Check still runs
// before any result is adopted.
func (g *Gate) SpeculationAllowed(toolName string, args json.RawMessage, readOnly bool) bool {
	if g == nil {
		return false
	}
	if canonicalRuleTool(toolName) == "bash" && !readOnly && BashCommandIsReadOnly(args) {
		readOnly = true
	}
	switch g.Policy.Decide(toolName, readOnly, args) {
	case Deny:
		return false
	case Ask:
		if g.Approver == nil {
			return true
		}
		approver, ok := g.Approver.(SpeculationApprover)
		return ok && approver.SpeculationPreapproved(toolName, Subject(args), args)
	default:
		return true
	}
}
