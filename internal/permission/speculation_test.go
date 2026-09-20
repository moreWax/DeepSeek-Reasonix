package permission

import (
	"context"
	"encoding/json"
	"testing"
)

type speculationApproverStub struct {
	preapproved bool
	prompted    bool
}

func (a *speculationApproverStub) Approve(context.Context, string, string, json.RawMessage) (bool, bool, error) {
	a.prompted = true
	return true, false, nil
}
func (a *speculationApproverStub) SpeculationPreapproved(string, string, json.RawMessage) bool {
	return a.preapproved
}

func TestGateSpeculationAllowedNeverPrompts(t *testing.T) {
	arguments := json.RawMessage(`{"path":"x"}`)
	approver := &speculationApproverStub{}
	gate := NewGate(New("allow", nil, []string{"read_file"}, nil), approver)
	if gate.SpeculationAllowed("read_file", arguments, true) {
		t.Fatal("unapproved Ask decision was speculatively allowed")
	}
	if approver.prompted {
		t.Fatal("speculation preauthorization invoked Approve")
	}
	approver.preapproved = true
	if !gate.SpeculationAllowed("read_file", arguments, true) {
		t.Fatal("recorded preapproval was not honored")
	}
	deny := NewGate(New("allow", nil, nil, []string{"read_file"}), approver)
	if deny.SpeculationAllowed("read_file", arguments, true) {
		t.Fatal("explicit deny was speculatively allowed")
	}
}

func TestGateSpeculationAllowedMatchesAutonomousNilApprover(t *testing.T) {
	gate := NewGate(New("allow", nil, []string{"read_file"}, nil), nil)
	if !gate.SpeculationAllowed("read_file", json.RawMessage(`{"path":"x"}`), true) {
		t.Fatal("nil-approver autonomous call should be preauthorized")
	}
}
