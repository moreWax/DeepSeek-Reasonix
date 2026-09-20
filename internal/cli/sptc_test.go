package cli

import (
	"context"
	"errors"
	"strings"
	"testing"

	"reasonix/internal/boot"
	"reasonix/internal/control"
	"reasonix/internal/event"
	"reasonix/internal/pluginpkg"
)

func installSptcForTest(t *testing.T, enabled bool) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("REASONIX_HOME", home)
	if err := pluginpkg.Upsert(home, pluginpkg.InstalledPlugin{
		Name:    specPTCPluginName,
		Root:    "plugins/spec-ptc",
		Enabled: enabled,
	}); err != nil {
		t.Fatal(err)
	}
	return home
}

func TestSptcStatusDistinguishesConfiguredAndRuntimeState(t *testing.T) {
	installSptcForTest(t, true)
	m := newTestChatTUI()
	m.ctrl = newOwnedTestController(t, control.Options{})

	if cmd := m.runSlashCommand("/sptc status"); cmd != nil {
		t.Fatal("/sptc status should render locally")
	}
	out := strings.Join(m.transcript, "\n")
	if !strings.Contains(out, "sPTC: configured enabled; runtime inactive") {
		t.Fatalf("unexpected /sptc status output:\n%s", out)
	}
}

func TestSptcToggleRejectsBusyRuntimeWithoutPersisting(t *testing.T) {
	home := installSptcForTest(t, false)
	ctrl := newOwnedTestController(t, control.Options{})
	rebuilder, calls := stubRuntimeRebuilder(nil, errors.New("must not run"))
	m := reloadTestModel(ctrl, rebuilder)
	m.pendingApproval = &event.Approval{}

	if cmd := m.runSptcCommand("/sptc on"); cmd != nil {
		t.Fatal("busy /sptc on returned a command")
	}
	installed, found, err := pluginpkg.FindInstalled(home, specPTCPluginName)
	if err != nil || !found {
		t.Fatalf("FindInstalled = %+v, %v, %v", installed, found, err)
	}
	if installed.Enabled {
		t.Fatal("busy /sptc on persisted enablement")
	}
	if *calls != 0 {
		t.Fatalf("busy /sptc on rebuilt %d times", *calls)
	}
}

func TestSptcTogglePersistsAndSchedulesRuntimeReload(t *testing.T) {
	home := installSptcForTest(t, false)
	oldCtrl := newOwnedTestController(t, control.Options{Label: "old"})
	newCtrl := newOwnedTestController(t, control.Options{Label: "new"})
	rebuilder, calls := stubRuntimeRebuilder(&boot.BuildResult{Controller: newCtrl}, nil)
	m := reloadTestModel(oldCtrl, rebuilder)

	cmd := m.runSlashCommand("/sptc on")
	if cmd == nil {
		t.Fatal("/sptc on did not schedule a runtime reload")
	}
	installed, found, err := pluginpkg.FindInstalled(home, specPTCPluginName)
	if err != nil || !found || !installed.Enabled {
		t.Fatalf("enabled plugin state = %+v, found=%v, err=%v", installed, found, err)
	}
	msg, ok := cmd().(modelSwitchMsg)
	if !ok {
		t.Fatalf("reload command returned an unexpected message")
	}
	if msg.err != nil || msg.ctrl != newCtrl || msg.oldCtrl != oldCtrl {
		t.Fatalf("reload message = %+v", msg)
	}
	if *calls != 1 {
		t.Fatalf("reload calls = %d, want 1", *calls)
	}
}

func TestSptcSameStateMismatchForcesRetryAndReportsInactive(t *testing.T) {
	installSptcForTest(t, true)
	oldCtrl := newOwnedTestController(t, control.Options{Label: "old"})
	newCtrl := newOwnedTestController(t, control.Options{Label: "new"})
	var gotSpec runtimeRebuildSpec
	rebuilder := func(_ context.Context, spec runtimeRebuildSpec, _ *control.Controller) (*boot.BuildResult, error) {
		gotSpec = spec
		return &boot.BuildResult{Controller: newCtrl}, nil
	}
	m := reloadTestModel(oldCtrl, rebuilder)

	cmd := m.runSptcCommand("/sptc on")
	if cmd == nil {
		t.Fatal("same-state inactive /sptc on did not retry")
	}
	msg, ok := cmd().(modelSwitchMsg)
	if !ok {
		t.Fatal("retry returned an unexpected message")
	}
	if !gotSpec.forceFull {
		t.Fatal("same-state inactive retry did not force a full rebuild")
	}
	if !strings.Contains(msg.successNotice, "runtime inactive") {
		t.Fatalf("retry success notice = %q, want inactive warning", msg.successNotice)
	}
}

func TestSptcMissingAndInvalidCommandsDoNotMutateState(t *testing.T) {
	home := t.TempDir()
	t.Setenv("REASONIX_HOME", home)
	m := newTestChatTUI()

	if cmd := m.runSptcCommand("/sptc sideways"); cmd != nil {
		t.Fatal("invalid /sptc command returned work")
	}
	if cmd := m.runSptcCommand("/sptc on"); cmd != nil {
		t.Fatal("missing plugin /sptc command returned work")
	}
	state, err := pluginpkg.LoadState(home)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Plugins) != 0 {
		t.Fatalf("commands unexpectedly mutated plugin state: %+v", state.Plugins)
	}
	out := strings.Join(m.transcript, "\n")
	for _, want := range []string{"usage: /sptc", "sPTC is not installed"} {
		if !strings.Contains(out, want) {
			t.Fatalf("output missing %q:\n%s", want, out)
		}
	}
}
