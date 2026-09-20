package cli

import (
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"

	"reasonix/internal/config"
	"reasonix/internal/control"
	"reasonix/internal/pluginpkg"
)

const specPTCPluginName = "spec-ptc"

type speculationOwnerReporter interface {
	SpeculationOwner() string
}

// runSptcCommand toggles the installed spec-ptc package and refreshes the
// extension runtime without replacing the current session.
func (m *chatTUI) runSptcCommand(input string) tea.Cmd {
	args := tokenizeArgs(input)
	if len(args) > 2 {
		m.notice("usage: /sptc [on|off|status]")
		return nil
	}
	action := "status"
	if len(args) == 2 {
		action = strings.ToLower(args[1])
	}
	if action != "on" && action != "off" && action != "status" {
		m.notice("usage: /sptc [on|off|status]")
		return nil
	}

	installed, found, err := pluginpkg.FindInstalled(config.ReasonixHomeDir(), specPTCPluginName)
	if err != nil {
		m.notice("sPTC: " + err.Error())
		return nil
	}
	if !found {
		m.notice(`sPTC is not installed; install plugin "spec-ptc" first`)
		return nil
	}
	if action == "status" {
		m.notice(m.sptcStatus(installed.Enabled))
		return nil
	}
	if !m.sptcToggleReady() {
		return nil
	}

	enabled := action == "on"
	owner, runtimeKnown := m.sptcRuntimeOwner()
	runtimeMatches := runtimeKnown && ((enabled && owner == specPTCPluginName) || (!enabled && owner != specPTCPluginName))
	if installed.Enabled == enabled && runtimeMatches {
		m.notice("sPTC is already " + action)
		return nil
	}
	if installed.Enabled != enabled {
		if err := pluginpkg.SetEnabled(config.ReasonixHomeDir(), specPTCPluginName, enabled); err != nil {
			m.notice("sPTC: " + err.Error())
			return nil
		}
	}

	verb := "disabled"
	if enabled {
		verb = "enabled"
	}
	m.notice("sPTC " + verb + " — refreshing runtime")
	forceFull := installed.Enabled == enabled
	return validateSptcReload(m.scheduleRuntimeReloadWithForce(forceFull), enabled)
}

func validateSptcReload(cmd tea.Cmd, enabled bool) tea.Cmd {
	if cmd == nil {
		return nil
	}
	return func() tea.Msg {
		result := cmd()
		msg, ok := result.(modelSwitchMsg)
		if !ok || msg.err != nil || msg.ctrl == nil {
			return result
		}
		owner, known := speculationOwner(msg.ctrl)
		switch {
		case !known && enabled:
			msg.successNotice = "sPTC is configured enabled, but runtime state could not be verified"
		case !known:
			msg.successNotice = "sPTC is configured disabled, but runtime state could not be verified"
		case enabled && owner == specPTCPluginName:
			msg.successNotice = "sPTC enabled and active"
		case enabled && owner != "":
			msg.successNotice = fmt.Sprintf("sPTC is configured enabled, but runtime inactive (speculation owned by %s)", owner)
		case enabled:
			msg.successNotice = "sPTC is configured enabled, but runtime inactive — check the plugin and retry /sptc on"
		case owner != specPTCPluginName:
			msg.successNotice = "sPTC disabled and inactive"
		default:
			msg.successNotice = "sPTC is configured disabled, but remains active — retry /sptc off"
		}
		return msg
	}
}

func (m *chatTUI) sptcToggleReady() bool {
	if m == nil {
		return false
	}
	if m.ctrl == nil || m.rebuildRuntime == nil {
		m.notice("sPTC toggle unavailable in this session")
		return false
	}
	if _, ok := m.ctrl.(*control.Controller); !ok {
		m.notice("sPTC toggle unavailable in this session")
		return false
	}
	if m.runtimeSwitchBusy() {
		m.notice("finish or cancel active work and stop background jobs before changing sPTC")
		return false
	}
	if m.modelSwitchPending {
		m.notice("wait for the current runtime switch to finish")
		return false
	}
	return true
}

func (m *chatTUI) sptcStatus(enabled bool) string {
	configured := "disabled"
	if enabled {
		configured = "enabled"
	}
	owner, known := m.sptcRuntimeOwner()
	runtime := "unavailable"
	if known {
		switch owner {
		case specPTCPluginName:
			runtime = "active"
		case "":
			runtime = "inactive"
		default:
			runtime = fmt.Sprintf("inactive (speculation owned by %s)", owner)
		}
	}
	return fmt.Sprintf("sPTC: configured %s; runtime %s", configured, runtime)
}

func (m *chatTUI) sptcRuntimeOwner() (string, bool) {
	if m == nil {
		return "", false
	}
	return speculationOwner(m.ctrl)
}

func speculationOwner(ctrl control.SessionAPI) (string, bool) {
	if ctrl == nil {
		return "", false
	}
	reporter, ok := ctrl.(speculationOwnerReporter)
	if !ok {
		return "", false
	}
	return reporter.SpeculationOwner(), true
}
