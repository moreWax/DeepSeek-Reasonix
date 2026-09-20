package cli

import (
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"

	"reasonix/internal/config"
	"reasonix/internal/pluginpkg"
)

func pluginArgNames() []string {
	names, err := pluginpkg.InstalledNames(config.ReasonixHomeDir())
	if err != nil {
		return nil
	}
	return names
}

func (m *chatTUI) runPluginOrSptcCommand(cmd, input string) tea.Cmd {
	if cmd == "/sptc" {
		return m.runSptcCommand(input)
	}
	m.runPluginSubcommand(input)
	return nil
}

func (m *chatTUI) runPluginSubcommand(input string) {
	args := tokenizeArgs(input)
	sub := ""
	if len(args) > 1 {
		sub = strings.ToLower(args[1])
	}
	switch sub {
	case "", "list", "ls":
		text, err := pluginpkg.InstalledListText(config.ReasonixHomeDir())
		if err != nil {
			m.notice("plugins: " + err.Error())
			return
		}
		m.commitLine(text)
	case "show", "cat":
		if len(args) < 3 {
			m.notice("usage: /plugins show <name>")
			return
		}
		text, err := pluginpkg.InstalledShowText(config.ReasonixHomeDir(), args[2])
		if err != nil {
			m.notice("plugins: " + err.Error())
			return
		}
		m.commitLine(text)
	default:
		m.notice(fmt.Sprintf("unknown /plugins subcommand %s - try: /plugins or /plugins show <name>", args[1]))
	}
}
