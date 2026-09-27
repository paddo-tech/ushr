package tart

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/paddo-tech/ushr/internal/driver"
)

// listEntry mirrors the JSON shape of `tart list --format json`.
type listEntry struct {
	Name   string `json:"Name"`
	Source string `json:"Source"`
	State  string `json:"State"`
}

// parseList returns local VMs whose name starts with prefix.
func parseList(jsonData []byte, prefix string) ([]driver.SlotHandle, error) {
	var entries []listEntry
	if err := json.Unmarshal(jsonData, &entries); err != nil {
		return nil, fmt.Errorf("parse tart list output: %w", err)
	}
	var out []driver.SlotHandle
	for _, e := range entries {
		if e.Source == "local" && strings.HasPrefix(e.Name, prefix) {
			out = append(out, driver.SlotHandle(e.Name))
		}
	}
	return out, nil
}

// parseVMState looks up a single VM by name and maps its Tart state to a Driver Status.
// Returns (StatusUnknown, error) if the VM is not present.
func parseVMState(jsonData []byte, name string) (driver.Status, error) {
	var entries []listEntry
	if err := json.Unmarshal(jsonData, &entries); err != nil {
		return driver.StatusUnknown, fmt.Errorf("parse tart list output: %w", err)
	}
	for _, e := range entries {
		if e.Source == "local" && e.Name == name {
			switch e.State {
			case "running":
				return driver.StatusRunning, nil
			case "stopped":
				// VM stopped without us asking — treat as terminal. Caller decides if
				// it's Done (clean shutdown) or Failed (crashed). At this layer we can't
				// distinguish, so report Done; richer signals come from the runner log.
				return driver.StatusDone, nil
			default:
				return driver.StatusUnknown, nil
			}
		}
	}
	return driver.StatusUnknown, fmt.Errorf("vm %q not found", name)
}
