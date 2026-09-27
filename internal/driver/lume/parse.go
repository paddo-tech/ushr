package lume

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/paddo-tech/ushr/internal/driver"
)

// vmDetails mirrors the JSON shape of `lume ls -f json` (array) and
// `lume get <name> -f json` (single object). Fields are camelCase; lume has
// no Source field (unlike Tart), so local VMs are identified by name prefix.
type vmDetails struct {
	Name      string `json:"name"`
	Status    string `json:"status"`
	IPAddress string `json:"ipAddress"`
}

// parseList returns VMs whose name starts with prefix.
func parseList(jsonData []byte, prefix string) ([]driver.SlotHandle, error) {
	var entries []vmDetails
	if err := json.Unmarshal(jsonData, &entries); err != nil {
		return nil, fmt.Errorf("parse lume ls output: %w", err)
	}
	var out []driver.SlotHandle
	for _, e := range entries {
		if strings.HasPrefix(e.Name, prefix) {
			out = append(out, driver.SlotHandle(e.Name))
		}
	}
	return out, nil
}

// parseVM looks up a single VM by name and maps its lume status to a Driver
// Status, also returning its IP (empty when the VM has no DHCP lease). The IP
// rides along so callers avoid a second `lume get` call. Returns
// (StatusUnknown, "", error) if the VM is not present.
func parseVM(jsonData []byte, name string) (driver.Status, string, error) {
	var entries []vmDetails
	if err := json.Unmarshal(jsonData, &entries); err != nil {
		return driver.StatusUnknown, "", fmt.Errorf("parse lume ls output: %w", err)
	}
	for _, e := range entries {
		if e.Name == name {
			ip := strings.TrimSpace(e.IPAddress)
			switch e.Status {
			case "running":
				return driver.StatusRunning, ip, nil
			case "stopped":
				// VM stopped without us asking — treat as terminal. Caller decides if
				// it's Done (clean shutdown) or Failed (crashed). At this layer we can't
				// distinguish, so report Done; richer signals come from the runner log.
				return driver.StatusDone, ip, nil
			default:
				// provisioning/pulling and anything else: not yet meaningful here.
				return driver.StatusUnknown, ip, nil
			}
		}
	}
	return driver.StatusUnknown, "", fmt.Errorf("vm %q not found", name)
}

// parseIP extracts the DHCP-assigned IP from a `lume get -f json` object.
// Returns "" (no error) when the guest has no lease yet (ipAddress null).
func parseIP(jsonData []byte) (string, error) {
	var d vmDetails
	if err := json.Unmarshal(jsonData, &d); err != nil {
		return "", fmt.Errorf("parse lume get output: %w", err)
	}
	return strings.TrimSpace(d.IPAddress), nil
}
