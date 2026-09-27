package docker

import (
	"strings"

	"github.com/paddo-tech/ushr/internal/driver"
)

// statusFromState maps a Docker container `State.Status` to a driver.Status.
// Pre-start and live states are Running; everything terminal (exited, dead) is
// Done. The agent treats Done and Failed identically, so a non-zero runner
// exit needs no special case here.
func statusFromState(state string) driver.Status {
	switch state {
	case "created", "running", "restarting", "paused":
		return driver.StatusRunning
	case "exited", "dead", "removing":
		return driver.StatusDone
	default:
		return driver.StatusUnknown
	}
}

// parseNames turns `docker ps --format {{.Names}}` output into handles.
func parseNames(out []byte) []driver.SlotHandle {
	lines := strings.Split(string(out), "\n")
	handles := make([]driver.SlotHandle, 0, len(lines))
	for _, l := range lines {
		if l = strings.TrimSpace(l); l != "" {
			handles = append(handles, driver.SlotHandle(l))
		}
	}
	return handles
}

// isNotFound reports whether a docker/podman CLI error means the container is
// gone, which Status/Destroy treat as success (the ephemeral runner self-removed
// or was reaped). Docker says "No such container"; podman's inspect/rm instead
// say "no such object" or "no container with name or ID".
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, s := range []string{"no such container", "no such object", "no container with name"} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}
