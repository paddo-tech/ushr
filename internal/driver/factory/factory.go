// Package factory builds a Driver from agent config. It exists so the agent and
// `ushr doctor` resolve the same driver, image store and free-space floor from
// the same config — a doctor that measured a different disk than the agent gates
// on would report health the agent doesn't act on.
package factory

import (
	"fmt"

	"github.com/paddo-tech/ushr/internal/config"
	"github.com/paddo-tech/ushr/internal/driver"
	"github.com/paddo-tech/ushr/internal/driver/docker"
	"github.com/paddo-tech/ushr/internal/driver/lume"
	"github.com/paddo-tech/ushr/internal/driver/tart"
)

// New constructs the configured driver. An unset type is tart (macOS was the
// first host shape).
func New(cfg config.DriverConfig) (driver.Driver, error) {
	switch cfg.Type {
	case config.DriverTypeDocker:
		// build_cache unset means on: per-repo warm caches are the point of a
		// persistent host, and the driver degrades to uncached when buildx is
		// missing.
		buildCache := cfg.BuildCache == nil || *cfg.BuildCache
		return docker.New(docker.Options{
			Image:             cfg.Image,
			Capacity:          cfg.Capacity,
			Runtime:           cfg.Runtime,
			Network:           cfg.Network,
			Volumes:           cfg.Volumes,
			ExtraHosts:        cfg.ExtraHosts,
			GroupAdd:          cfg.GroupAdd,
			Memory:            cfg.Memory,
			Entrypoint:        cfg.Entrypoint,
			BuildCache:        buildCache,
			BuildkitImage:     cfg.BuildkitImage,
			BuildCacheGB:      cfg.BuildCacheGB,
			BuildCacheIdleHrs: cfg.BuildCacheIdleHrs,
			StateDir:          cfg.StateDir,
			Exclusive:         cfg.ExclusiveDaemon,
		}), nil
	case config.DriverTypeLume:
		return lume.New(lume.Options{
			BaseImage: cfg.Image,
			Capacity:  cfg.Capacity,
		}), nil
	case config.DriverTypeTart, "":
		return tart.New(tart.Options{
			BaseImage: cfg.Image,
			Capacity:  cfg.Capacity,
		}), nil
	default:
		return nil, fmt.Errorf("unsupported driver type %q", cfg.Type)
	}
}

// MinFreeBytes resolves the free-space floor the admission gate uses: the
// configured value, else the driver's default. Negative disables the gate.
func MinFreeBytes(cfg config.DriverConfig) uint64 {
	if cfg.MinFreeGB < 0 {
		return 0
	}
	return ReclaimFloorBytes(cfg)
}

// ReclaimFloorBytes is the same floor as a reclaim target, and survives a
// negative min_free_gb: an operator who never wants a job refused still wants
// the caches collected, so disabling the gate must not disable upkeep.
func ReclaimFloorBytes(cfg config.DriverConfig) uint64 {
	gb := cfg.MinFreeGB
	if gb <= 0 {
		switch cfg.Type {
		case config.DriverTypeDocker:
			gb = docker.DefaultMinFreeGB
		case config.DriverTypeLume:
			gb = lume.DefaultMinFreeGB
		default:
			gb = tart.DefaultMinFreeGB
		}
	}
	return uint64(gb) << 30
}
