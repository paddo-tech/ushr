package vmdriver

import (
	"context"
	"testing"

	"github.com/paddo-tech/ushr/internal/driver"
)

type fakeCmds struct {
	vms     []driver.SlotHandle
	store   string
	deleted []string
	pruned  []int
}

func (f *fakeCmds) Clone(context.Context, string, string) error { return nil }
func (f *fakeCmds) Start(string) error                          { return nil }
func (f *fakeCmds) Stop(context.Context, string) error          { return nil }
func (f *fakeCmds) Delete(_ context.Context, name string) error {
	f.deleted = append(f.deleted, name)
	return nil
}
func (f *fakeCmds) List(context.Context, string) ([]driver.SlotHandle, error) { return f.vms, nil }
func (f *fakeCmds) State(context.Context, string) (driver.Status, string, error) {
	return driver.StatusRunning, "", nil
}
func (f *fakeCmds) IP(context.Context, string) (string, error) { return "", nil }
func (f *fakeCmds) StorePath() string                          { return f.store }
func (f *fakeCmds) PruneCaches(_ context.Context, olderThanDays int) error {
	f.pruned = append(f.pruned, olderThanDays)
	return nil
}

// Reclaim on a VM host touches only pulled-image caches: deleting a VM is the
// orphan reconciler's job, and doing it here could take a running runner.
func TestReclaimTiersOnlyPruneCaches(t *testing.T) {
	f := &fakeCmds{vms: []driver.SlotHandle{"ushr-a-1", "ushr-b-2"}, store: t.TempDir()}
	d := New(f, Config{})

	tiers := d.ReclaimTiers(context.Background())
	for _, tier := range tiers {
		if tier.Drain {
			t.Fatalf("tier %q wants a drain it doesn't need", tier.Name)
		}
		if err := tier.Run(); err != nil {
			t.Fatal(err)
		}
	}

	if len(f.deleted) != 0 {
		t.Fatalf("reclaim deleted VMs %v", f.deleted)
	}
	if len(f.pruned) != 2 || f.pruned[0] != staleCacheDays || f.pruned[1] != 0 {
		t.Fatalf("cache prunes %v, want stale-first then a full flush", f.pruned)
	}
}
