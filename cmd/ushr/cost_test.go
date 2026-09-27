package main

import (
	"testing"
	"time"

	"github.com/paddo-tech/ushr/internal/ledger"
)

func TestAggregate(t *testing.T) {
	since := time.Unix(1000, 0)
	min := func(n int) time.Duration { return time.Duration(n) * time.Minute }
	base := since.Add(time.Hour)
	records := []ledger.Record{
		{JobID: 1, Org: "acme", StartedAt: base, CompletedAt: base.Add(min(2))},                        // 2 min
		{JobID: 1, Org: "acme", StartedAt: base, CompletedAt: base.Add(min(2))},                        // dup job id
		{JobID: 2, Org: "acme", StartedAt: base, CompletedAt: base.Add(90 * time.Second)},              // 1.5 -> ceil 2
		{JobID: 3, Org: "acme", StartedAt: base, CompletedAt: base},                                    // zero duration
		{JobID: 4, Org: "old", StartedAt: since.Add(-time.Hour), CompletedAt: since.Add(-time.Minute)}, // before cutoff
		{JobID: 5, Org: "beta", StartedAt: base, CompletedAt: base.Add(min(10))},                       // 10 min
	}
	rows := aggregate(records, since)
	if len(rows) != 2 {
		t.Fatalf("got %d orgs, want 2 (acme, beta): %+v", len(rows), rows)
	}
	if rows[0].org != "acme" || rows[0].jobs != 2 || rows[0].minutes != 4 {
		t.Errorf("acme = %+v, want jobs=2 minutes=4", rows[0])
	}
	if rows[1].org != "beta" || rows[1].jobs != 1 || rows[1].minutes != 10 {
		t.Errorf("beta = %+v, want jobs=1 minutes=10", rows[1])
	}
}
