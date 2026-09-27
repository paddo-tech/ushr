package domain

import "testing"

func TestLabelsSatisfied(t *testing.T) {
	if !LabelsSatisfied([]string{"self-hosted", "linux"}, []string{"self-hosted", "Linux", "X64"}) {
		t.Fatal("case-insensitive subset should match")
	}
	if LabelsSatisfied([]string{"gpu"}, []string{"self-hosted"}) {
		t.Fatal("missing label must not match")
	}
	// A GitHub-hosted job: no self-hosted runner advertises "ubuntu-latest", so
	// no agent should ever carry one in its queue.
	if LabelsSatisfied([]string{"ubuntu-latest"}, []string{"self-hosted", "Linux", "X64"}) {
		t.Fatal("a GitHub-hosted label must not match a self-hosted runner")
	}
}
