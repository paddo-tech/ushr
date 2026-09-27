package main

import (
	"strings"
	"testing"
)

func TestRewriteWorkflow(t *testing.T) {
	tests := []struct {
		name        string
		in          string
		want        string
		wantChanged int
		wantWarns   int
	}{
		{
			name:        "scalar ubuntu",
			in:          "jobs:\n  build:\n    runs-on: ubuntu-latest\n    steps: []\n",
			want:        "jobs:\n  build:\n    runs-on: [self-hosted, Linux, X64]\n    steps: []\n",
			wantChanged: 1,
		},
		{
			name:        "scalar macos with comment",
			in:          "jobs:\n  build:\n    runs-on: macos-14 # apple silicon\n",
			want:        "jobs:\n  build:\n    runs-on: [self-hosted, macOS, ARM64] # apple silicon\n",
			wantChanged: 1,
		},
		{
			name:        "quoted scalar",
			in:          "jobs:\n  build:\n    runs-on: \"ubuntu-22.04\"\n",
			want:        "jobs:\n  build:\n    runs-on: [self-hosted, Linux, X64]\n",
			wantChanged: 1,
		},
		{
			name:        "ubuntu arm",
			in:          "jobs:\n  build:\n    runs-on: ubuntu-24.04-arm\n",
			want:        "jobs:\n  build:\n    runs-on: [self-hosted, Linux, ARM64]\n",
			wantChanged: 1,
		},
		{
			name:        "flow seq single hosted",
			in:          "jobs:\n  build:\n    runs-on: [macos-latest]\n",
			want:        "jobs:\n  build:\n    runs-on: [self-hosted, macOS, ARM64]\n",
			wantChanged: 1,
		},
		{
			name:        "block seq single hosted",
			in:          "jobs:\n  build:\n    runs-on:\n      - ubuntu-latest\n    steps: []\n",
			want:        "jobs:\n  build:\n    runs-on: [self-hosted, Linux, X64]\n    steps: []\n",
			wantChanged: 1,
		},
		{
			name:        "next-line scalar folds into key line",
			in:          "jobs:\n  build:\n    runs-on:\n      ubuntu-latest\n    steps: []\n",
			want:        "jobs:\n  build:\n    runs-on: [self-hosted, Linux, X64]\n    steps: []\n",
			wantChanged: 1,
		},
		{
			name:        "block seq item comment preserved",
			in:          "jobs:\n  build:\n    runs-on:\n      - ubuntu-latest # keep on linux\n",
			want:        "jobs:\n  build:\n    runs-on: [self-hosted, Linux, X64] # keep on linux\n",
			wantChanged: 1,
		},
		{
			name:      "comment before single block item warns untouched",
			in:        "jobs:\n  build:\n    runs-on:\n      # needs linux\n      - ubuntu-latest\n",
			want:      "jobs:\n  build:\n    runs-on:\n      # needs linux\n      - ubuntu-latest\n",
			wantWarns: 1,
		},
		{
			name: "block seq with comment between items untouched",
			in:   "jobs:\n  build:\n    runs-on:\n      - ubuntu-latest\n      # need a gpu\n      - gpu\n",
			want: "jobs:\n  build:\n    runs-on:\n      - ubuntu-latest\n      # need a gpu\n      - gpu\n",
		},
		{
			name: "runs-on under with is an action input",
			in:   "jobs:\n  call:\n    runs-on: [self-hosted, Linux, X64]\n    steps:\n      - uses: some/action@v1\n        with:\n          runs-on: ubuntu-latest\n",
			want: "jobs:\n  call:\n    runs-on: [self-hosted, Linux, X64]\n    steps:\n      - uses: some/action@v1\n        with:\n          runs-on: ubuntu-latest\n",
		},
		{
			name: "runs-on inside run script untouched",
			in:   "jobs:\n  gen:\n    runs-on: [self-hosted, Linux, X64]\n    steps:\n      - run: |\n          cat > wf.yml <<EOF\n          runs-on: ubuntu-latest\n          EOF\n",
			want: "jobs:\n  gen:\n    runs-on: [self-hosted, Linux, X64]\n    steps:\n      - run: |\n          cat > wf.yml <<EOF\n          runs-on: ubuntu-latest\n          EOF\n",
		},
		{
			name: "already self-hosted untouched",
			in:   "jobs:\n  build:\n    runs-on: [self-hosted, macOS, ARM64]\n",
			want: "jobs:\n  build:\n    runs-on: [self-hosted, macOS, ARM64]\n",
		},
		{
			name: "custom label untouched",
			in:   "jobs:\n  build:\n    runs-on: paddo-runner-mac\n",
			want: "jobs:\n  build:\n    runs-on: paddo-runner-mac\n",
		},
		{
			name:      "matrix expression warns",
			in:        "jobs:\n  build:\n    strategy:\n      matrix:\n        os: [ubuntu-latest]\n    runs-on: ${{ matrix.os }}\n",
			want:      "jobs:\n  build:\n    strategy:\n      matrix:\n        os: [ubuntu-latest]\n    runs-on: ${{ matrix.os }}\n",
			wantWarns: 1,
		},
		{
			name:      "windows warns",
			in:        "jobs:\n  build:\n    runs-on: windows-latest\n",
			want:      "jobs:\n  build:\n    runs-on: windows-latest\n",
			wantWarns: 1,
		},
		{
			name:      "intel macos warns untouched",
			in:        "jobs:\n  build:\n    runs-on: macos-13\n",
			want:      "jobs:\n  build:\n    runs-on: macos-13\n",
			wantWarns: 1,
		},
		{
			name:      "intel macos large tier warns untouched",
			in:        "jobs:\n  build:\n    runs-on: macos-14-large\n",
			want:      "jobs:\n  build:\n    runs-on: macos-14-large\n",
			wantWarns: 1,
		},
		{
			name:        "arm xlarge tier rewritten",
			in:          "jobs:\n  build:\n    runs-on: macos-14-xlarge\n",
			want:        "jobs:\n  build:\n    runs-on: [self-hosted, macOS, ARM64]\n",
			wantChanged: 1,
		},
		{
			name:      "anchored value warns untouched",
			in:        "jobs:\n  a:\n    runs-on: &r ubuntu-latest\n  b:\n    runs-on: *r\n",
			want:      "jobs:\n  a:\n    runs-on: &r ubuntu-latest\n  b:\n    runs-on: *r\n",
			wantWarns: 2,
		},
		{
			name:      "group form warns",
			in:        "jobs:\n  build:\n    runs-on: { group: big-pool }\n",
			want:      "jobs:\n  build:\n    runs-on: { group: big-pool }\n",
			wantWarns: 1,
		},
		{
			name:      "empty runs-on warns",
			in:        "jobs:\n  build:\n    runs-on:\n    steps: []\n",
			want:      "jobs:\n  build:\n    runs-on:\n    steps: []\n",
			wantWarns: 1,
		},
		{
			name:        "multiple jobs",
			in:          "jobs:\n  a:\n    runs-on: ubuntu-latest\n  b:\n    runs-on: macos-15\n",
			want:        "jobs:\n  a:\n    runs-on: [self-hosted, Linux, X64]\n  b:\n    runs-on: [self-hosted, macOS, ARM64]\n",
			wantChanged: 2,
		},
		{
			name:        "crlf preserved",
			in:          "jobs:\r\n  build:\r\n    runs-on: ubuntu-latest\r\n    steps: []\r\n",
			want:        "jobs:\r\n  build:\r\n    runs-on: [self-hosted, Linux, X64]\r\n    steps: []\r\n",
			wantChanged: 1,
		},
		{
			name:      "invalid yaml untouched",
			in:        "jobs:\n  build:\n\truns-on: ubuntu-latest\n",
			want:      "jobs:\n  build:\n\truns-on: ubuntu-latest\n",
			wantWarns: 1,
		},
		{
			name: "top-level runs-on ignored",
			in:   "runs-on: ubuntu-latest\njobs:\n  build:\n    steps: []\n",
			want: "runs-on: ubuntu-latest\njobs:\n  build:\n    steps: []\n",
		},
		{
			name:        "unrelated lines preserved",
			in:          "name: ci\non: push # trigger\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo runs-on ubuntu-latest\n",
			want:        "name: ci\non: push # trigger\njobs:\n  build:\n    runs-on: [self-hosted, Linux, X64]\n    steps:\n      - run: echo runs-on ubuntu-latest\n",
			wantChanged: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, changed, warns := rewriteWorkflow(tt.in)
			if got != tt.want {
				t.Errorf("output mismatch\n got: %q\nwant: %q", got, tt.want)
			}
			if changed != tt.wantChanged {
				t.Errorf("changed = %d, want %d", changed, tt.wantChanged)
			}
			if len(warns) != tt.wantWarns {
				t.Errorf("warnings = %v, want %d", warns, tt.wantWarns)
			}
		})
	}
}

func TestRewriteWorkflowWarningLineNumbers(t *testing.T) {
	// A block-seq rewrite above must not shift the reported line of a warning
	// below it.
	in := "jobs:\n  a:\n    runs-on:\n      - ubuntu-latest\n  b:\n    runs-on: windows-latest\n"
	_, changed, warns := rewriteWorkflow(in)
	if changed != 1 {
		t.Fatalf("changed = %d, want 1", changed)
	}
	if len(warns) != 1 || !strings.HasPrefix(warns[0], "6:") {
		t.Errorf("warning should cite original line 6, got %v", warns)
	}
}
