package sshrunner

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestShellQuote(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"abc123+/=", `'abc123+/='`},
		{"", `''`},
		{"a b", `'a b'`},
		{"it's", `'it'\''s'`},
		{"; rm -rf /", `'; rm -rf /'`},
		{"'$(touch x)'", `''\''$(touch x)'\'''`},
	}
	for _, c := range cases {
		if got := shellQuote(c.in); got != c.want {
			t.Errorf("shellQuote(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestInstallAndStart(t *testing.T) {
	for _, mode := range []string{"current", "update", "copy-failed", "extract-failed"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			log := filepath.Join(dir, "commands")
			t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
			t.Setenv("RUNNER_TEST_LOG", log)
			t.Setenv("RUNNER_TEST_MODE", mode)
			for name, script := range map[string]string{
				"ssh": `#!/bin/sh
case "$*" in
  *Runner.Listener*--version*) test "$RUNNER_TEST_MODE" = current ;;
  *"tar xzf"*)
    printf 'extract\n' >> "$RUNNER_TEST_LOG"
    test "$RUNNER_TEST_MODE" != extract-failed ;;
  *"bash -s"*)
    printf 'start\n' >> "$RUNNER_TEST_LOG"
    cat > "$RUNNER_TEST_LOG.script" ;;
  *) exit 1 ;;
esac
`,
				"scp": `#!/bin/sh
printf 'copy\n' >> "$RUNNER_TEST_LOG"
test "$RUNNER_TEST_MODE" != copy-failed
`,
			} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(script), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			err := InstallAndStart(context.Background(), "runner", "127.0.0.1", Runner{Version: "2.337.0", Tar: "runner.tar.gz"}, "test-jit", "/Volumes/My Shared Files/actions")
			if (err != nil) != strings.HasSuffix(mode, "failed") {
				t.Fatalf("unexpected install result: %v", err)
			}
			commands, readErr := os.ReadFile(log)
			if readErr != nil {
				t.Fatal(readErr)
			}
			want := map[string]string{
				"current":        "start\n",
				"update":         "copy\nextract\nstart\n",
				"copy-failed":    "copy\n",
				"extract-failed": "copy\nextract\n",
			}[mode]
			if string(commands) != want {
				t.Fatalf("commands %q, want %q", commands, want)
			}
			if strings.HasSuffix(mode, "failed") {
				return
			}
			script, readErr := os.ReadFile(log + ".script")
			if readErr != nil {
				t.Fatal(readErr)
			}
			if !strings.Contains(string(script), "export ACTIONS_RUNNER_ACTION_ARCHIVE_CACHE='/Volumes/My Shared Files/actions'\n") {
				t.Fatalf("script does not export the action cache:\n%s", script)
			}
		})
	}
}
