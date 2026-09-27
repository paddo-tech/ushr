package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// Hosted-runner labels ushr can serve, mapped to the label sets every
// self-hosted runner advertises automatically (GitHub adds self-hosted/OS/Arch
// labels itself, so no agent config is needed to know them).
var (
	hostedMacRe     = regexp.MustCompile(`(?i)^macos-(latest|\d+(\.\d+)?)(-x?large)?$`)
	hostedUbuntuRe  = regexp.MustCompile(`(?i)^ubuntu-(latest|\d{2}\.\d{2})(-arm(64)?)?$`)
	hostedWindowsRe = regexp.MustCompile(`(?i)^windows-`)

	runsOnLineRe = regexp.MustCompile(`^(\s*)runs-on:\s*(?:[^#]*?)\s*(#.*)?$`)
)

func runInit(_ context.Context, args []string) error {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	dryRun := fs.Bool("dry-run", false, "report what would change without writing files")
	if err := fs.Parse(args); err != nil {
		return err
	}
	dir := "."
	if rest := fs.Args(); len(rest) > 0 {
		// Stdlib flag stops parsing at the first positional, so a flag placed
		// after the directory would otherwise be silently ignored.
		if len(rest) > 1 {
			return fmt.Errorf("unexpected arguments after %q (flags go before the directory): %s", rest[0], strings.Join(rest[1:], " "))
		}
		dir = rest[0]
	}
	wfDir := filepath.Join(dir, ".github", "workflows")
	entries, err := os.ReadDir(wfDir)
	if errors.Is(err, os.ErrNotExist) {
		fmt.Println("==> No workflows found.")
		return nil
	}
	if err != nil {
		return err
	}

	var rewrote int
	var warnings []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || (!strings.HasSuffix(name, ".yml") && !strings.HasSuffix(name, ".yaml")) {
			continue
		}
		rel := filepath.Join(".github", "workflows", name)
		path := filepath.Join(wfDir, name)
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		out, jobs, warns := rewriteWorkflow(string(src))
		for _, w := range warns {
			warnings = append(warnings, fmt.Sprintf("%s:%s", rel, w))
		}
		if jobs == 0 {
			continue
		}
		if *dryRun {
			fmt.Printf("==> %s: would rewrite %d runs-on target(s)\n", rel, jobs)
		} else {
			if err := os.WriteFile(path, []byte(out), 0o644); err != nil {
				return err
			}
			fmt.Printf("==> %s: rewrote %d runs-on target(s)\n", rel, jobs)
		}
		rewrote++
	}

	if rewrote == 0 {
		fmt.Println("==> No workflows needed changes.")
	}
	for _, w := range warnings {
		fmt.Fprintln(os.Stderr, "warning:", w)
	}
	switch {
	case rewrote > 0 && *dryRun:
		fmt.Println("==> Dry run — no files written.")
	case rewrote > 0:
		fmt.Println()
		fmt.Println("==> Done. Jobs now target ushr's self-hosted labels.")
		fmt.Println("    Make sure the ushr GitHub App covers this repo —")
		fmt.Println("    run `ushr setup --org ORG` (or --repo OWNER/REPO) if it doesn't.")
	}
	return nil
}

// rewriteWorkflow rewrites every job-level runs-on target in one workflow
// file. The YAML parse only *locates* targets (so runs-on strings inside
// `with:` inputs or `run:` scripts are never touched); the edit itself is a
// text splice, keeping every untouched line byte-for-byte. Returns the new
// content, how many targets changed, and "line: message" warnings for targets
// it can't rewrite safely.
func rewriteWorkflow(src string) (string, int, []string) {
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(src), &doc); err != nil {
		return src, 0, []string{fmt.Sprintf("1: not valid YAML — left unchanged (%v)", err)}
	}
	targets := collectTargets(&doc)
	lines := strings.Split(src, "\n")
	var changed int
	var warnings []string
	warn := func(line int, msg string) {
		warnings = append(warnings, fmt.Sprintf("%d: %s", line, msg))
	}

	// Targets come in document order; walk them in reverse so folding a value
	// line away never shifts an earlier target's line numbers.
	for ti := len(targets) - 1; ti >= 0; ti-- {
		key, val := targets[ti].key, targets[ti].val

		var labels []string
		switch val.Kind {
		case yaml.ScalarNode:
			if val.Tag == "!!null" {
				warn(key.Line, "empty runs-on")
				continue
			}
			labels = []string{val.Value}
		case yaml.SequenceNode:
			for _, item := range val.Content {
				labels = append(labels, item.Value)
			}
		case yaml.MappingNode:
			warn(key.Line, "runs-on group form isn't supported — set ushr labels manually")
			continue
		default:
			warn(key.Line, "unsupported runs-on form — set ushr labels manually")
			continue
		}

		expr := false
		for _, l := range labels {
			expr = expr || strings.Contains(l, "${{")
		}
		if expr {
			warn(key.Line, "runs-on is an expression (matrix?) — update the matrix values to ushr labels manually")
			continue
		}
		repl, w := mapTarget(labels)
		if w != "" {
			warn(key.Line, w)
		}
		if repl == "" {
			continue
		}

		// mapTarget only maps single-label targets, so the value is one node:
		// the scalar itself or the sequence's only item (the repl != "" guard
		// above is what makes the Content[0] deref safe on empty sequences).
		// When it sits on its own line (block-seq item or next-line scalar) it
		// folds into the key line — but only from the line directly below;
		// anything between (a comment) or spanning lines can't be spliced
		// safely.
		node := val
		if val.Kind == yaml.SequenceNode {
			node = val.Content[0]
		}
		foldLine := 0
		switch {
		case val.Anchor != "" || node.Anchor != "":
			// The splice would strip the anchor, breaking any alias to it.
			warn(key.Line, "runs-on has a YAML anchor — set ushr labels manually")
			continue
		case val.Kind == yaml.ScalarNode && (val.Style == yaml.LiteralStyle || val.Style == yaml.FoldedStyle):
			warn(key.Line, "multiline runs-on — set ushr labels manually")
			continue
		case node.Line == key.Line:
		case val.Kind == yaml.SequenceNode && val.Style == yaml.FlowStyle:
			warn(key.Line, "multiline runs-on — set ushr labels manually")
			continue
		case node.Line != key.Line+1 || (val.Kind == yaml.ScalarNode && node.Style != 0):
			warn(key.Line, "unsupported runs-on layout — set ushr labels manually")
			continue
		default:
			foldLine = node.Line
		}

		var foldComment string
		if foldLine > 0 {
			foldComment = trailingComment(lines[foldLine-1])
		}
		if !replaceLine(lines, key.Line, repl, foldComment) {
			warn(key.Line, "could not rewrite line — set ushr labels manually")
			continue
		}
		if foldLine > 0 {
			lines = append(lines[:foldLine-1], lines[foldLine:]...)
		}
		changed++
	}
	return strings.Join(lines, "\n"), changed, warnings
}

type target struct {
	key, val *yaml.Node
}

// collectTargets walks jobs.<id>.runs-on only — the sole place the runs-on key
// means "runner labels". Same-named keys elsewhere (action inputs under
// `with:`, text inside `run:` scripts) are never candidates.
func collectTargets(doc *yaml.Node) []target {
	root := doc
	if root.Kind == yaml.DocumentNode && len(root.Content) > 0 {
		root = root.Content[0]
	}
	if root.Kind != yaml.MappingNode {
		return nil
	}
	var jobs *yaml.Node
	for i := 0; i+1 < len(root.Content); i += 2 {
		if root.Content[i].Value == "jobs" {
			jobs = root.Content[i+1]
			break
		}
	}
	if jobs == nil || jobs.Kind != yaml.MappingNode {
		return nil
	}
	var out []target
	for j := 0; j+1 < len(jobs.Content); j += 2 {
		job := jobs.Content[j+1]
		if job.Kind != yaml.MappingNode {
			continue
		}
		for k := 0; k+1 < len(job.Content); k += 2 {
			if job.Content[k].Value == "runs-on" {
				out = append(out, target{key: job.Content[k], val: job.Content[k+1]})
			}
		}
	}
	return out
}

// replaceLine rewrites the 1-based line's runs-on value in place, preserving
// indentation, a trailing comment, and a CRLF ending. extraComment carries the
// comment of a value line being folded away; the key line's own comment wins.
func replaceLine(lines []string, line int, repl, extraComment string) bool {
	if line < 1 || line > len(lines) {
		return false
	}
	orig := lines[line-1]
	crlf := strings.HasSuffix(orig, "\r")
	m := runsOnLineRe.FindStringSubmatch(strings.TrimSuffix(orig, "\r"))
	if m == nil {
		return false
	}
	out := m[1] + "runs-on: " + repl
	switch {
	case m[2] != "":
		out += " " + m[2]
	case extraComment != "":
		out += " " + extraComment
	}
	if crlf {
		out += "\r"
	}
	lines[line-1] = out
	return true
}

// trailingComment returns a line's "#..." comment, if any. Runner labels can't
// contain '#', so the first one starts the comment.
func trailingComment(line string) string {
	if i := strings.Index(line, "#"); i >= 0 {
		return strings.TrimRight(line[i:], "\r\t ")
	}
	return ""
}

// mapTarget maps a runs-on label set to its ushr replacement (in flow-seq
// syntax), or returns "" with an optional warning when it shouldn't change.
func mapTarget(items []string) (string, string) {
	for _, it := range items {
		if strings.EqualFold(it, "self-hosted") {
			return "", "" // already self-hosted
		}
	}
	if len(items) != 1 {
		return "", "" // multi-label without self-hosted: custom setup, leave alone
	}
	label := items[0]
	switch {
	case hostedMacRe.MatchString(label):
		if intelMac(label) {
			return "", fmt.Sprintf("%s is Intel-hosted but ushr macs are ARM64 — retarget manually if the job can run on ARM", label)
		}
		return "[self-hosted, macOS, ARM64]", ""
	case hostedUbuntuRe.MatchString(label):
		if strings.Contains(strings.ToLower(label), "-arm") {
			return "[self-hosted, Linux, ARM64]", ""
		}
		return "[self-hosted, Linux, X64]", ""
	case hostedWindowsRe.MatchString(label):
		return "", fmt.Sprintf("%s has no ushr driver — left unchanged", label)
	default:
		return "", "" // custom label, leave alone
	}
}

// intelMac reports whether a GitHub-hosted macOS label is Intel x64: macos-13
// and the "-large" tier are Intel; "-xlarge" and macos-14+ are Apple Silicon.
func intelMac(label string) bool {
	low := strings.ToLower(label)
	if strings.HasSuffix(low, "-xlarge") {
		return false
	}
	return strings.HasPrefix(low, "macos-13") || strings.HasSuffix(low, "-large")
}
