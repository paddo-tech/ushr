package config

import (
	"bytes"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// AddOrg appends o to the orgs list in the agent config at path.
func AddOrg(path string, o Org) error { return addListEntry(path, "orgs", o) }

// AddRepo appends r to the repos list in the agent config at path.
func AddRepo(path string, r RepoTarget) error { return addListEntry(path, "repos", r) }

// addListEntry edits the YAML via a node round-trip rather than re-marshaling
// the typed config, so comments and key order in a hand-maintained file
// survive the write.
func addListEntry(path, key string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(b, &doc); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	if len(doc.Content) == 0 || doc.Content[0].Kind != yaml.MappingNode {
		return fmt.Errorf("%s: expected a YAML mapping at the top level", path)
	}
	root := doc.Content[0]

	var seq *yaml.Node
	for i := 0; i+1 < len(root.Content); i += 2 {
		if root.Content[i].Value == key {
			seq = root.Content[i+1]
			break
		}
	}
	switch {
	case seq == nil:
		seq = &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
		root.Content = append(root.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}, seq)
	case seq.Kind != yaml.SequenceNode:
		// `orgs:` with no entries parses as a null scalar; turn it into a list.
		*seq = yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
	}

	var entry yaml.Node
	if err := entry.Encode(v); err != nil {
		return err
	}
	seq.Content = append(seq.Content, &entry)
	// A previously-empty `[]` is flow-style; force block style so the
	// appended mapping renders as a normal multi-line list entry.
	seq.Style = 0

	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(&doc); err != nil {
		return err
	}
	if err := enc.Close(); err != nil {
		return err
	}
	return os.WriteFile(path, buf.Bytes(), 0o644)
}
