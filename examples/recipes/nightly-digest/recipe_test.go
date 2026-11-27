// Package nightlydigest checks the nightly-digest recipe: the manifests parse
// and the skill's shell tests pass. It has no non-test code.
package nightlydigest

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"

	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

func TestManifestsParse(t *testing.T) {
	for file, kinds := range map[string][]string{
		"knight.yaml":  {"ServiceAccount", "ClusterRoleBinding", "Knight"},
		"trigger.yaml": {"Trigger"},
	} {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		docs, err := splitDocuments(data)
		if err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		if len(docs) != len(kinds) {
			t.Fatalf("%s: %d documents, want %d", file, len(docs), len(kinds))
		}
		for i, doc := range docs {
			var obj struct {
				Kind string `json:"kind"`
			}
			if err := yaml.Unmarshal(doc, &obj); err != nil {
				t.Fatalf("%s document %d: %v", file, i, err)
			}
			if obj.Kind != kinds[i] {
				t.Errorf("%s document %d: kind %q, want %q", file, i, obj.Kind, kinds[i])
			}
		}
	}
}

func TestSkillScripts(t *testing.T) {
	for _, tool := range []string{"bash", "jq"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("no %s available", tool)
		}
	}
	out, err := exec.Command("bash", "skill/nightly-digest/tests/run.sh").CombinedOutput()
	if err != nil {
		t.Fatalf("tests/run.sh: %v\n%s", err, out)
	}
	if !strings.HasSuffix(strings.TrimSpace(string(out)), "tests: ok") {
		t.Fatalf("unexpected output:\n%s", out)
	}
}

// TestFailureOutcomeInstructions checks that the task, the skill and the
// knight identity all tell the knight to end a failed run with the
// `Outcome: failed` line, so the instruction cannot be dropped silently.
func TestFailureOutcomeInstructions(t *testing.T) {
	for file, marker := range map[string]string{
		"trigger.yaml":                  "Outcome: failed",
		"skill/nightly-digest/SKILL.md": "Outcome: failed",
		"knight.yaml":                   "Outcome: failed",
	} {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(data, []byte(marker)) {
			t.Errorf("%s does not contain %q", file, marker)
		}
	}
}

func TestKnightLabelMatchesTrigger(t *testing.T) {
	read := func(file string, docIdx int, out any) {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		docs, err := splitDocuments(data)
		if err != nil {
			t.Fatal(err)
		}
		if err := yaml.Unmarshal(docs[docIdx], out); err != nil {
			t.Fatal(err)
		}
	}
	var knight struct {
		Metadata struct {
			Labels map[string]string `json:"labels"`
		} `json:"metadata"`
	}
	var trigger struct {
		Spec struct {
			RoundTableRef struct {
				Name string `json:"name"`
			} `json:"roundTableRef"`
		} `json:"spec"`
	}
	read("knight.yaml", 2, &knight)
	read("trigger.yaml", 0, &trigger)
	want := trigger.Spec.RoundTableRef.Name
	if want == "" {
		t.Fatal("trigger has no roundTableRef")
	}
	if got := knight.Metadata.Labels["ai.roundtable.io/round-table"]; got != want {
		t.Errorf("knight round-table label %q, want %q", got, want)
	}
}

// splitDocuments splits a multi-document YAML file into its non-empty
// documents. It accepts a leading "---" and "--- # comment" separators, and
// skips blank and comment-only documents, so the count is the number of objects.
func splitDocuments(data []byte) ([][]byte, error) {
	r := utilyaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(data)))
	var docs [][]byte
	for {
		doc, err := r.Read()
		if errors.Is(err, io.EOF) {
			return docs, nil
		}
		if err != nil {
			return nil, err
		}
		if hasContent(doc) {
			docs = append(docs, doc)
		}
	}
}

func hasContent(doc []byte) bool {
	for line := range strings.SplitSeq(string(doc), "\n") {
		if l := strings.TrimSpace(line); l != "" && !strings.HasPrefix(l, "#") {
			return true
		}
	}
	return false
}

// TestSplitDocuments pins the leading "---", comment separator and
// comment-only document cases.
func TestSplitDocuments(t *testing.T) {
	in := "---\n# header\nkind: A\n--- # second\n\n---\n# only a comment\n---\nkind: B\n"
	docs, err := splitDocuments([]byte(in))
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 2 {
		t.Fatalf("got %d documents, want 2: %q", len(docs), docs)
	}
}
