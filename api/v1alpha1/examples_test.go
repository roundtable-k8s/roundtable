/*
Copyright 2026 dapperdivers.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// acceptedExampleNATSURLs is the set of spec.nats.url values an example may
// use: the quickstart's in-cluster NATS (namespace roundtable) and the
// namespace "database" of the author's cluster used by the other examples.
// The two disagree on purpose until the owner picks one (see
// examples/README.md, "Adapting the examples"). A new author-specific value
// must not slip in: extend this set only deliberately.
var acceptedExampleNATSURLs = []string{
	"nats://nats.roundtable.svc:4222",
	"nats://nats.database.svc:4222",
}

// TestExamplesDecodeStrictly decodes every Round Table object in examples/
// into the API types, rejecting unknown fields, so the examples can't drift
// from the CRDs (the API server would silently prune a stale field).
func TestExamplesDecodeStrictly(t *testing.T) {
	files, err := filepath.Glob("../../examples/*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	nested, err := filepath.Glob("../../examples/*/*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	files = append(files, nested...)
	recipes, err := filepath.Glob("../../examples/recipes/*/*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	files = append(files, recipes...)
	if len(files) == 0 {
		t.Fatal("no examples found")
	}

	decoded := 0
	for _, f := range files {
		for _, eo := range decodeExampleFile(t, f) {
			decoded++
			var natsURL string
			switch o := eo.obj.(type) {
			case *Knight:
				natsURL = o.Spec.NATS.URL
			case *RoundTable:
				natsURL = o.Spec.NATS.URL
			}
			if natsURL != "" && !slices.Contains(acceptedExampleNATSURLs, natsURL) {
				t.Errorf("%s doc %d (%s): spec.nats.url %q is not one of %v", f, eo.doc, eo.kind, natsURL, acceptedExampleNATSURLs)
			}
		}
	}
	if decoded == 0 {
		t.Fatal("no Round Table objects decoded")
	}
}

// exampleObject is one Round Table object strictly decoded from an example file.
type exampleObject struct {
	file string // base name of the example file
	doc  int    // document index within the file
	kind string
	name string // metadata.name
	obj  any    // *Knight, *Chain, *Mission, *RoundTable or *Trigger
}

// decodeExampleFile strictly decodes every Round Table document in file.
// Documents of other API groups are skipped; a document that does not decode
// is reported on t and left out.
func decodeExampleFile(t *testing.T, file string) []exampleObject {
	t.Helper()
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	sep := regexp.MustCompile(`(?m)^---\s*$`)
	var out []exampleObject
	for i, doc := range sep.Split(string(data), -1) {
		if strings.TrimSpace(doc) == "" {
			continue
		}
		var meta struct {
			APIVersion string `json:"apiVersion"`
			Kind       string `json:"kind"`
			Metadata   struct {
				Name string `json:"name"`
			} `json:"metadata"`
		}
		if err := yaml.Unmarshal([]byte(doc), &meta); err != nil {
			t.Errorf("%s doc %d: %v", file, i, err)
			continue
		}
		if meta.APIVersion != GroupVersion.String() {
			continue
		}
		var obj any
		switch meta.Kind {
		case "Knight":
			obj = &Knight{}
		case "Chain":
			obj = &Chain{}
		case "Mission":
			obj = &Mission{}
		case "RoundTable":
			obj = &RoundTable{}
		case "Trigger":
			obj = &Trigger{}
		default:
			t.Errorf("%s doc %d: unknown kind %q", file, i, meta.Kind)
			continue
		}
		if err := yaml.UnmarshalStrict([]byte(doc), obj); err != nil {
			t.Errorf("%s doc %d (%s): %v", file, i, meta.Kind, err)
			continue
		}
		out = append(out, exampleObject{
			file: filepath.Base(file),
			doc:  i,
			kind: meta.Kind,
			name: meta.Metadata.Name,
			obj:  obj,
		})
	}
	return out
}

// TestBacklogGroomingRecipe pins the backlog-grooming recipe's policy: its
// prompts carry the three binding rules, the file step is gated on the
// survey, and nothing names a real repository.
func TestBacklogGroomingRecipe(t *testing.T) {
	dir := "../../examples/recipes/backlog-grooming"
	data, err := os.ReadFile(filepath.Join(dir, "chain.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var chain Chain
	if err := yaml.UnmarshalStrict(data, &chain); err != nil {
		t.Fatal(err)
	}
	steps := map[string]ChainStep{}
	for _, s := range chain.Spec.Steps {
		steps[s.Name] = s
	}
	for _, name := range []string{"survey", "file"} {
		step, ok := steps[name]
		if !ok {
			t.Fatalf("step %q missing", name)
		}
		for _, rule := range []string{
			"Never add the label rt:ready",
			"Never touch an issue labelled humanLabel",
			"File at most maxIssuesPerRun issues per run",
		} {
			if !strings.Contains(step.Task, rule) {
				t.Errorf("step %s: prompt lacks rule %q", name, rule)
			}
		}
	}
	if steps["file"].When == "" || !strings.Contains(steps["file"].When, "steps.survey.output") {
		t.Errorf("file step must be gated with when on survey output, got %q", steps["file"].When)
	}

	if w := steps["file"].When; !strings.Contains(w, `matches("(?m)^CANDIDATES: [1-9]")`) {
		t.Errorf("file step must run only on an explicit candidate count, got %q", w)
	}

	var trig Trigger
	tdata, err := os.ReadFile(filepath.Join(dir, "trigger.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.UnmarshalStrict(tdata, &trig); err != nil {
		t.Fatal(err)
	}
	if trig.Spec.Source.Schedule == nil {
		t.Error("trigger must use a schedule source")
	}
	var in map[string]any
	if err := yaml.Unmarshal([]byte(trig.Spec.Input), &in); err != nil {
		t.Fatalf("trigger input is not JSON: %v", err)
	}
	for _, k := range []string{"repo", "epicLabel", "issueLabel", "humanLabel", "maxIssuesPerRun", "maxOpenGroomed"} {
		if _, ok := in[k]; !ok {
			t.Errorf("trigger input lacks %q", k)
		}
	}
	if in["repo"] != "OWNER/REPO" {
		t.Errorf("repo must be a placeholder, got %v", in["repo"])
	}
	for _, f := range []string{"knight.yaml", "chain.yaml", "trigger.yaml", "README.md"} {
		b, err := os.ReadFile(filepath.Join(dir, f))
		if err != nil {
			t.Fatal(err)
		}
		// The platform's own runtime image is not a stand-in for a user's repo.
		text := strings.ReplaceAll(string(b), "ghcr.io/roundtable-k8s/claude-knight", "")
		if strings.Contains(text, "roundtable-k8s") || strings.Contains(text, "dapperdivers") {
			t.Errorf("%s names a real org", f)
		}
	}
}

// TestOllamaKnightExample pins the local-model Knight: it runs an ollama/<tag>
// model against the in-cluster endpoint from KNIGHT_MODEL_BASE_URL, reaches
// that Service through allowedServices (not an open host), and holds no
// hosted provider key.
func TestOllamaKnightExample(t *testing.T) {
	data, err := os.ReadFile("../../examples/knight-ollama.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var knight Knight
	if err := yaml.UnmarshalStrict(data, &knight); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(knight.Spec.Model, "ollama/") || len(knight.Spec.Model) == len("ollama/") {
		t.Errorf("model = %q, want ollama/<tag>", knight.Spec.Model)
	}
	var baseURL string
	for _, e := range knight.Spec.Env {
		switch e.Name {
		case "KNIGHT_MODEL_BASE_URL":
			baseURL = e.Value
		case "ANTHROPIC_API_KEY", "OPENROUTER_API_KEY", "OPENAI_API_KEY":
			t.Errorf("hosted provider key %s must not be set", e.Name)
		}
	}
	if !strings.HasPrefix(baseURL, "http://") || !strings.HasSuffix(baseURL, "/v1") {
		t.Errorf("KNIGHT_MODEL_BASE_URL = %q, want http:// URL ending in /v1", baseURL)
	}
	if knight.Spec.Egress == nil || len(knight.Spec.Egress.AllowedServices) == 0 {
		t.Fatal("egress.allowedServices must name the model Service")
	}
	svc := knight.Spec.Egress.AllowedServices[0]
	if !strings.Contains(baseURL, svc.Name+"."+svc.Namespace+".svc") {
		t.Errorf("KNIGHT_MODEL_BASE_URL %q does not point at allowedServices[0] %s/%s", baseURL, svc.Namespace, svc.Name)
	}
	if !slices.Contains(svc.Ports, 11434) {
		t.Errorf("allowedServices[0] ports = %v, want Ollama port 11434", svc.Ports)
	}
}

// TestRecipeClaudeAliasesNameClaudeKnightImage fails when a recipe Knight (or
// a recipe RoundTable's defaults) names a Claude CLI model alias but the
// effective image is not claude-knight. The default knight image is pi-knight,
// which reads a bare name as an anthropic model that does not exist.
func TestRecipeClaudeAliasesNameClaudeKnightImage(t *testing.T) {
	aliases := []string{"opus", "sonnet", "haiku", "fable", "opusplan"}
	// Copy of internal/knight.AgentRuntimeForImage's rule (that package
	// imports this one, so it can't be used here): the image's repository
	// name, without registry, tag and digest, is claude-knight or starts
	// with "claude-knight-".
	isClaudeKnight := func(image string) bool {
		ref, _, _ := strings.Cut(image, "@")
		if i := strings.LastIndex(ref, "/"); i >= 0 {
			ref = ref[i+1:]
		}
		ref, _, _ = strings.Cut(ref, ":")
		return ref == "claude-knight" || strings.HasPrefix(ref, "claude-knight-")
	}
	files, err := filepath.Glob("../../examples/recipes/*/*.yaml")
	if err != nil || len(files) == 0 {
		t.Fatalf("no recipe files found: %v", err)
	}
	byRecipe := map[string][]exampleObject{}
	for _, f := range files {
		byRecipe[filepath.Dir(f)] = append(byRecipe[filepath.Dir(f)], decodeExampleFile(t, f)...)
	}
	for dir, objs := range byRecipe {
		tableImage := ""
		for _, o := range objs {
			if rt, ok := o.obj.(*RoundTable); ok {
				if d := rt.Spec.Defaults; d != nil {
					tableImage = d.Image
					if slices.Contains(aliases, d.Model) && !isClaudeKnight(tableImage) {
						t.Errorf("%s/%s: defaults.model %q is a Claude alias but defaults.image is %q", dir, o.file, d.Model, tableImage)
					}
				}
			}
		}
		for _, o := range objs {
			k, ok := o.obj.(*Knight)
			if !ok || !slices.Contains(aliases, k.Spec.Model) {
				continue
			}
			image := k.Spec.Image
			if image == "" {
				image = tableImage
			}
			if !isClaudeKnight(image) {
				t.Errorf("%s/%s: knight %s has Claude alias model %q but image %q is not claude-knight", dir, o.file, o.name, k.Spec.Model, image)
			}
		}
	}
}

// TestHomelabWatchRecipeFiles pins that the recipe is safe to apply as
// written: the placeholder token is below the gateway's 16-character minimum
// and no .yaml/.yml/.json file in the directory is a non-Kubernetes snippet
// that kubectl apply -f <dir> would fail on.
func TestHomelabWatchRecipeFiles(t *testing.T) {
	dir := "../../examples/recipes/homelab-watch"
	data, err := os.ReadFile(filepath.Join(dir, "trigger.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, doc := range strings.Split(string(data), "\n---\n") {
		var sec struct {
			Kind       string            `json:"kind"`
			StringData map[string]string `json:"stringData"`
		}
		if err := yaml.Unmarshal([]byte(doc), &sec); err != nil {
			t.Fatal(err)
		}
		if sec.Kind != "Secret" {
			continue
		}
		if tok := sec.StringData["token"]; len(tok) >= 16 {
			t.Errorf("placeholder token %q is %d chars, a valid gateway credential", tok, len(tok))
		}
	}
	// kubectl apply -f <dir> loads every .yaml/.yml/.json file, so each one
	// must be a Kubernetes object.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		switch filepath.Ext(e.Name()) {
		case ".yaml", ".yml", ".json":
		default:
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		for _, doc := range strings.Split(string(b), "\n---\n") {
			var obj struct {
				APIVersion string `json:"apiVersion"`
				Kind       string `json:"kind"`
			}
			if err := yaml.Unmarshal([]byte(doc), &obj); err != nil {
				t.Fatalf("%s: %v", e.Name(), err)
			}
			if obj.APIVersion == "" || obj.Kind == "" {
				t.Errorf("%s is not a Kubernetes object (no apiVersion/kind); kubectl apply -f <dir> would fail: rename it to *.yaml.example", e.Name())
			}
		}
	}
}
