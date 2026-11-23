package knight

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// runSkillFilter executes the generated skill-filter script once against a
// temp dir standing in for /arsenal and /skills.
//
//nolint:unparam // the script output is only asserted by some callers
func runSkillFilter(t *testing.T, k *aiv1alpha1.Knight, root string) string {
	t.Helper()
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("no sh available")
	}
	b := NewPodBuilder(k, "img").WithSkillFilter()
	script := b.sidecars[0].Args[0]
	// Re-root only the ARSENALS/TARGET assignments (a blanket replace would
	// also rewrite the second "/arsenal" in "/arsenal/arsenal").
	lines := strings.Split(script, "\n")
	for i, l := range lines {
		switch {
		case strings.HasPrefix(l, `ARSENALS="`):
			paths := strings.Fields(strings.Trim(strings.TrimPrefix(l, "ARSENALS="), `"`))
			for j, p := range paths {
				paths[j] = filepath.Join(root, p)
			}
			lines[i] = `ARSENALS="` + strings.Join(paths, " ") + `"`
		case strings.HasPrefix(l, `TARGET="`):
			lines[i] = `TARGET="` + filepath.Join(root, "skills") + `"`
		}
	}
	script = strings.Join(lines, "\n")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, sh, "-c", script)
	cmd.Env = append(os.Environ(), "SKILL_FILTER_ONCE=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("skill-filter failed: %v\n%s", err, out)
	}
	return string(out)
}

func mkSkill(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "SKILL.md"), []byte("---\nname: x\n---\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestSkillFilterMergesArsenalsPerSkill(t *testing.T) {
	root := t.TempDir()
	platform := filepath.Join(root, "arsenal", "arsenal")
	personal := filepath.Join(root, "arsenal", "personal")

	mkSkill(t, filepath.Join(platform, "shared", "web-search"))
	mkSkill(t, filepath.Join(platform, "shared", "web-fetch"))
	mkSkill(t, filepath.Join(platform, "coding", "roundtable-operator"))
	if err := os.WriteFile(filepath.Join(platform, "coding", "coding-agent.md"), []byte("rules"), 0o644); err != nil {
		t.Fatal(err)
	}
	mkSkill(t, filepath.Join(personal, "shared", "web-search")) // overrides platform
	mkSkill(t, filepath.Join(personal, "security", "cve-deep-dive"))
	mkSkill(t, filepath.Join(personal, "finance", "tax-prep")) // category not requested

	k := &aiv1alpha1.Knight{}
	k.Spec.Skills = []string{"shared", "coding", "security"}
	k.Spec.Arsenal = &aiv1alpha1.KnightArsenal{}
	k.Spec.Arsenals = []aiv1alpha1.KnightArsenal{{Name: "personal", Repo: "https://github.com/x/roundtable-arsenal"}}

	runSkillFilter(t, k, root)

	skills := filepath.Join(root, "skills")
	want := map[string]string{
		"shared/web-search":          filepath.Join(personal, "shared", "web-search"),
		"shared/web-fetch":           filepath.Join(platform, "shared", "web-fetch"),
		"coding/roundtable-operator": filepath.Join(platform, "coding", "roundtable-operator"),
		"coding/coding-agent.md":     filepath.Join(platform, "coding", "coding-agent.md"),
		"security/cve-deep-dive":     filepath.Join(personal, "security", "cve-deep-dive"),
	}
	for rel, target := range want {
		got, err := os.Readlink(filepath.Join(skills, rel))
		if err != nil {
			t.Errorf("%s: %v", rel, err)
			continue
		}
		if got != target {
			t.Errorf("%s -> %s, want %s", rel, got, target)
		}
	}
	if _, err := os.Lstat(filepath.Join(skills, "finance")); !os.IsNotExist(err) {
		t.Errorf("unrequested category finance should not be linked")
	}

	// A skill removed upstream is pruned; an override removed falls back.
	if err := os.RemoveAll(filepath.Join(platform, "shared", "web-fetch")); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(personal, "shared", "web-search")); err != nil {
		t.Fatal(err)
	}
	runSkillFilter(t, k, root)

	if _, err := os.Lstat(filepath.Join(skills, "shared", "web-fetch")); !os.IsNotExist(err) {
		t.Errorf("web-fetch should have been pruned")
	}
	got, _ := os.Readlink(filepath.Join(skills, "shared", "web-search"))
	if got != filepath.Join(platform, "shared", "web-search") {
		t.Errorf("web-search should fall back to platform, got %s", got)
	}
}

func TestSkillFilterReplacesLegacyCategoryLink(t *testing.T) {
	root := t.TempDir()
	platform := filepath.Join(root, "arsenal", "arsenal")
	mkSkill(t, filepath.Join(platform, "shared", "web-search"))

	skills := filepath.Join(root, "skills")
	if err := os.MkdirAll(skills, 0o755); err != nil {
		t.Fatal(err)
	}
	// Old layout: /skills/shared -> /arsenal/<repo>/shared
	if err := os.Symlink(filepath.Join(platform, "shared"), filepath.Join(skills, "shared")); err != nil {
		t.Fatal(err)
	}

	k := &aiv1alpha1.Knight{}
	k.Spec.Skills = []string{"shared"}
	k.Spec.Arsenal = &aiv1alpha1.KnightArsenal{}
	runSkillFilter(t, k, root)

	fi, err := os.Lstat(filepath.Join(skills, "shared"))
	if err != nil || fi.Mode()&os.ModeSymlink != 0 || !fi.IsDir() {
		t.Fatalf("shared should now be a real directory: %v %v", fi, err)
	}
	if _, err := os.Stat(filepath.Join(skills, "shared", "web-search", "SKILL.md")); err != nil {
		t.Errorf("web-search not reachable: %v", err)
	}
}

// The sidecar runs the script as PID 1, which ignores SIGTERM unless trapped.
func TestSkillFilterScriptTrapsTerm(t *testing.T) {
	k := &aiv1alpha1.Knight{}
	k.Spec.Skills = []string{"core"}
	script := NewPodBuilder(k, "img").WithSkillFilter().sidecars[0].Args[0]
	if !strings.Contains(script, "trap 'exit 0' TERM INT") {
		t.Fatalf("script has no TERM/INT trap:\n%s", script)
	}
	for _, l := range strings.Split(script, "\n") {
		if strings.Contains(l, "sleep ") && !strings.Contains(l, "sleep 2 & wait $!") && !strings.Contains(l, "sleep 60 & wait $!") {
			t.Errorf("sleep is not interruptible: %q", l)
		}
	}
}

func TestSkillFilterExitsOnSIGTERM(t *testing.T) {
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("no sh available")
	}
	root := t.TempDir()
	k := &aiv1alpha1.Knight{}
	k.Spec.Skills = []string{"core"}
	script := NewPodBuilder(k, "img").WithSkillFilter().sidecars[0].Args[0]
	script = strings.ReplaceAll(script, `ARSENALS="/arsenal"`, `ARSENALS="`+root+`"`)
	script = strings.ReplaceAll(script, `TARGET="/skills"`, `TARGET="`+filepath.Join(root, "skills")+`"`)

	cmd := exec.Command(sh, "-c", "exec "+"sh -c \"$0\"", script) //nolint:gosec // test-only
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond) // let it reach the sleep loop
	_ = cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected clean exit on SIGTERM, got %v", err)
		}
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("skill-filter did not exit on SIGTERM")
	}
}
