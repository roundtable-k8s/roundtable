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

package knight

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/util"
)

// knightToolPATH returns the container PATH for a knight, set as a pod-spec env
// var so EVERY process — kubectl exec shells, the agent, and any subprocess —
// resolves the knight's tools, not just the entrypoint's exec'd process tree.
// Missing dirs are ignored, so the list covers every tool source at once.
func knightToolPATH() string {
	return strings.Join([]string{
		"/home/node/.nix-profile/bin",
		"/data/nix-env/bin",
		ToolsImageProfilePath + "/bin",
		"/data/bin",
		"/data/.mise/shims",
		"/app/.mise/shims",
		"/usr/local/sbin", "/usr/local/bin", "/usr/sbin", "/usr/bin", "/sbin", "/bin",
	}, ":")
}

// PodBuilder provides a composable way to build Knight pod specs.
// Each With* method adds its own volumes, mounts, and/or containers.
type PodBuilder struct {
	knight     *aiv1alpha1.Knight
	volumes    []corev1.Volume
	mounts     []corev1.VolumeMount
	sidecars   []corev1.Container
	env        []corev1.EnvVar
	envFrom    []corev1.EnvFromSource
	defaultImg string
	security   PodSecurity
	reader     client.Reader
}

// NewPodBuilder creates a new PodBuilder for the given Knight.
func NewPodBuilder(k *aiv1alpha1.Knight, defaultImage string) *PodBuilder {
	return &PodBuilder{
		knight:     k,
		volumes:    []corev1.Volume{},
		mounts:     []corev1.VolumeMount{},
		sidecars:   []corev1.Container{},
		env:        []corev1.EnvVar{},
		defaultImg: defaultImage,
		security:   DefaultPodSecurity(),
	}
}

// WithSecurity sets the pod-level security context. Defaults to
// DefaultPodSecurity when not called.
func (b *PodBuilder) WithSecurity(s PodSecurity) *PodBuilder {
	b.security = s.OrDefault()
	return b
}

// WithReader sets the client reader for looking up resources.
func (b *PodBuilder) WithReader(r client.Reader) *PodBuilder {
	b.reader = r
	return b
}

// WithWorkspace adds the workspace PVC mount at /data.
//
// In ephemeral mode (workspace.mode: ephemeral) /data is a per-pod emptyDir
// limited to workspace.size, so several replicas can run side by side, and
// KNIGHT_WORKSPACE_MODE=ephemeral tells the runtime its /data doesn't persist.
func (b *PodBuilder) WithWorkspace() *PodBuilder {
	if EphemeralWorkspace(b.knight) {
		emptyDir := &corev1.EmptyDirVolumeSource{}
		if size := b.knight.Spec.Workspace.Size; size != nil && !size.IsZero() {
			emptyDir.SizeLimit = new(size.DeepCopy())
		}
		b.volumes = append(b.volumes, corev1.Volume{
			Name:         "data",
			VolumeSource: corev1.VolumeSource{EmptyDir: emptyDir},
		})
		b.mounts = append(b.mounts, corev1.VolumeMount{Name: "data", MountPath: "/data"})
		b.env = append(b.env, corev1.EnvVar{Name: "KNIGHT_WORKSPACE_MODE", Value: aiv1alpha1.WorkspaceModeEphemeral})
		return b
	}

	pvcName := b.knight.Name
	if b.knight.Spec.Workspace != nil && b.knight.Spec.Workspace.ExistingClaim != "" {
		pvcName = b.knight.Spec.Workspace.ExistingClaim
	}

	b.volumes = append(b.volumes, corev1.Volume{
		Name: "data",
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: pvcName,
			},
		},
	})
	b.mounts = append(b.mounts, corev1.VolumeMount{
		Name:      "data",
		MountPath: "/data",
	})
	return b
}

// WithConfig adds the config ConfigMap mount at /config.
func (b *PodBuilder) WithConfig(configMapName string) *PodBuilder {
	b.volumes = append(b.volumes, corev1.Volume{
		Name: "config",
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: configMapName},
			},
		},
	})
	b.mounts = append(b.mounts, corev1.VolumeMount{
		Name:      "config",
		MountPath: "/config",
		ReadOnly:  true,
	})
	return b
}

// WithNixStore mounts the knight's tools image (see tools_image.go) read-only
// at /nix as an image volume, when the knight has Nix tools and a tools image
// repository is configured. It mounts the image last published for the knight,
// so a changed tool list only rolls the pod once the new image exists; a
// knight with nothing published yet waits in image pull for its first image.
// Tools resolve via the image's profile bin dir, already on the container PATH
// (knightToolPATH).
func (b *PodBuilder) WithNixStore() *PodBuilder {
	if ref := MountedToolsImageRef(b.knight); ref != "" {
		b.volumes = append(b.volumes, corev1.Volume{
			Name: "nix",
			VolumeSource: corev1.VolumeSource{
				Image: &corev1.ImageVolumeSource{
					Reference:  ref,
					PullPolicy: corev1.PullIfNotPresent,
				},
			},
		})
		// The image holds /nix/store and /nix/profile; subPath "nix" lands
		// them at the paths the store's own symlinks expect.
		b.mounts = append(b.mounts, corev1.VolumeMount{
			Name:      "nix",
			MountPath: "/nix",
			SubPath:   "nix",
			ReadOnly:  true,
		})
		b.env = append(b.env, corev1.EnvVar{
			Name:  "KNIGHT_NIX_PROFILE",
			Value: ToolsImageProfilePath,
		})
	}
	return b
}

// WithVault adds the vault PVC with optional writable subpaths.
func (b *PodBuilder) WithVault() *PodBuilder {
	if b.knight.Spec.Vault == nil {
		return b
	}

	claimName := b.knight.Spec.Vault.ClaimName
	if claimName == "" {
		claimName = "obsidian-vault"
	}

	// PVC must be ReadOnly=false when writablePaths exist
	pvcReadOnly := b.knight.Spec.Vault.ReadOnly
	if len(b.knight.Spec.Vault.WritablePaths) > 0 {
		pvcReadOnly = false
	}

	b.volumes = append(b.volumes, corev1.Volume{
		Name: "vault",
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: claimName,
				ReadOnly:  pvcReadOnly,
			},
		},
	})

	// Base mount
	b.mounts = append(b.mounts, corev1.VolumeMount{
		Name:      "vault",
		MountPath: "/vault",
		ReadOnly:  b.knight.Spec.Vault.ReadOnly,
	})

	// Writable subpaths override the read-only base
	for _, wp := range b.knight.Spec.Vault.WritablePaths {
		b.mounts = append(b.mounts, corev1.VolumeMount{
			Name:      "vault",
			MountPath: fmt.Sprintf("/vault/%s", strings.TrimSuffix(wp, "/")),
			SubPath:   strings.TrimSuffix(wp, "/"),
			ReadOnly:  false,
		})
	}

	return b
}

// WithSharedWorkspace adds the RoundTable shared workspace PVC if configured.
func (b *PodBuilder) WithSharedWorkspace(ctx context.Context) *PodBuilder {
	if b.reader == nil {
		return b
	}

	tableName, ok := b.knight.Labels["ai.roundtable.io/table"]
	if !ok {
		return b
	}

	rt := &aiv1alpha1.RoundTable{}
	if err := b.reader.Get(ctx, types.NamespacedName{
		Name:      tableName,
		Namespace: b.knight.Namespace,
	}, rt); err != nil {
		return b
	}

	if rt.Spec.SharedWorkspace == nil {
		return b
	}

	mountPath := rt.Spec.SharedWorkspace.MountPath
	if mountPath == "" {
		mountPath = "/shared"
	}

	b.volumes = append(b.volumes, corev1.Volume{
		Name: "shared-workspace",
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: rt.Spec.SharedWorkspace.ClaimName,
			},
		},
	})
	b.mounts = append(b.mounts, corev1.VolumeMount{
		Name:      "shared-workspace",
		MountPath: mountPath,
	})

	return b
}

// FallbackImage is the knight image used when neither spec.image nor the
// operator's DEFAULT_KNIGHT_IMAGE is set.
const FallbackImage = "ghcr.io/roundtable-k8s/pi-knight:latest"

// EffectiveImage returns the image a knight pod runs: spec.image, else
// defaultImage, else FallbackImage.
func EffectiveImage(k *aiv1alpha1.Knight, defaultImage string) string {
	if k.Spec.Image != "" {
		return k.Spec.Image
	}
	if defaultImage != "" {
		return defaultImage
	}
	return FallbackImage
}

const (
	// GitHubTokenDir is where the GitHub token Secret is mounted in the knight.
	GitHubTokenDir = "/var/run/secrets/roundtable/github"
	// GitHubTokenFile is the token file inside GitHubTokenDir, exported to the
	// knight as GITHUB_TOKEN_FILE.
	GitHubTokenFile = GitHubTokenDir + "/token"
	// defaultGitHubTokenKey is the Secret key used when tokenSecretRef.key is empty.
	defaultGitHubTokenKey = "token"
	// legacyTableLabel is the older knight-to-table label (still read by
	// WithSharedWorkspace and the planner prompt).
	legacyTableLabel = "ai.roundtable.io/table"
)

// ResolveGitHubTokenSecretRef returns the GitHub token Secret for a knight:
// the knight's own spec.github.tokenSecretRef, else the one on its RoundTable
// (found via the ai.roundtable.io/round-table label, then the legacy
// ai.roundtable.io/table label). Returns nil when neither is set. An empty
// key falls back to "token".
func ResolveGitHubTokenSecretRef(ctx context.Context, r client.Reader, k *aiv1alpha1.Knight) *corev1.SecretKeySelector {
	ref := githubTokenRef(k.Spec.GitHub)
	if ref == nil {
		if rt := findTable(ctx, r, k, func(rt *aiv1alpha1.RoundTable) bool {
			return githubTokenRef(rt.Spec.GitHub) != nil
		}); rt != nil {
			ref = githubTokenRef(rt.Spec.GitHub)
		}
	}
	if ref == nil {
		return nil
	}
	out := ref.DeepCopy()
	if out.Key == "" {
		out.Key = defaultGitHubTokenKey
	}
	return out
}

// findTable looks up the RoundTable a knight belongs to via the
// ai.roundtable.io/round-table label, then the legacy ai.roundtable.io/table
// label, returning the first one for which accept is true (nil if none).
func findTable(ctx context.Context, r client.Reader, k *aiv1alpha1.Knight, accept func(*aiv1alpha1.RoundTable) bool) *aiv1alpha1.RoundTable {
	if r == nil {
		return nil
	}
	for _, label := range []string{aiv1alpha1.LabelRoundTable, legacyTableLabel} {
		tableName := k.Labels[label]
		if tableName == "" {
			continue
		}
		rt := &aiv1alpha1.RoundTable{}
		if err := r.Get(ctx, types.NamespacedName{Name: tableName, Namespace: k.Namespace}, rt); err != nil {
			continue
		}
		if accept(rt) {
			return rt
		}
	}
	return nil
}

// WithTableSecrets appends the knight's RoundTable spec.secrets (found via
// findTable) to the container's envFrom, skipping Secrets already referenced
// by the knight's own envFrom. The Knight object itself is not modified.
func (b *PodBuilder) WithTableSecrets(ctx context.Context) *PodBuilder {
	rt := findTable(ctx, b.reader, b.knight, func(rt *aiv1alpha1.RoundTable) bool {
		return len(rt.Spec.Secrets) > 0
	})
	if rt == nil {
		return b
	}
	seen := map[string]bool{}
	for _, e := range b.knight.Spec.EnvFrom {
		if e.SecretRef != nil {
			seen[e.SecretRef.Name] = true
		}
	}
	for _, ref := range rt.Spec.Secrets {
		if ref.Name == "" || seen[ref.Name] {
			continue
		}
		seen[ref.Name] = true
		b.envFrom = append(b.envFrom, corev1.EnvFromSource{
			SecretRef: &corev1.SecretEnvSource{LocalObjectReference: ref},
		})
	}
	return b
}

func githubTokenRef(g *aiv1alpha1.GitHubConfig) *corev1.SecretKeySelector {
	if g == nil || g.TokenSecretRef == nil || g.TokenSecretRef.Name == "" {
		return nil
	}
	return g.TokenSecretRef
}

// WithGitHubToken mounts the knight's GitHub token Secret (see
// ResolveGitHubTokenSecretRef) read-only at GitHubTokenDir and sets
// GITHUB_TOKEN_FILE. The whole directory is mounted (no subPath) so the
// kubelet refreshes the file in place when the Secret is rotated; the token is
// never put in the environment.
func (b *PodBuilder) WithGitHubToken(ctx context.Context) *PodBuilder {
	ref := ResolveGitHubTokenSecretRef(ctx, b.reader, b.knight)
	if ref == nil {
		return b
	}
	b.volumes = append(b.volumes, corev1.Volume{
		Name: "github-token",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: ref.Name,
				Items:      []corev1.KeyToPath{{Key: ref.Key, Path: "token"}},
				Optional:   ref.Optional,
			},
		},
	})
	b.mounts = append(b.mounts, corev1.VolumeMount{
		Name:      "github-token",
		MountPath: GitHubTokenDir,
		ReadOnly:  true,
	})
	b.env = append(b.env, corev1.EnvVar{Name: "GITHUB_TOKEN_FILE", Value: GitHubTokenFile})
	return b
}

// WithArsenal adds emptyDir volumes for git-sync and skills.
func (b *PodBuilder) WithArsenal() *PodBuilder {
	b.volumes = append(b.volumes,
		corev1.Volume{
			Name:         "arsenal",
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		},
		corev1.Volume{
			Name:         "skills",
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		},
	)
	b.mounts = append(b.mounts,
		corev1.VolumeMount{Name: "arsenal", MountPath: "/arsenal", ReadOnly: true},
		corev1.VolumeMount{Name: "skills", MountPath: "/skills", ReadOnly: true},
	)
	return b
}

const (
	defaultArsenalRepo                = "https://github.com/roundtable-k8s/arsenal"
	defaultArsenalRef                 = "main"
	defaultArsenalPeriodSeconds int32 = 300
	defaultGitSyncImage               = "registry.k8s.io/git-sync/git-sync:v4.4.0"
)

// resolvedArsenal is a KnightArsenal with defaults applied and its on-disk path.
type resolvedArsenal struct {
	aiv1alpha1.KnightArsenal
	// path is where the synced tree appears inside the arsenal volume.
	path string
	// primary is true for spec.arsenal, whose sidecar keeps the name "git-sync".
	primary bool
}

// arsenalName derives a DNS-label-ish name from a repo URL's basename.
func arsenalName(repo string) string {
	parts := strings.Split(strings.TrimSuffix(strings.TrimSuffix(repo, "/"), ".git"), "/")
	name := strings.ToLower(parts[len(parts)-1])
	var sb strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			sb.WriteRune(r)
		} else {
			sb.WriteRune('-')
		}
	}
	name = strings.Trim(sb.String(), "-")
	if name == "" {
		name = "arsenal"
	}
	if len(name) > 40 {
		name = strings.TrimRight(name[:40], "-")
	}
	return name
}

// arsenals returns spec.arsenal (if set) followed by spec.arsenals, with
// defaults applied, in merge order. Duplicate names get a numeric suffix.
func (b *PodBuilder) arsenals() []resolvedArsenal {
	var in []aiv1alpha1.KnightArsenal
	primary := b.knight.Spec.Arsenal != nil
	if primary {
		in = append(in, *b.knight.Spec.Arsenal)
	}
	in = append(in, b.knight.Spec.Arsenals...)

	out := make([]resolvedArsenal, 0, len(in))
	seen := map[string]int{}
	for i, a := range in {
		if a.Repo == "" {
			a.Repo = defaultArsenalRepo
		}
		if a.Ref == "" {
			a.Ref = defaultArsenalRef
		}
		if a.PeriodSeconds <= 0 {
			a.PeriodSeconds = defaultArsenalPeriodSeconds
		}
		if a.Image == "" {
			a.Image = defaultGitSyncImage
		}
		if a.Name == "" {
			a.Name = arsenalName(a.Repo)
		}
		if n := seen[a.Name]; n > 0 {
			seen[a.Name] = n + 1
			a.Name = fmt.Sprintf("%s-%d", a.Name, n+1)
		} else {
			seen[a.Name] = 1
		}
		out = append(out, resolvedArsenal{
			KnightArsenal: a,
			path:          "/arsenal/" + a.Name,
			primary:       primary && i == 0,
		})
	}
	return out
}

// WithSkillFilter adds the skill-filter sidecar container. It merges every
// arsenal into /skills per skill: /skills/<category>/<skill> is a symlink into
// the last arsenal (in merge order) that provides <category>/<skill>.
func (b *PodBuilder) WithSkillFilter() *PodBuilder {
	skillCategories := strings.Join(b.knight.Spec.Skills, " ")

	arsenals := b.arsenals()
	paths := make([]string, 0, len(arsenals))
	for _, a := range arsenals {
		paths = append(paths, a.path)
	}
	if len(paths) == 0 {
		// No arsenal configured: nothing syncs, but keep the sidecar so a
		// manually populated /arsenal still works.
		paths = []string{"/arsenal"}
	}

	skillFilterScript := fmt.Sprintf(`
ARSENALS="%s"
TARGET="/skills"
SKILL_CATEGORIES="%s"`, strings.Join(paths, " "), skillCategories) + `
# PID 1 ignores SIGTERM unless a handler is installed; without this the pod
# stays Terminating for the full grace period. Sleeps run in the background
# and are waited on so the trap fires immediately.
trap 'exit 0' TERM INT
# Wait until every arsenal has completed its first sync.
until ready=1; for a in $ARSENALS; do [ -d "$a" ] || ready=0; done; [ "$ready" = 1 ]; do
  sleep 2 & wait $!
done
echo "Arsenals synced: $ARSENALS"
sync_skills() {
  for cat in $SKILL_CATEGORIES; do
    dst_dir="$TARGET/$cat"
    # Older layouts linked the whole category; replace with a real dir.
    [ -L "$dst_dir" ] && rm -f "$dst_dir"
    mkdir -p "$dst_dir"
    # Later arsenals override earlier ones: record the winning source per entry.
    want=""
    for a in $ARSENALS; do
      [ -d "$a/$cat" ] || continue
      for src in "$a/$cat"/*; do
        [ -e "$src" ] || continue
        name="${src##*/}"
        want="$(echo "$want" | awk -v n="$name" '$1 != n')
$name $src"
      done
    done
    echo "$want" | while read -r name src; do
      [ -n "$name" ] || continue
      current=$(readlink "$dst_dir/$name" 2>/dev/null || echo "")
      if [ "$current" != "$src" ]; then
        ln -sfn "$src" "$dst_dir/$name"
        echo "Linked $cat/$name -> $src"
      fi
    done
    # Prune links for skills no arsenal provides any more.
    for link in "$dst_dir"/*; do
      [ -L "$link" ] || continue
      name="${link##*/}"
      echo "$want" | awk -v n="$name" '$1 == n {f=1} END {exit !f}' || { rm -f "$link"; echo "Unlinked $cat/$name"; }
    done
  done
}
sync_skills
for cat in $SKILL_CATEGORIES; do
  [ -n "$(ls -A "$TARGET/$cat" 2>/dev/null)" ] || echo "WARN: category $cat not found in any arsenal"
done
echo "Skills linked"
[ -n "$SKILL_FILTER_ONCE" ] && exit 0
while true; do
  sleep 60 & wait $!
  sync_skills
done`

	skillFilterContainer := corev1.Container{
		Name:    "skill-filter",
		Image:   "alpine:3.21",
		Command: []string{"/bin/sh", "-c"},
		Args:    []string{skillFilterScript},
		Env: []corev1.EnvVar{
			{Name: "SKILL_CATEGORIES", Value: skillCategories},
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("8Mi"),
				corev1.ResourceCPU:    resource.MustParse("5m"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("16Mi"),
				corev1.ResourceCPU:    resource.MustParse("50m"),
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "arsenal", MountPath: "/arsenal", ReadOnly: true},
			{Name: "skills", MountPath: "/skills"},
		},
	}

	b.sidecars = append(b.sidecars, skillFilterContainer)
	return b
}

// WithGitSync adds one git-sync sidecar per arsenal. Each syncs under
// /arsenal/.sync/<name> and exposes the current tree at /arsenal/<name>
// (the same path the single-arsenal layout used). spec.arsenal's sidecar
// keeps the name "git-sync"; spec.arsenals entries are "git-sync-<name>".
func (b *PodBuilder) WithGitSync() *PodBuilder {
	for _, a := range b.arsenals() {
		name := "git-sync-" + a.Name
		if a.primary {
			name = "git-sync"
		}
		// Each arsenal gets its own git-sync root: git-sync may wipe a root
		// that isn't a clean repo, so roots must never nest or be shared.
		env := []corev1.EnvVar{
			{Name: "GITSYNC_REPO", Value: a.Repo},
			{Name: "GITSYNC_REF", Value: a.Ref},
			{Name: "GITSYNC_ROOT", Value: "/arsenal/.sync/" + a.Name},
			{Name: "GITSYNC_LINK", Value: a.path},
			{Name: "GITSYNC_PERIOD", Value: fmt.Sprintf("%ds", a.PeriodSeconds)},
		}

		b.sidecars = append(b.sidecars, corev1.Container{
			Name:  name,
			Image: a.Image,
			Env:   env,
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceMemory: resource.MustParse("32Mi"),
					corev1.ResourceCPU:    resource.MustParse("10m"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceMemory: resource.MustParse("64Mi"),
				},
			},
			VolumeMounts: []corev1.VolumeMount{
				{Name: "arsenal", MountPath: "/arsenal"},
			},
		})
	}
	return b
}

// WithBrowser adds a headless Chrome sidecar with agent-browser for web automation.
func (b *PodBuilder) WithBrowser() *PodBuilder {
	browserContainer := corev1.Container{
		Name:  "browser",
		Image: "docker.io/chromedp/headless-shell:latest",
		Ports: []corev1.ContainerPort{
			{ContainerPort: 9222, Name: "cdp", Protocol: corev1.ProtocolTCP},
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("50m"),
				corev1.ResourceMemory: resource.MustParse("128Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("512Mi"),
			},
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{
					Port: intstr.FromInt32(9222),
				},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       10,
		},
		SecurityContext: &corev1.SecurityContext{
			ReadOnlyRootFilesystem:   new(false),
			AllowPrivilegeEscalation: new(false),
		},
	}

	b.sidecars = append(b.sidecars, browserContainer)
	return b
}

// Build assembles the complete PodSpec with all configured components.
func (b *PodBuilder) Build(ctx context.Context) corev1.PodSpec {
	image := EffectiveImage(b.knight, b.defaultImg)

	// Build environment variables
	taskTimeoutMs := int64(b.knight.Spec.TaskTimeoutSeconds) * 1000
	env := []corev1.EnvVar{
		{Name: "KNIGHT_NAME", Value: util.Capitalize(b.knight.Name)},
		{Name: "KNIGHT_MODEL", Value: b.knight.Spec.Model},
		{Name: "NATS_URL", Value: b.knight.Spec.NATS.URL},
		{Name: "NATS_TASKS_STREAM", Value: b.knight.Spec.NATS.Stream},
		{Name: "NATS_RESULTS_STREAM", Value: b.knight.Spec.NATS.ResultsStream},
		{Name: "NATS_RESULTS_PREFIX", Value: DeriveResultsPrefix(b.knight.Spec.NATS.Subjects)},
		{Name: "SUBSCRIBE_TOPICS", Value: strings.Join(b.knight.Spec.NATS.Subjects, ",")},
		{Name: "MAX_CONCURRENT_TASKS", Value: fmt.Sprintf("%d", b.knight.Spec.Concurrency)},
		{Name: "TASK_TIMEOUT_MS", Value: fmt.Sprintf("%d", taskTimeoutMs)},
		{Name: "METRICS_PORT", Value: "3000"},
		{Name: "LOG_LEVEL", Value: "info"},
		{Name: "TZ", Value: "America/Chicago"},
		// PATH at the container level so exec shells and all subprocesses see
		// the knight's nix/mise tools (not just the entrypoint's process tree).
		{Name: "PATH", Value: knightToolPATH()},
	}

	// Browser capability
	if b.knight.Spec.Capabilities != nil && b.knight.Spec.Capabilities.Browser {
		env = append(env, corev1.EnvVar{Name: "BROWSER_ENABLED", Value: "true"})
		env = append(env, corev1.EnvVar{Name: "BROWSER_CDP_URL", Value: "http://localhost:9222"})
	}

	// Append user-defined env vars
	env = append(env, b.knight.Spec.Env...)
	env = append(env, b.env...)

	// Main knight container
	probePort := 3000
	knightContainer := corev1.Container{
		Name:         "app",
		Image:        image,
		Env:          env,
		EnvFrom:      append(append([]corev1.EnvFromSource{}, b.knight.Spec.EnvFrom...), b.envFrom...),
		Resources:    KnightResources(b.knight),
		VolumeMounts: b.mounts,
		StartupProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/health",
					Port: util.IntstrPort(probePort),
				},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       10,
			FailureThreshold:    60, // 10 minutes for Nix builds
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/health",
					Port: util.IntstrPort(probePort),
				},
			},
			PeriodSeconds: 30,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/ready",
					Port: util.IntstrPort(probePort),
				},
			},
			PeriodSeconds: 15,
		},
	}

	// Combine main container with sidecars
	containers := make([]corev1.Container, 0, 1+len(b.sidecars))
	containers = append(containers, knightContainer)
	containers = append(containers, b.sidecars...)

	return corev1.PodSpec{
		Containers:                   containers,
		Volumes:                      b.volumes,
		EnableServiceLinks:           new(false),
		SecurityContext:              b.security.PodSecurityContext(),
		ServiceAccountName:           b.knight.Spec.ServiceAccountName,
		AutomountServiceAccountToken: new(true),
	}
}

// DeriveResultsPrefix extracts the NATS subject prefix for results from task subjects.
// e.g., ["table-prefix.tasks.security.>"] → "table-prefix.results"
func DeriveResultsPrefix(subjects []string) string {
	for _, subj := range subjects {
		if strings.Contains(subj, ".tasks.") {
			parts := strings.SplitN(subj, ".tasks.", 2)
			if len(parts) == 2 {
				return parts[0] + ".results"
			}
		}
	}
	// Fallback: use first segment
	for _, subj := range subjects {
		parts := strings.Split(subj, ".")
		if len(parts) > 1 {
			return parts[0] + ".results"
		}
	}
	return ""
}

// KnightResources returns the app container's resource requirements:
// spec.resources verbatim when set, else the operator default (requests of
// 100m CPU and 256Mi memory, no limits).
func KnightResources(k *aiv1alpha1.Knight) corev1.ResourceRequirements {
	if k.Spec.Resources != nil {
		return *k.Spec.Resources.DeepCopy()
	}
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("256Mi"),
			corev1.ResourceCPU:    resource.MustParse("100m"),
		},
	}
}
