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
	"strings"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func TestKnight(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Knight Package Suite")
}

var _ = Describe("PodBuilder", func() {
	var knight *aiv1alpha1.Knight
	var builder *PodBuilder

	BeforeEach(func() {
		knight = &aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-knight",
				Namespace: "default",
			},
			Spec: aiv1alpha1.KnightSpec{
				Model:              "anthropic/claude-3-5-sonnet-20241022",
				Domain:             "testing",
				Concurrency:        1,
				TaskTimeoutSeconds: 300,
				Skills:             []string{"testing", "validation"},
				NATS: aiv1alpha1.KnightNATS{
					URL:           "nats://nats.default.svc:4222",
					Stream:        "test_tasks",
					ResultsStream: "test_results",
					Subjects:      []string{"test.tasks.testing.>"},
				},
			},
		}
		builder = NewPodBuilder(knight, "ghcr.io/roundtable-k8s/pi-knight:latest")
	})

	Describe("WithWorkspace", func() {
		It("adds workspace PVC volume and mount", func() {
			builder.WithWorkspace()

			Expect(builder.volumes).To(HaveLen(1))
			Expect(builder.volumes[0].Name).To(Equal("data"))
			Expect(builder.volumes[0].PersistentVolumeClaim.ClaimName).To(Equal("test-knight"))

			Expect(builder.mounts).To(HaveLen(1))
			Expect(builder.mounts[0].Name).To(Equal("data"))
			Expect(builder.mounts[0].MountPath).To(Equal("/data"))
		})

		It("uses existing claim if specified", func() {
			knight.Spec.Workspace = &aiv1alpha1.KnightWorkspace{
				ExistingClaim: "custom-pvc",
			}
			builder.WithWorkspace()

			Expect(builder.volumes[0].PersistentVolumeClaim.ClaimName).To(Equal("custom-pvc"))
		})
	})

	Describe("WithConfig", func() {
		It("adds config ConfigMap volume and mount", func() {
			builder.WithConfig("knight-test-knight-config")

			Expect(builder.volumes).To(HaveLen(1))
			Expect(builder.volumes[0].Name).To(Equal("config"))
			Expect(builder.volumes[0].ConfigMap.Name).To(Equal("knight-test-knight-config"))

			Expect(builder.mounts).To(HaveLen(1))
			Expect(builder.mounts[0].Name).To(Equal("config"))
			Expect(builder.mounts[0].MountPath).To(Equal("/config"))
			Expect(builder.mounts[0].ReadOnly).To(BeTrue())
		})
	})

	Describe("WithNixStore", func() {
		It("does nothing when no Nix tools configured", func() {
			builder.WithNixStore()
			Expect(builder.volumes).To(BeEmpty())
			Expect(builder.mounts).To(BeEmpty())
		})

		It("mounts nothing for tools.nix when no tools image repository is configured", func() {
			knight.Spec.Tools = &aiv1alpha1.KnightTools{Nix: []string{"kubectl", "helm"}}
			builder.WithNixStore()
			Expect(builder.volumes).To(BeEmpty())
			Expect(builder.mounts).To(BeEmpty())
		})
	})

	Describe("WithNixStore with a tools image repository", func() {
		BeforeEach(func() {
			GinkgoT().Setenv("KNIGHT_TOOLS_IMAGE_REPO", "registry.example.com/knight-tools")
		})

		It("does nothing when no Nix tools configured", func() {
			builder.WithNixStore()
			Expect(builder.volumes).To(BeEmpty())
			Expect(builder.mounts).To(BeEmpty())
		})

		It("mounts the knight's tools image read-only at /nix", func() {
			knight.Spec.Tools = &aiv1alpha1.KnightTools{Nix: []string{"kubectl", "helm"}}
			builder.WithNixStore()

			Expect(builder.volumes).To(HaveLen(1))
			vol := builder.volumes[0]
			Expect(vol.Name).To(Equal("nix"))
			Expect(vol.PersistentVolumeClaim).To(BeNil())
			Expect(vol.Image).NotTo(BeNil())
			Expect(vol.Image.Reference).To(Equal("registry.example.com/knight-tools:" + ToolsImageTag(knight)))
			Expect(vol.Image.PullPolicy).To(Equal(corev1.PullIfNotPresent))

			// subPath "nix": the image carries /nix/store and /nix/profile.
			Expect(builder.mounts).To(Equal([]corev1.VolumeMount{{
				Name: "nix", MountPath: "/nix", SubPath: "nix", ReadOnly: true,
			}}))
			Expect(builder.env).To(ContainElement(corev1.EnvVar{
				Name: "KNIGHT_NIX_PROFILE", Value: "/nix/profile",
			}))
		})

		It("keeps mounting the published image while a changed tool list is building", func() {
			knight.Spec.Tools = &aiv1alpha1.KnightTools{Nix: []string{"kubectl"}}
			published := ToolsImageRef(knight)
			knight.Status.ToolsImage = published
			knight.Spec.Tools.Nix = []string{"kubectl", "helm"}
			Expect(ToolsImageRef(knight)).NotTo(Equal(published))

			builder.WithNixStore()
			Expect(builder.volumes).To(HaveLen(1))
			Expect(builder.volumes[0].Image.Reference).To(Equal(published))
		})

		It("has the tools image profile on the container PATH", func() {
			Expect(strings.Split(knightToolPATH(), ":")).To(ContainElement("/nix/profile/bin"))
		})
	})

	Describe("WithVault", func() {
		It("does nothing when vault not configured", func() {
			builder.WithVault()
			Expect(builder.volumes).To(BeEmpty())
			Expect(builder.mounts).To(BeEmpty())
		})

		It("adds read-only vault mount", func() {
			knight.Spec.Vault = &aiv1alpha1.KnightVault{
				ClaimName: "my-vault",
				ReadOnly:  new(true),
			}
			builder.WithVault()

			Expect(builder.volumes).To(HaveLen(1))
			Expect(builder.volumes[0].Name).To(Equal("vault"))
			Expect(builder.volumes[0].PersistentVolumeClaim.ClaimName).To(Equal("my-vault"))
			Expect(builder.volumes[0].PersistentVolumeClaim.ReadOnly).To(BeTrue())

			Expect(builder.mounts).To(HaveLen(1))
			Expect(builder.mounts[0].MountPath).To(Equal("/vault"))
			Expect(builder.mounts[0].ReadOnly).To(BeTrue())
		})

		It("adds writable subpaths correctly", func() {
			knight.Spec.Vault = &aiv1alpha1.KnightVault{
				ClaimName:     "my-vault",
				ReadOnly:      new(true),
				WritablePaths: []string{"logs/", "temp"},
			}
			builder.WithVault()

			// PVC must be non-readonly when writable paths exist
			Expect(builder.volumes[0].PersistentVolumeClaim.ReadOnly).To(BeFalse())

			// Should have 3 mounts: base + 2 writable subpaths
			Expect(builder.mounts).To(HaveLen(3))

			// Base mount is still read-only
			Expect(builder.mounts[0].MountPath).To(Equal("/vault"))
			Expect(builder.mounts[0].ReadOnly).To(BeTrue())

			// Writable subpaths
			Expect(builder.mounts[1].MountPath).To(Equal("/vault/logs"))
			Expect(builder.mounts[1].SubPath).To(Equal("logs"))
			Expect(builder.mounts[1].ReadOnly).To(BeFalse())

			Expect(builder.mounts[2].MountPath).To(Equal("/vault/temp"))
			Expect(builder.mounts[2].SubPath).To(Equal("temp"))
			Expect(builder.mounts[2].ReadOnly).To(BeFalse())
		})

		It("treats an unset readOnly as read-only with a single mount", func() {
			knight.Spec.Vault = &aiv1alpha1.KnightVault{ClaimName: "my-vault"}
			builder.WithVault()

			Expect(builder.volumes[0].PersistentVolumeClaim.ReadOnly).To(BeTrue())
			Expect(builder.mounts).To(HaveLen(1))
			Expect(builder.mounts[0].ReadOnly).To(BeTrue())
		})

		It("honours readOnly false", func() {
			knight.Spec.Vault = &aiv1alpha1.KnightVault{ClaimName: "my-vault", ReadOnly: new(false)}
			builder.WithVault()

			Expect(builder.volumes[0].PersistentVolumeClaim.ReadOnly).To(BeFalse())
			Expect(builder.mounts).To(HaveLen(1))
			Expect(builder.mounts[0].ReadOnly).To(BeFalse())
		})

		It("uses default vault claim name", func() {
			knight.Spec.Vault = &aiv1alpha1.KnightVault{
				ReadOnly: new(true),
			}
			builder.WithVault()

			Expect(builder.volumes[0].PersistentVolumeClaim.ClaimName).To(Equal("obsidian-vault"))
		})
	})

	Describe("WithArsenal", func() {
		It("adds arsenal and skills emptyDir volumes", func() {
			builder.WithArsenal()

			Expect(builder.volumes).To(HaveLen(2))
			Expect(builder.volumes[0].Name).To(Equal("arsenal"))
			Expect(builder.volumes[0].EmptyDir).NotTo(BeNil())
			Expect(builder.volumes[1].Name).To(Equal("skills"))
			Expect(builder.volumes[1].EmptyDir).NotTo(BeNil())

			Expect(builder.mounts).To(HaveLen(2))
			Expect(builder.mounts[0].Name).To(Equal("arsenal"))
			Expect(builder.mounts[0].MountPath).To(Equal("/arsenal"))
			Expect(builder.mounts[0].ReadOnly).To(BeTrue())
			Expect(builder.mounts[1].Name).To(Equal("skills"))
			Expect(builder.mounts[1].MountPath).To(Equal("/skills"))
			Expect(builder.mounts[1].ReadOnly).To(BeTrue())
		})
	})

	Describe("WithSkillFilter", func() {
		It("adds skill-filter sidecar container", func() {
			builder.WithSkillFilter()

			Expect(builder.sidecars).To(HaveLen(1))
			Expect(builder.sidecars[0].Name).To(Equal("skill-filter"))
			Expect(builder.sidecars[0].Image).To(Equal("alpine:3.21"))

			// Check it has the right environment variable
			Expect(builder.sidecars[0].Env).To(ContainElement(
				corev1.EnvVar{Name: "SKILL_CATEGORIES", Value: "testing validation"},
			))

			// Check it mounts arsenal and skills
			Expect(builder.sidecars[0].VolumeMounts).To(HaveLen(2))
			Expect(builder.sidecars[0].VolumeMounts[0].Name).To(Equal("arsenal"))
			Expect(builder.sidecars[0].VolumeMounts[1].Name).To(Equal("skills"))
		})
	})

	Describe("WithGitSync", func() {
		It("does nothing when arsenal not configured", func() {
			builder.WithGitSync()
			Expect(builder.sidecars).To(BeEmpty())
		})

		It("adds git-sync sidecar with defaults", func() {
			knight.Spec.Arsenal = &aiv1alpha1.KnightArsenal{}
			builder.WithGitSync()

			Expect(builder.sidecars).To(HaveLen(1))
			Expect(builder.sidecars[0].Name).To(Equal("git-sync"))
			Expect(builder.sidecars[0].Image).To(Equal("registry.k8s.io/git-sync/git-sync:v4.4.0"))

			// Check default env vars
			envMap := make(map[string]string)
			for _, e := range builder.sidecars[0].Env {
				envMap[e.Name] = e.Value
			}
			Expect(envMap["GITSYNC_REPO"]).To(Equal("https://github.com/roundtable-k8s/arsenal"))
			Expect(envMap["GITSYNC_REF"]).To(Equal("main"))
			Expect(envMap["GITSYNC_ROOT"]).To(Equal("/arsenal/.sync/arsenal"))
			Expect(envMap["GITSYNC_LINK"]).To(Equal("/arsenal/arsenal"))
			Expect(envMap["GITSYNC_PERIOD"]).To(Equal("300s"))
		})

		It("adds one git-sync sidecar per arsenal with isolated roots", func() {
			knight.Spec.Arsenal = &aiv1alpha1.KnightArsenal{}
			knight.Spec.Arsenals = []aiv1alpha1.KnightArsenal{
				{Name: "personal", Repo: "https://github.com/someone/roundtable-arsenal.git", Ref: "dev"},
			}
			builder.WithGitSync()

			Expect(builder.sidecars).To(HaveLen(2))
			Expect(builder.sidecars[0].Name).To(Equal("git-sync"))
			Expect(builder.sidecars[1].Name).To(Equal("git-sync-personal"))

			envMap := make(map[string]string)
			for _, e := range builder.sidecars[1].Env {
				envMap[e.Name] = e.Value
			}
			Expect(envMap["GITSYNC_REPO"]).To(Equal("https://github.com/someone/roundtable-arsenal.git"))
			Expect(envMap["GITSYNC_REF"]).To(Equal("dev"))
			Expect(envMap["GITSYNC_ROOT"]).To(Equal("/arsenal/.sync/personal"))
			Expect(envMap["GITSYNC_LINK"]).To(Equal("/arsenal/personal"))
			Expect(envMap["GITSYNC_PERIOD"]).To(Equal("300s"))
		})

		It("supports arsenals without a primary arsenal and dedupes derived names", func() {
			knight.Spec.Arsenals = []aiv1alpha1.KnightArsenal{
				{Repo: "https://github.com/a/skills"},
				{Repo: "https://github.com/b/skills.git"},
			}
			builder.WithGitSync()

			Expect(builder.sidecars).To(HaveLen(2))
			Expect(builder.sidecars[0].Name).To(Equal("git-sync-skills"))
			Expect(builder.sidecars[1].Name).To(Equal("git-sync-skills-2"))
		})

		It("uses custom arsenal configuration", func() {
			knight.Spec.Arsenal = &aiv1alpha1.KnightArsenal{
				Repo:          "https://github.com/custom/arsenal",
				Ref:           "develop",
				PeriodSeconds: 60,
				Image:         "custom/git-sync:v1.0",
			}
			builder.WithGitSync()

			envMap := make(map[string]string)
			for _, e := range builder.sidecars[0].Env {
				envMap[e.Name] = e.Value
			}
			Expect(builder.sidecars[0].Image).To(Equal("custom/git-sync:v1.0"))
			Expect(envMap["GITSYNC_REPO"]).To(Equal("https://github.com/custom/arsenal"))
			Expect(envMap["GITSYNC_REF"]).To(Equal("develop"))
			Expect(envMap["GITSYNC_PERIOD"]).To(Equal("60s"))
		})
	})

	Describe("telemetry env", func() {
		appEnv := func() map[string]corev1.EnvVar {
			spec := builder.Build(context.Background())
			m := map[string]corev1.EnvVar{}
			for _, c := range spec.Containers {
				if c.Name == "app" {
					for _, e := range c.Env {
						m[e.Name] = e
					}
				} else {
					for _, e := range c.Env {
						Expect(e.Name).NotTo(HavePrefix("OTEL_"))
					}
				}
			}
			return m
		}
		tel := Telemetry{
			Endpoint: "http://otel:4318", Protocol: "http/protobuf",
			Sampler: "parentbased_traceidratio", SamplerArg: "0.5",
			ResourceAttrs: "deployment.environment=prod",
		}

		It("injects endpoint, sampler and knight attributes", func() {
			knight.Labels = map[string]string{aiv1alpha1.LabelRoundTable: "camelot"}
			builder.WithTelemetry(tel)
			env := appEnv()
			Expect(env["OTEL_EXPORTER_OTLP_ENDPOINT"].Value).To(Equal("http://otel:4318"))
			Expect(env["OTEL_TRACES_SAMPLER"].Value).To(Equal("parentbased_traceidratio"))
			Expect(env["OTEL_TRACES_SAMPLER_ARG"].Value).To(Equal("0.5"))
			Expect(env["ROUNDTABLE_TELEMETRY_CAPTURE_CONTENT"].Value).To(Equal("true"))
			Expect(env).NotTo(HaveKey("OTEL_SERVICE_NAME"))
			Expect(env).NotTo(HaveKey("OTEL_SDK_DISABLED"))
			attrs := env["OTEL_RESOURCE_ATTRIBUTES"].Value
			Expect(attrs).To(ContainSubstring("deployment.environment=prod"))
			Expect(attrs).To(ContainSubstring("roundtable.knight=test-knight"))
			Expect(attrs).To(ContainSubstring("roundtable.table=camelot"))
			Expect(attrs).To(ContainSubstring("k8s.namespace.name=$(POD_NAMESPACE)"))
			Expect(attrs).To(ContainSubstring("k8s.pod.name=$(POD_NAME)"))
			Expect(attrs).To(ContainSubstring("k8s.node.name=$(NODE_NAME)"))
			Expect(attrs).NotTo(ContainSubstring("roundtable.mission"))
			Expect(env["POD_NAME"].ValueFrom).NotTo(BeNil())
		})

		It("adds the mission for mission knights", func() {
			knight.Labels = map[string]string{aiv1alpha1.LabelMission: "m1"}
			builder.WithTelemetry(tel)
			Expect(appEnv()["OTEL_RESOURCE_ATTRIBUTES"].Value).To(ContainSubstring("roundtable.mission=m1"))
		})

		It("sets only OTEL_SDK_DISABLED=true without an endpoint", func() {
			builder.WithTelemetry(Telemetry{})
			env := appEnv()
			Expect(env).To(HaveKeyWithValue("OTEL_SDK_DISABLED", corev1.EnvVar{Name: "OTEL_SDK_DISABLED", Value: "true"}))
			for name := range env {
				if strings.HasPrefix(name, "OTEL_") {
					Expect(name).To(Equal("OTEL_SDK_DISABLED"))
				}
			}
		})

		It("lets the knight's own env win", func() {
			knight.Spec.Env = []corev1.EnvVar{{Name: "OTEL_TRACES_SAMPLER", Value: "always_off"}}
			builder.WithTelemetry(tel)
			spec := builder.Build(context.Background())
			n := 0
			for _, c := range spec.Containers {
				if c.Name != "app" {
					continue
				}
				for _, e := range c.Env {
					if e.Name == "OTEL_TRACES_SAMPLER" {
						n++
						Expect(e.Value).To(Equal("always_off"))
					}
				}
			}
			Expect(n).To(Equal(1))
		})
	})

	Describe("ROUNDTABLE_TABLE", func() {
		envOf := func() map[string]string {
			spec := builder.Build(context.Background())
			m := map[string]string{}
			for _, c := range spec.Containers {
				if c.Name == "app" {
					for _, e := range c.Env {
						m[e.Name] = e.Value
					}
				}
			}
			return m
		}

		It("is the knight's round-table label", func() {
			knight.Labels = map[string]string{aiv1alpha1.LabelRoundTable: "x"}
			Expect(envOf()).To(HaveKeyWithValue("ROUNDTABLE_TABLE", "x"))
		})

		It("falls back to the legacy table label", func() {
			knight.Labels = map[string]string{LegacyTableLabel: "legacy"}
			Expect(envOf()).To(HaveKeyWithValue("ROUNDTABLE_TABLE", "legacy"))
		})

		It("is empty when the knight has no table", func() {
			Expect(envOf()).To(HaveKeyWithValue("ROUNDTABLE_TABLE", ""))
		})

		It("sets KNIGHT_CARDS_BUCKET to the default cards bucket", func() {
			Expect(envOf()).To(HaveKeyWithValue("KNIGHT_CARDS_BUCKET", "roundtable-cards"))
		})

		It("sets KNIGHT_CARDS_BUCKET to the configured cards bucket", func() {
			builder.WithCardsBucket("my-cards")
			Expect(envOf()).To(HaveKeyWithValue("KNIGHT_CARDS_BUCKET", "my-cards"))
		})
	})

	Describe("Build", func() {
		It("creates a valid PodSpec with security context", func() {
			builder.WithWorkspace().WithConfig("test-config")
			spec := builder.Build(context.Background())

			// Check security context
			Expect(spec.SecurityContext).NotTo(BeNil())
			Expect(*spec.SecurityContext.RunAsUser).To(Equal(int64(1000)))
			Expect(*spec.SecurityContext.RunAsGroup).To(Equal(int64(1000)))
			Expect(*spec.SecurityContext.FSGroup).To(Equal(int64(1000)))
			Expect(*spec.SecurityContext.FSGroupChangePolicy).To(Equal(corev1.FSGroupChangeOnRootMismatch))

			// Check service links disabled
			Expect(*spec.EnableServiceLinks).To(BeFalse())

			// Check automount service account token
			Expect(*spec.AutomountServiceAccountToken).To(BeTrue())
		})

		It("gives the app container the default requests when spec.resources is unset", func() {
			spec := builder.Build(context.Background())
			res := spec.Containers[0].Resources
			Expect(res.Requests.Cpu().String()).To(Equal("100m"))
			Expect(res.Requests.Memory().String()).To(Equal("256Mi"))
			Expect(res.Limits).To(BeEmpty())
		})

		It("applies spec.resources verbatim to the app container", func() {
			knight.Spec.Resources = &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("500m"),
					corev1.ResourceMemory: resource.MustParse("1Gi"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("2"),
					corev1.ResourceMemory: resource.MustParse("4Gi"),
				},
			}
			spec := builder.Build(context.Background())
			Expect(spec.Containers[0].Resources).To(Equal(*knight.Spec.Resources))
			// A copy, not an alias of the Knight's spec.
			spec.Containers[0].Resources.Limits[corev1.ResourceCPU] = resource.MustParse("1")
			Expect(knight.Spec.Resources.Limits.Cpu().String()).To(Equal("2"))
		})

		It("sets a container PATH env exposing nix/mise tools to all processes", func() {
			builder.WithWorkspace().WithConfig("test-config")
			spec := builder.Build(context.Background())

			var path string
			for _, e := range spec.Containers[0].Env {
				if e.Name == "PATH" {
					path = e.Value
				}
			}
			Expect(path).To(ContainSubstring("/home/node/.nix-profile/bin"))
			Expect(path).To(ContainSubstring("/nix/profile/bin"))
			Expect(path).NotTo(ContainSubstring("/nix/var/nix/profiles"))
			Expect(path).To(ContainSubstring("/data/.mise/shims"))
			Expect(path).To(ContainSubstring("/usr/local/bin")) // base PATH preserved
		})

		It("creates main container with proper configuration", func() {
			builder.WithWorkspace()
			spec := builder.Build(context.Background())

			Expect(spec.Containers).To(HaveLen(1))
			mainContainer := spec.Containers[0]

			Expect(mainContainer.Name).To(Equal("app"))
			Expect(mainContainer.Image).To(Equal("ghcr.io/roundtable-k8s/pi-knight:latest"))

			// Check env vars
			envMap := make(map[string]string)
			for _, e := range mainContainer.Env {
				envMap[e.Name] = e.Value
			}
			Expect(envMap["KNIGHT_NAME"]).To(Equal("Test-knight"))
			Expect(envMap["KNIGHT_MODEL"]).To(Equal("anthropic/claude-3-5-sonnet-20241022"))
			Expect(envMap["NATS_URL"]).To(Equal("nats://nats.default.svc:4222"))
			Expect(envMap["MAX_CONCURRENT_TASKS"]).To(Equal("1"))
			Expect(envMap["TASK_TIMEOUT_MS"]).To(Equal("300000"))

			// Check probes
			Expect(mainContainer.StartupProbe).NotTo(BeNil())
			Expect(mainContainer.LivenessProbe).NotTo(BeNil())
			Expect(mainContainer.ReadinessProbe).NotTo(BeNil())
			Expect(mainContainer.StartupProbe.FailureThreshold).To(Equal(int32(60)))
		})

		It("injects pod identity env from the downward API", func() {
			spec := builder.Build(context.Background())
			want := map[string]string{
				"POD_NAME":      "metadata.name",
				"POD_NAMESPACE": "metadata.namespace",
				"NODE_NAME":     "spec.nodeName",
			}
			got := map[string]string{}
			for _, e := range spec.Containers[0].Env {
				if _, ok := want[e.Name]; ok && e.ValueFrom != nil && e.ValueFrom.FieldRef != nil {
					got[e.Name] = e.ValueFrom.FieldRef.FieldPath
				}
			}
			Expect(got).To(Equal(want))
		})

		It("uses custom image if specified", func() {
			knight.Spec.Image = "custom/knight:v1.0"
			spec := builder.Build(context.Background())

			Expect(spec.Containers[0].Image).To(Equal("custom/knight:v1.0"))
		})

		It("includes sidecars when configured", func() {
			builder.WithArsenal().WithSkillFilter()
			knight.Spec.Arsenal = &aiv1alpha1.KnightArsenal{}
			builder.WithGitSync()

			spec := builder.Build(context.Background())

			// Should have main + skill-filter + git-sync
			Expect(spec.Containers).To(HaveLen(3))
			Expect(spec.Containers[0].Name).To(Equal("app"))
			Expect(spec.Containers[1].Name).To(Equal("skill-filter"))
			Expect(spec.Containers[2].Name).To(Equal("git-sync"))
		})

		It("includes all volumes from With* methods", func() {
			builder.
				WithWorkspace().
				WithConfig("test-config").
				WithArsenal()

			GinkgoT().Setenv("KNIGHT_TOOLS_IMAGE_REPO", "registry.example.com/knight-tools")
			knight.Spec.Tools = &aiv1alpha1.KnightTools{Nix: []string{"kubectl"}}
			builder.WithNixStore()

			spec := builder.Build(context.Background())

			// data, config, arsenal, skills, nix
			Expect(spec.Volumes).To(HaveLen(5))

			volumeNames := make([]string, len(spec.Volumes))
			for i, v := range spec.Volumes {
				volumeNames[i] = v.Name
			}
			Expect(volumeNames).To(ConsistOf("data", "config", "arsenal", "skills", "nix"))
		})
	})
})

var _ = Describe("PodBuilder restricted security", func() {
	It("gives every container, with all sidecars enabled, the restricted context", func() {
		GinkgoT().Setenv("KNIGHT_TOOLS_IMAGE_REPO", "registry.example.com/knight-tools")
		k := &aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{Name: "k", Namespace: "default"},
			Spec: aiv1alpha1.KnightSpec{
				Model:   "m",
				Domain:  "d",
				Skills:  []string{"a"},
				Arsenal: &aiv1alpha1.KnightArsenal{},
				Tools:   &aiv1alpha1.KnightTools{Nix: []string{"kubectl"}},
			},
		}
		b := NewPodBuilder(k, "img:1").WithWorkspace().WithArsenal().WithSkillFilter().WithGitSync().WithBrowser().WithNixStore()
		spec := b.Build(context.Background())

		Expect(len(spec.Containers)).To(BeNumerically(">=", 4))
		Expect(spec.SecurityContext.SeccompProfile).NotTo(BeNil())
		Expect(spec.SecurityContext.SeccompProfile.Type).To(Equal(corev1.SeccompProfileTypeRuntimeDefault))
		all := append(append([]corev1.Container{}, spec.InitContainers...), spec.Containers...)
		for _, c := range all {
			sc := c.SecurityContext
			Expect(sc).NotTo(BeNil(), c.Name)
			Expect(sc.AllowPrivilegeEscalation).To(HaveValue(BeFalse()), c.Name)
			Expect(sc.RunAsNonRoot).To(HaveValue(BeTrue()), c.Name)
			Expect(sc.Capabilities).NotTo(BeNil(), c.Name)
			Expect(sc.Capabilities.Drop).To(ConsistOf(corev1.Capability("ALL")), c.Name)
			Expect(sc.Capabilities.Add).To(BeEmpty(), c.Name)
			Expect(sc.SeccompProfile).NotTo(BeNil(), c.Name)
			Expect(sc.SeccompProfile.Type).To(Equal(corev1.SeccompProfileTypeRuntimeDefault), c.Name)
			Expect(sc.Privileged).To(BeNil(), c.Name)
		}
	})
})

var _ = Describe("Knight memory volume (#388)", func() {
	ctx := context.Background()

	newKnight := func(memory *aiv1alpha1.KnightMemory) *aiv1alpha1.Knight {
		return &aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{
				Name: "merlin", Namespace: "default",
				Labels: map[string]string{aiv1alpha1.LabelRoundTable: "mem-table"},
			},
			Spec: aiv1alpha1.KnightSpec{
				Domain: "d", Skills: []string{"s"},
				NATS:   aiv1alpha1.KnightNATS{URL: "nats://x:4222", Stream: "s", ResultsStream: "r", Subjects: []string{"a.>"}},
				Memory: memory,
			},
		}
	}
	build := func(k *aiv1alpha1.Knight) corev1.PodSpec {
		return NewPodBuilder(k, "img").WithWorkspace().WithMemory().Build(ctx)
	}
	memoryVolumes := func(spec corev1.PodSpec) []corev1.Volume {
		var out []corev1.Volume
		for _, v := range spec.Volumes {
			if v.Name == "memory" {
				out = append(out, v)
			}
		}
		return out
	}
	memoryMounts := func(spec corev1.PodSpec) []corev1.VolumeMount {
		var out []corev1.VolumeMount
		for _, m := range spec.Containers[0].VolumeMounts {
			if m.Name == "memory" {
				out = append(out, m)
			}
		}
		return out
	}
	env := func(spec corev1.PodSpec) map[string]string {
		out := map[string]string{}
		for _, e := range spec.Containers[0].Env {
			out[e.Name] = e.Value
		}
		return out
	}

	It("adds no volume, mount or env without spec.memory", func() {
		spec := build(newKnight(nil))
		Expect(memoryVolumes(spec)).To(BeEmpty())
		Expect(memoryMounts(spec)).To(BeEmpty())
		Expect(env(spec)).NotTo(HaveKey("KNIGHT_MEMORY_DIR"))
		Expect(env(spec)).NotTo(HaveKey("KNIGHT_MEMORY_ROOT"))
	})

	It("mounts the per-knight claim read-write at /memory/<knight>", func() {
		spec := build(newKnight(&aiv1alpha1.KnightMemory{}))

		vols := memoryVolumes(spec)
		Expect(vols).To(HaveLen(1))
		Expect(vols[0].PersistentVolumeClaim).NotTo(BeNil())
		Expect(vols[0].PersistentVolumeClaim.ClaimName).To(Equal("merlin-memory"))
		Expect(vols[0].PersistentVolumeClaim.ReadOnly).To(BeFalse())

		Expect(memoryMounts(spec)).To(ConsistOf(corev1.VolumeMount{Name: "memory", MountPath: "/memory/merlin"}))
		Expect(env(spec)).To(HaveKeyWithValue("KNIGHT_MEMORY_DIR", "/memory/merlin"))
		Expect(env(spec)).To(HaveKeyWithValue("KNIGHT_MEMORY_ROOT", "/memory"))
	})

	It("mounts an existingClaim read-only at /memory and its <knight> subPath read-write", func() {
		spec := build(newKnight(&aiv1alpha1.KnightMemory{ExistingClaim: "table-memory"}))

		vols := memoryVolumes(spec)
		Expect(vols).To(HaveLen(1))
		Expect(vols[0].PersistentVolumeClaim.ClaimName).To(Equal("table-memory"))
		// The volume itself is writable so the subPath mount can be.
		Expect(vols[0].PersistentVolumeClaim.ReadOnly).To(BeFalse())

		Expect(memoryMounts(spec)).To(ConsistOf(
			corev1.VolumeMount{Name: "memory", MountPath: "/memory", ReadOnly: true},
			corev1.VolumeMount{Name: "memory", MountPath: "/memory/merlin", SubPath: "merlin"},
		))
		Expect(env(spec)).To(HaveKeyWithValue("KNIGHT_MEMORY_DIR", "/memory/merlin"))
		Expect(env(spec)).To(HaveKeyWithValue("KNIGHT_MEMORY_ROOT", "/memory"))
	})

	Describe("table defaults", func() {
		reader := func(memory *aiv1alpha1.KnightMemory) *fake.ClientBuilder {
			s := runtime.NewScheme()
			Expect(aiv1alpha1.AddToScheme(s)).To(Succeed())
			rt := &aiv1alpha1.RoundTable{
				ObjectMeta: metav1.ObjectMeta{Name: "mem-table", Namespace: "default"},
				Spec:       aiv1alpha1.RoundTableSpec{Defaults: &aiv1alpha1.RoundTableDefaults{Memory: memory}},
			}
			return fake.NewClientBuilder().WithScheme(s).WithObjects(rt)
		}

		It("inherits the table's memory, existingClaim included, when the knight leaves it unset", func() {
			k := newKnight(nil)
			ApplyTableDefaults(ctx, reader(&aiv1alpha1.KnightMemory{ExistingClaim: "table-memory"}).Build(), k)

			Expect(k.Spec.Memory).To(Equal(&aiv1alpha1.KnightMemory{ExistingClaim: "table-memory"}))
			Expect(memoryMounts(build(k))).To(HaveLen(2))
		})

		It("keeps the knight's own memory", func() {
			size := resource.MustParse("5Gi")
			own := &aiv1alpha1.KnightMemory{Size: &size, StorageClassName: "cephfs"}
			k := newKnight(own.DeepCopy())
			ApplyTableDefaults(ctx, reader(&aiv1alpha1.KnightMemory{ExistingClaim: "table-memory"}).Build(), k)

			Expect(k.Spec.Memory).To(Equal(own))
			Expect(memoryVolumes(build(k))[0].PersistentVolumeClaim.ClaimName).To(Equal("merlin-memory"))
		})

		It("adds no memory when neither the knight nor the table sets it", func() {
			k := newKnight(nil)
			ApplyTableDefaults(ctx, reader(nil).Build(), k)
			Expect(k.Spec.Memory).To(BeNil())
		})
	})
})
