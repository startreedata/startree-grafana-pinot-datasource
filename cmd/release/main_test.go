package main

import (
	"os"
	"os/exec"
	"testing"
)

func TestNormalizeReleaseVersion(t *testing.T) {
	for _, version := range []string{"1.1.0", "v1.1.0", "0.0.0", "1.2.3-feature-123", "1.2.3-rc.0+build.01"} {
		t.Run(version, func(t *testing.T) {
			got, err := normalizeReleaseVersion(version)
			if err != nil || (got != version && "v"+got != version) {
				t.Fatalf("normalizeReleaseVersion(%q) = %q, %v", version, got, err)
			}
		})
	}
	for _, version := range []string{"", "1.1", "01.1.0", "1.2.3-01", "1.2.3-rc..1", "1.2.3+", "1.2.3/foo", "../1.2.3", "1.2.3\n", "v1.2.3.zip"} {
		t.Run("invalid_"+version, func(t *testing.T) {
			if _, err := normalizeReleaseVersion(version); err == nil {
				t.Fatalf("normalizeReleaseVersion(%q) succeeded", version)
			}
		})
	}
}

func TestReleaseArchiveNamesUseExplicitVersion(t *testing.T) {
	// An explicit artifact version must work even without a Git checkout or tag.
	t.Chdir(t.TempDir())
	t.Setenv("RELEASE_NAME", "example")
	for _, version := range []string{"1.1.0", "1.2.3-feature-123+build.4"} {
		t.Setenv("RELEASE_VERSION", "v"+version)
		manager := new(ReleaseManager)
		if got := manager.getInternalArchiveName(); got != PluginName+"-"+version+".zip" {
			t.Fatalf("internal archive = %q", got)
		}
		// Keep the customer archive tied to the source version selected above.
		t.Setenv("RELEASE_VERSION", "2.0.0")
		if got := manager.getCustomerArchiveName(); got != PluginName+"-"+version+"-example.zip" {
			t.Fatalf("customer archive = %q", got)
		}
	}
}

func TestReleaseVersionDefaultsToLatestTag(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("RELEASE_VERSION", "")
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	for _, args := range [][]string{
		{"init", "--quiet"},
		{"-c", "user.name=Release Test", "-c", "user.email=release@example.com", "-c", "commit.gpgsign=false", "commit", "--quiet", "--allow-empty", "-m", "fixture"},
		{"tag", "v1.0.23"},
		{"tag", "v1.0.24"},
	} {
		if output, err := exec.Command("git", args...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, output)
		}
	}
	manager := new(ReleaseManager)
	if got := manager.getLatestReleaseVersion(); got != "1.0.24" {
		t.Fatalf("default version = %q, want 1.0.24", got)
	}
}
