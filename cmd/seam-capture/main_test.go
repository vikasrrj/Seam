package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestSeamCaptureCLIRejectsInvalidConfig runs the built capture binary with
// malformed configuration and requires a non-zero exit naming the variable.
func TestSeamCaptureCLIRejectsInvalidConfig(t *testing.T) {
	bin := buildBinary(t)
	cases := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{"malformed max tx events", map[string]string{"SEAM_MAX_TX_EVENTS": "abc"}, "SEAM_MAX_TX_EVENTS"},
		{"zero max tx events", map[string]string{"SEAM_MAX_TX_EVENTS": "0"}, "must be positive"},
		{"negative max tx events", map[string]string{"SEAM_MAX_TX_EVENTS": "-5"}, "must be positive"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, exit, _ := runBinary(t, bin, tc.env)
			if exit == 0 {
				t.Fatalf("expected non-zero exit; output:\n%s", out)
			}
			if !strings.Contains(out, tc.wantErr) {
				t.Fatalf("output does not contain %q:\n%s", tc.wantErr, out)
			}
		})
	}
}

func buildBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "seam-capture")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/seam-capture")
	cmd.Dir = repoRoot(t)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	return bin
}

// repoRoot walks up from the test working directory to the module root.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above working directory")
		}
		dir = parent
	}
}

func runBinary(t *testing.T, bin string, env map[string]string) (string, int, error) {
	t.Helper()
	cmd := exec.Command(bin)
	cmd.Env = os.Environ()
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	out, err := cmd.CombinedOutput()
	exit := 0
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			exit = ee.ExitCode()
		} else {
			return string(out), -1, err
		}
	}
	return string(out), exit, nil
}
