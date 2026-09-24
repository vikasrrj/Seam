package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestSeamLabCLIRejectsBadArguments runs the built seam-lab binary with
// missing or unknown subcommands and requires a non-zero exit with usage.
func TestSeamLabCLIRejectsBadArguments(t *testing.T) {
	bin := buildBinary(t)
	cases := []struct {
		name string
		args []string
	}{
		{"no arguments", nil},
		{"unknown subcommand", []string{"frobnicate"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, exit, _ := runBinary(t, bin, tc.args)
			if exit == 0 {
				t.Fatalf("expected non-zero exit; output:\n%s", out)
			}
			if !strings.Contains(out, "Commands:") {
				t.Fatalf("output does not include usage:\n%s", out)
			}
		})
	}
}

func buildBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "seam-lab")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/seam-lab")
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

func runBinary(t *testing.T, bin string, args []string) (string, int, error) {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Env = os.Environ()
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
