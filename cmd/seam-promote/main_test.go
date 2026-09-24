package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestSeamPromoteCLIRequiresJobIDs runs the built promote binary with missing
// or invalid job ids and requires a non-zero exit with a clear diagnostic.
func TestSeamPromoteCLIRequiresJobIDs(t *testing.T) {
	bin := buildBinary(t)
	cases := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{"no arguments", nil, "--live-job and --shadow-job are required"},
		{"missing shadow job", []string{"--live-job", "live"}, "--live-job and --shadow-job are required"},
		{"missing live job", []string{"--shadow-job", "shadow"}, "--live-job and --shadow-job are required"},
		{"same job for both", []string{"--live-job", "same", "--shadow-job", "same"}, "must be distinct"},
		{"unknown flag", []string{"--bogus"}, "flag provided but not defined"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, exit, _ := runBinary(t, bin, tc.args)
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
	bin := filepath.Join(t.TempDir(), "seam-promote")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/seam-promote")
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
