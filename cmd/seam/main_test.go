package main

import (
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestConfigFromEnvDefaults(t *testing.T) {
	cfg, err := configFromEnv()
	if err != nil {
		t.Fatalf("configFromEnv: %v", err)
	}
	checks := []struct {
		name     string
		got, exp interface{}
	}{
		{"JobID", cfg.JobID, "seam-default"},
		{"ChunkSize", cfg.ChunkSize, 1000},
		{"Workers", cfg.Workers, 1},
		{"MaxInMemoryCandidates", cfg.MaxInMemoryCandidates, 1_000_000},
		{"MaxCandidateBytes", cfg.MaxCandidateBytes, int64(256 << 20)},
		{"MaxRecordsPerBatch", cfg.MaxRecordsPerBatch, 100},
		{"MaxSourceScans", cfg.MaxSourceScans, 4},
		{"MaxDestinationTx", cfg.MaxDestinationTx, 8},
		{"MaxCDCLagRecords", cfg.MaxCDCLagRecords, int64(10_000)},
		{"ResourcePollInterval", cfg.ResourcePollInterval, time.Second},
		{"LeaseDuration", cfg.LeaseDuration, 30 * time.Second},
		{"HeartbeatInterval", cfg.HeartbeatInterval, time.Duration(0)},
		{"AdaptiveChunking", cfg.AdaptiveChunking, false},
		{"ChunkSizeMin", cfg.ChunkSizeMin, 100},
		{"ChunkSizeMax", cfg.ChunkSizeMax, 1_000_000},
	}
	for _, c := range checks {
		if c.got != c.exp {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.exp)
		}
	}
}

func TestConfigFromEnvValidValues(t *testing.T) {
	env := map[string]string{
		"SEAM_CHUNK_SIZE":               "250",
		"SEAM_WORKERS":                  "4",
		"SEAM_MAX_IN_MEMORY_CANDIDATES": "500",
		"SEAM_MAX_CANDIDATE_BYTES":      "268435456",
		"SEAM_MAX_RECORDS_PER_BATCH":    "50",
		"SEAM_LEASE_DURATION":           "90s",
		"SEAM_HEARTBEAT_INTERVAL":       "15s",
		"SEAM_TARGET_CHUNK_DURATION":    "3s",
		"SEAM_CHUNK_SIZE_MIN":           "50",
		"SEAM_CHUNK_SIZE_MAX":           "20000",
		"SEAM_ADAPTIVE_CHUNKING":        "off",
		"SEAM_MAX_SOURCE_SCANS":         "2",
		"SEAM_MAX_DESTINATION_TX":       "3",
		"SEAM_MAX_CDC_LAG_RECORDS":      "750",
		"SEAM_RESOURCE_POLL_INTERVAL":   "250ms",
	}
	for k, v := range env {
		t.Setenv(k, v)
	}
	cfg, err := configFromEnv()
	if err != nil {
		t.Fatalf("configFromEnv: %v", err)
	}
	if cfg.ChunkSize != 250 || cfg.Workers != 4 || cfg.MaxInMemoryCandidates != 500 {
		t.Errorf("integer parsing: chunk=%d workers=%d candidates=%d", cfg.ChunkSize, cfg.Workers, cfg.MaxInMemoryCandidates)
	}
	if cfg.MaxCandidateBytes != 268435456 || cfg.MaxRecordsPerBatch != 50 {
		t.Errorf("int64 parsing: bytes=%d batch=%d", cfg.MaxCandidateBytes, cfg.MaxRecordsPerBatch)
	}
	if cfg.LeaseDuration != 90*time.Second || cfg.HeartbeatInterval != 15*time.Second || cfg.TargetChunkDuration != 3*time.Second {
		t.Errorf("duration parsing: lease=%s heartbeat=%s target=%s", cfg.LeaseDuration, cfg.HeartbeatInterval, cfg.TargetChunkDuration)
	}
	if cfg.ChunkSizeMin != 50 || cfg.ChunkSizeMax != 20000 {
		t.Errorf("chunk bounds: min=%d max=%d", cfg.ChunkSizeMin, cfg.ChunkSizeMax)
	}
	if cfg.AdaptiveChunking {
		t.Error("SEAM_ADAPTIVE_CHUNKING=off should parse to false")
	}
	if cfg.MaxSourceScans != 2 || cfg.MaxDestinationTx != 3 || cfg.MaxCDCLagRecords != 750 || cfg.ResourcePollInterval != 250*time.Millisecond {
		t.Errorf("resource config: scans=%d dest=%d lag=%d poll=%s", cfg.MaxSourceScans, cfg.MaxDestinationTx, cfg.MaxCDCLagRecords, cfg.ResourcePollInterval)
	}
}

func TestConfigFromEnvRejectsMalformed(t *testing.T) {
	cases := []struct {
		name string
		env  string
		val  string
	}{
		{"chunk size", "SEAM_CHUNK_SIZE", "abc"},
		{"chunk size trailing garbage", "SEAM_CHUNK_SIZE", "100x"},
		{"workers", "SEAM_WORKERS", "1,2"},
		{"candidate limit", "SEAM_MAX_IN_MEMORY_CANDIDATES", "lots"},
		{"candidate bytes", "SEAM_MAX_CANDIDATE_BYTES", "1.5G"},
		{"records per batch", "SEAM_MAX_RECORDS_PER_BATCH", "-"},
		{"chunk size min", "SEAM_CHUNK_SIZE_MIN", "1_000"},
		{"chunk size max", "SEAM_CHUNK_SIZE_MAX", "100000000000000000000"},
		{"lease duration", "SEAM_LEASE_DURATION", "not-a-duration"},
		{"heartbeat interval", "SEAM_HEARTBEAT_INTERVAL", "10"},
		{"target chunk duration", "SEAM_TARGET_CHUNK_DURATION", "2 parsecs"},
		{"adaptive flag", "SEAM_ADAPTIVE_CHUNKING", "banana"},
		{"empty broker list", "KAFKA_BROKERS", ",,"},
		{"blank-only broker list", "KAFKA_BROKERS", ", ,,"},
		{"source scan limit", "SEAM_MAX_SOURCE_SCANS", "many"},
		{"destination tx limit", "SEAM_MAX_DESTINATION_TX", "lots"},
		{"cdc lag limit", "SEAM_MAX_CDC_LAG_RECORDS", "far"},
		{"resource poll", "SEAM_RESOURCE_POLL_INTERVAL", "soon"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(tc.env, tc.val)
			_, err := configFromEnv()
			if err == nil {
				t.Fatalf("expected error for %s=%q", tc.env, tc.val)
			}
			if !strings.Contains(err.Error(), tc.env) {
				t.Fatalf("error %q does not name the offending variable %q", err, tc.env)
			}
		})
	}
}

func TestConfigValidate(t *testing.T) {
	base := func() config {
		cfg, err := configFromEnv()
		if err != nil {
			t.Fatal(err)
		}
		return cfg
	}
	cases := []struct {
		name    string
		mutate  func(*config)
		wantErr string
	}{
		{"valid defaults", func(c *config) {}, ""},
		{"zero workers", func(c *config) { c.Workers = 0 }, "worker count"},
		{"negative workers", func(c *config) { c.Workers = -4 }, "worker count"},
		{"zero chunk size", func(c *config) { c.ChunkSize = 0 }, "chunk size, worker count, and candidate limit"},
		{"chunk larger than candidate limit", func(c *config) { c.ChunkSize = 5000; c.MaxInMemoryCandidates = 100 }, "chunk size, worker count, and candidate limit"},
		{"zero candidate limit", func(c *config) { c.MaxInMemoryCandidates = 0 }, "chunk size, worker count, and candidate limit"},
		{"byte budget below 1MiB per worker", func(c *config) { c.MaxCandidateBytes = 1 << 19 }, "candidate byte budget"},
		{"byte budget ok for two workers", func(c *config) { c.Workers = 2; c.MaxCandidateBytes = 2 << 20 }, ""},
		{"zero lease duration", func(c *config) { c.LeaseDuration = 0 }, "SEAM_LEASE_DURATION"},
		{"negative lease duration", func(c *config) { c.LeaseDuration = -30 * time.Second }, "SEAM_LEASE_DURATION"},
		{"zero records per batch", func(c *config) { c.MaxRecordsPerBatch = 0 }, "SEAM_MAX_RECORDS_PER_BATCH"},
		{"negative heartbeat", func(c *config) { c.HeartbeatInterval = -1 * time.Second }, "SEAM_HEARTBEAT_INTERVAL"},
		{"heartbeat at lease duration", func(c *config) { c.HeartbeatInterval = c.LeaseDuration }, "strictly shorter"},
		{"heartbeat beyond lease duration", func(c *config) { c.HeartbeatInterval = 2 * c.LeaseDuration }, "strictly shorter"},
		{"zero heartbeat derives from lease", func(c *config) { c.HeartbeatInterval = 0 }, ""},
		{"zero target chunk duration", func(c *config) { c.TargetChunkDuration = 0 }, "SEAM_TARGET_CHUNK_DURATION"},
		{"negative target chunk duration", func(c *config) { c.TargetChunkDuration = -1 * time.Second }, "SEAM_TARGET_CHUNK_DURATION"},
		{"zero chunk size min", func(c *config) { c.ChunkSizeMin = 0 }, "SEAM_CHUNK_SIZE_MIN"},
		{"negative chunk size min", func(c *config) { c.ChunkSizeMin = -10 }, "SEAM_CHUNK_SIZE_MIN"},
		{"chunk max below min", func(c *config) { c.ChunkSizeMin = 5000; c.ChunkSizeMax = 100 }, "SEAM_CHUNK_SIZE_MAX"},
		{"worker count overflows byte budget", func(c *config) { c.Workers = math.MaxInt64 / 4 }, "candidate byte budget"},
		{"worker count at overflow boundary ok", func(c *config) { c.Workers = math.MaxInt32; c.MaxCandidateBytes = 1 << 60 }, ""},
		{"unsupported destination table", func(c *config) { c.DestTable = "orders" }, "unsupported destination table"},
		{"shadow destination table", func(c *config) { c.DestTable = "accounts_shadow" }, ""},
		{"adaptive chunking rejected", func(c *config) { c.AdaptiveChunking = true }, "adaptive chunking"},
		{"non-accounts source table", func(c *config) { c.SourceTable = "orders" }, "only public.accounts"},
		{"non-id source key", func(c *config) { c.SourceKey = "uuid" }, "only public.accounts"},
		{"zero source scans", func(c *config) { c.MaxSourceScans = 0 }, "SEAM_MAX_SOURCE_SCANS"},
		{"zero destination tx", func(c *config) { c.MaxDestinationTx = 0 }, "SEAM_MAX_DESTINATION_TX"},
		{"negative cdc lag", func(c *config) { c.MaxCDCLagRecords = -1 }, "SEAM_MAX_CDC_LAG_RECORDS"},
		{"zero resource poll", func(c *config) { c.ResourcePollInterval = 0 }, "SEAM_RESOURCE_POLL_INTERVAL"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base()
			tc.mutate(&cfg)
			err := cfg.validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("validate: unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validate: expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("validate: error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
}

// TestSeamCLIInvalidArguments runs the real seam binary with invalid or
// missing arguments and requires a non-zero exit with a clear diagnostic.
func TestSeamCLIInvalidArguments(t *testing.T) {
	bin := buildCmdBinary(t, "./cmd/seam")
	cases := []struct {
		name    string
		env     map[string]string
		args    []string
		wantErr string
	}{
		{"malformed chunk size", map[string]string{"SEAM_CHUNK_SIZE": "abc"}, nil, "SEAM_CHUNK_SIZE"},
		{"negative workers", map[string]string{"SEAM_WORKERS": "-1"}, nil, "worker count"},
		{"malformed lease duration", map[string]string{"SEAM_LEASE_DURATION": "not-a-duration"}, nil, "SEAM_LEASE_DURATION"},
		{"candidate budget too small", map[string]string{"SEAM_MAX_CANDIDATE_BYTES": "1024"}, nil, "candidate byte budget"},
		{"unsupported destination table", map[string]string{"SEAM_DEST_TABLE": "orders"}, nil, "unsupported destination table"},
		{"empty broker list", map[string]string{"KAFKA_BROKERS": ",,"}, nil, "KAFKA_BROKERS"},
		{"zero lease duration", map[string]string{"SEAM_LEASE_DURATION": "0s"}, nil, "SEAM_LEASE_DURATION"},
		{"zero records per batch", map[string]string{"SEAM_MAX_RECORDS_PER_BATCH": "0"}, nil, "SEAM_MAX_RECORDS_PER_BATCH"},
		{"negative heartbeat", map[string]string{"SEAM_HEARTBEAT_INTERVAL": "-5s"}, nil, "SEAM_HEARTBEAT_INTERVAL"},
		{"heartbeat beyond lease", map[string]string{"SEAM_LEASE_DURATION": "30s", "SEAM_HEARTBEAT_INTERVAL": "30s"}, nil, "strictly shorter"},
		{"zero target chunk duration", map[string]string{"SEAM_TARGET_CHUNK_DURATION": "0s"}, nil, "SEAM_TARGET_CHUNK_DURATION"},
		{"zero chunk size min", map[string]string{"SEAM_CHUNK_SIZE_MIN": "0"}, nil, "SEAM_CHUNK_SIZE_MIN"},
		{"chunk max below min", map[string]string{"SEAM_CHUNK_SIZE_MIN": "5000", "SEAM_CHUNK_SIZE_MAX": "100"}, nil, "SEAM_CHUNK_SIZE_MAX"},
		{"worker count overflow", map[string]string{"SEAM_WORKERS": "9223372036854775806"}, nil, "candidate byte budget"},
		{"unknown flag", nil, []string{"--bogus"}, "flag provided but not defined"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, exit, _ := runBinary(t, bin, tc.env, tc.args)
			if exit == 0 {
				t.Fatalf("expected non-zero exit; output:\n%s", out)
			}
			if !strings.Contains(out, tc.wantErr) {
				t.Fatalf("output does not contain %q:\n%s", tc.wantErr, out)
			}
		})
	}
}

// buildCmdBinary compiles one command into a temp directory.
func buildCmdBinary(t *testing.T, pkg string) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "seam-bin")
	cmd := exec.Command("go", "build", "-o", bin, pkg)
	cmd.Dir = repoRoot(t)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go build %s: %v\n%s", pkg, err, out)
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

// runBinary executes a built binary with an optional env overlay and returns
// its combined output and exit code.
func runBinary(t *testing.T, bin string, env map[string]string, args []string) (string, int, error) {
	t.Helper()
	cmd := exec.Command(bin, args...)
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
