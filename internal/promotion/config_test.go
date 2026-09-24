package promotion

import (
	"context"
	"strings"
	"testing"
)

// TestPromoteRejectsMissingJobIDs verifies that promotion fails fast with a
// clear error when live or shadow job ids are missing or identical. The guard
// runs before any database or broker connection is attempted.
func TestPromoteRejectsMissingJobIDs(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
	}{
		{"both missing", Config{}},
		{"missing live job", Config{ShadowJobID: "shadow"}},
		{"missing shadow job", Config{LiveJobID: "live"}},
		{"same job id", Config{LiveJobID: "same", ShadowJobID: "same"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Promote(context.Background(), tc.cfg); err == nil {
				t.Fatal("expected error for missing or identical job ids")
			} else if !strings.Contains(err.Error(), "live and shadow job IDs are required") {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
