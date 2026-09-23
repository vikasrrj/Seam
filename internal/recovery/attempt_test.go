package recovery

import "testing"

func TestBumpAttempt(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"gen:0:attempt:0", "gen:0:attempt:1"},
		{"gen:0:attempt:9", "gen:0:attempt:10"},
		{"gen:3:attempt:7", "gen:3:attempt:8"},
	}
	for _, c := range cases {
		got, err := BumpAttempt(c.in)
		if err != nil {
			t.Fatalf("BumpAttempt(%q) error: %v", c.in, err)
		}
		if got != c.want {
			t.Fatalf("BumpAttempt(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestBumpGeneration(t *testing.T) {
	got, err := BumpGeneration("gen:2:attempt:5")
	if err != nil {
		t.Fatalf("BumpGeneration error: %v", err)
	}
	if want := "gen:3:attempt:0"; got != want {
		t.Fatalf("BumpGeneration = %q, want %q", got, want)
	}
}

func TestParseAttemptInvalid(t *testing.T) {
	invalid := []string{
		"",
		"gen:0",
		"gen:0:attempt",
		"gen:x:attempt:0",
		"gen:0:attempt:y",
		"foo:0:attempt:0",
		"gen:0:foo:0",
	}
	for _, s := range invalid {
		if _, _, err := parseAttempt(s); err == nil {
			t.Fatalf("parseAttempt(%q) expected error", s)
		}
	}
}
