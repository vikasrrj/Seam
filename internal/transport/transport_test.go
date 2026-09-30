package transport

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func clearKafkaEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"KAFKA_TLS_ENABLED", "KAFKA_CA_CERT", "KAFKA_CLIENT_CERT", "KAFKA_CLIENT_KEY",
		"KAFKA_SASL_USERNAME", "KAFKA_SASL_PASSWORD", "KAFKA_SASL_MECHANISM",
	} {
		t.Setenv(name, "")
	}
}

func TestKafkaOptionsFailsClosed(t *testing.T) {
	tests := []struct {
		name  string
		env   map[string]string
		match string
	}{
		{name: "malformed TLS flag", env: map[string]string{"KAFKA_TLS_ENABLED": "sometimes"}, match: "invalid boolean"},
		{name: "unreadable CA", env: map[string]string{"KAFKA_TLS_ENABLED": "true", "KAFKA_CA_CERT": "/missing/ca.pem"}, match: "read ca cert"},
		{name: "partial client certificate", env: map[string]string{"KAFKA_CLIENT_CERT": "/tmp/client.pem"}, match: "configured together"},
		{name: "partial SASL", env: map[string]string{"KAFKA_SASL_USERNAME": "user"}, match: "configured together"},
		{name: "unknown SASL mechanism", env: map[string]string{"KAFKA_SASL_USERNAME": "user", "KAFKA_SASL_PASSWORD": "secret", "KAFKA_SASL_MECHANISM": "PLAIN"}, match: "unsupported"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clearKafkaEnvironment(t)
			for name, value := range test.env {
				t.Setenv(name, value)
			}
			if _, err := KafkaOptions(); err == nil || !strings.Contains(err.Error(), test.match) {
				t.Fatalf("error = %v, want substring %q", err, test.match)
			}
		})
	}
}

func TestKafkaOptionsAcceptsExplicitConfigurations(t *testing.T) {
	clearKafkaEnvironment(t)
	if options, err := KafkaOptions(); err != nil || len(options) != 0 {
		t.Fatalf("plaintext options = %d, error = %v", len(options), err)
	}

	t.Run("SASL", func(t *testing.T) {
		clearKafkaEnvironment(t)
		t.Setenv("KAFKA_SASL_USERNAME", "user")
		t.Setenv("KAFKA_SASL_PASSWORD", "secret")
		t.Setenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
		options, err := KafkaOptions()
		if err != nil || len(options) != 1 {
			t.Fatalf("SASL options = %d, error = %v", len(options), err)
		}
	})

	t.Run("TLS client pair is parsed", func(t *testing.T) {
		clearKafkaEnvironment(t)
		directory := t.TempDir()
		// Parsing an invalid pair proves the configured files are consumed. A
		// warning-and-continue implementation would incorrectly return success.
		certificate := filepath.Join(directory, "client.pem")
		key := filepath.Join(directory, "client-key.pem")
		if err := os.WriteFile(certificate, []byte("not a certificate"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(key, []byte("not a key"), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("KAFKA_CLIENT_CERT", certificate)
		t.Setenv("KAFKA_CLIENT_KEY", key)
		if _, err := KafkaOptions(); err == nil || !strings.Contains(err.Error(), "load client cert") {
			t.Fatalf("error = %v", err)
		}
	})
}
