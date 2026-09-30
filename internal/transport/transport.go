// Package transport builds network configurations from environment variables.
package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strconv"
	"strings"

	"example.com/seam/internal/retry"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func ConnectPostgres(ctx context.Context, dsn string) (*pgx.Conn, error) {
	return retry.Do(ctx, retry.DefaultConfig(), func() (*pgx.Conn, error) {
		return connectPostgresOnce(ctx, dsn)
	}, retry.IsRetryablePG)
}

func connectPostgresOnce(ctx context.Context, dsn string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	tlsConfig, sslMode, err := postgresTLSFromEnv(cfg.Host)
	if err != nil {
		return nil, fmt.Errorf("postgres tls config: %w", err)
	}
	switch sslMode {
	case "disable":
		cfg.TLSConfig = nil
	case "require":
		if tlsConfig == nil {
			tlsConfig = &tls.Config{InsecureSkipVerify: true}
		} else {
			tlsConfig.InsecureSkipVerify = true
		}
		cfg.TLSConfig = tlsConfig
	case "verify-ca", "verify-full":
		if tlsConfig == nil {
			tlsConfig = &tls.Config{}
		}
		if sslMode == "verify-full" {
			tlsConfig.ServerName = cfg.Host
		}
		cfg.TLSConfig = tlsConfig
	default:
		if tlsConfig != nil {
			tlsConfig.ServerName = cfg.Host
			cfg.TLSConfig = tlsConfig
		}
	}
	return pgx.ConnectConfig(ctx, cfg)
}

func postgresTLSFromEnv(host string) (*tls.Config, string, error) {
	sslMode := os.Getenv("PGSSLMODE")
	rootCert := os.Getenv("PGSSLROOTCERT")
	clientCert := os.Getenv("PGSSLCERT")
	clientKey := os.Getenv("PGSSLKEY")
	if rootCert == "" && clientCert == "" && clientKey == "" {
		return nil, sslMode, nil
	}
	tlsConfig := &tls.Config{ServerName: host}
	if rootCert != "" {
		pool := x509.NewCertPool()
		pem, err := os.ReadFile(rootCert)
		if err != nil {
			return nil, "", fmt.Errorf("read root cert: %w", err)
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, "", fmt.Errorf("invalid root cert")
		}
		tlsConfig.RootCAs = pool
	}
	if clientCert != "" && clientKey != "" {
		cert, err := tls.LoadX509KeyPair(clientCert, clientKey)
		if err != nil {
			return nil, "", fmt.Errorf("load client cert: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	return tlsConfig, sslMode, nil
}

// KafkaOptions builds transport options and fails closed. A requested but
// unreadable TLS certificate or a partial/unknown SASL configuration must not
// silently downgrade a production replication stream to plaintext or unauthenticated access.
func KafkaOptions() ([]kgo.Opt, error) {
	var opts []kgo.Opt
	tlsConfig, err := kafkaTLSFromEnv()
	if err != nil {
		return nil, fmt.Errorf("kafka TLS configuration: %w", err)
	}
	if tlsConfig != nil {
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}
	mechanism, err := kafkaSASLFromEnv()
	if err != nil {
		return nil, fmt.Errorf("kafka SASL configuration: %w", err)
	}
	if mechanism != nil {
		opts = append(opts, kgo.SASL(mechanism))
	}
	return opts, nil
}

func kafkaTLSFromEnv() (*tls.Config, error) {
	enabled := false
	if raw := strings.TrimSpace(os.Getenv("KAFKA_TLS_ENABLED")); raw != "" {
		parsed, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, fmt.Errorf("KAFKA_TLS_ENABLED: invalid boolean %q", raw)
		}
		enabled = parsed
	}
	caCert := os.Getenv("KAFKA_CA_CERT")
	clientCert := os.Getenv("KAFKA_CLIENT_CERT")
	clientKey := os.Getenv("KAFKA_CLIENT_KEY")
	if !enabled && caCert == "" && clientCert == "" && clientKey == "" {
		return nil, nil
	}
	tlsConfig := &tls.Config{}
	if caCert != "" {
		pool := x509.NewCertPool()
		pem, err := os.ReadFile(caCert)
		if err != nil {
			return nil, fmt.Errorf("read ca cert: %w", err)
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("invalid ca cert")
		}
		tlsConfig.RootCAs = pool
	}
	if (clientCert == "") != (clientKey == "") {
		return nil, fmt.Errorf("KAFKA_CLIENT_CERT and KAFKA_CLIENT_KEY must be configured together")
	}
	if clientCert != "" {
		cert, err := tls.LoadX509KeyPair(clientCert, clientKey)
		if err != nil {
			return nil, fmt.Errorf("load client cert: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	return tlsConfig, nil
}

func kafkaSASLFromEnv() (sasl.Mechanism, error) {
	user := strings.TrimSpace(os.Getenv("KAFKA_SASL_USERNAME"))
	pass := os.Getenv("KAFKA_SASL_PASSWORD")
	mechanism := strings.TrimSpace(os.Getenv("KAFKA_SASL_MECHANISM"))
	if user == "" && pass == "" && mechanism == "" {
		return nil, nil
	}
	if user == "" || pass == "" {
		return nil, fmt.Errorf("KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD must be configured together")
	}
	auth := scram.Auth{User: user, Pass: pass}
	switch mechanism {
	case "", "SCRAM-SHA-256":
		return auth.AsSha256Mechanism(), nil
	case "SCRAM-SHA-512":
		return auth.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("unsupported KAFKA_SASL_MECHANISM %q", mechanism)
	}
}
