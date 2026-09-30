package snowflake

import (
	"context"
	"errors"
	"net"
	"testing"

	gosnowflake "github.com/snowflakedb/gosnowflake/v2"
)

func TestIsRetryable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", want: false},
		{name: "canceled", err: context.Canceled, want: false},
		{name: "deadline", err: context.DeadlineExceeded, want: true},
		{name: "network", err: &net.DNSError{IsTimeout: true}, want: true},
		{name: "connection SQL state", err: &gosnowflake.SnowflakeError{SQLState: "08006"}, want: true},
		{name: "serialization SQL state", err: &gosnowflake.SnowflakeError{SQLState: "40001"}, want: true},
		{name: "service unavailable", err: &gosnowflake.SnowflakeError{Number: gosnowflake.ErrCodeServiceUnavailable}, want: true},
		{name: "syntax error", err: &gosnowflake.SnowflakeError{SQLState: "42000"}, want: false},
		{name: "wrapped transient", err: errors.Join(errors.New("apply"), &gosnowflake.SnowflakeError{SQLState: "08001"}), want: true},
		{name: "ordinary error", err: errors.New("bad row"), want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := IsRetryable(test.err); got != test.want {
				t.Fatalf("IsRetryable(%v) = %v, want %v", test.err, got, test.want)
			}
		})
	}
}
