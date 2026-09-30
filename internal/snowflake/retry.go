package snowflake

import (
	"context"
	"errors"
	"io"
	"net"

	gosnowflake "github.com/snowflakedb/gosnowflake/v2"
)

// IsRetryable reports only failures for which replaying the same idempotent
// Snowflake transaction is safe and useful. SQL/schema/data errors fail
// immediately; network, service, session, serialization, and warehouse
// availability failures are retried and resolved against the durable ledger.
func IsRetryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var network net.Error
	if errors.As(err, &network) {
		return true
	}
	var snowflakeError *gosnowflake.SnowflakeError
	if !errors.As(err, &snowflakeError) {
		return false
	}
	if len(snowflakeError.SQLState) >= 2 {
		switch snowflakeError.SQLState[:2] {
		case "08", "40", "53", "57", "58":
			return true
		}
	}
	switch snowflakeError.Number {
	case gosnowflake.ErrCodeServiceUnavailable,
		gosnowflake.ErrFailedToPostQuery,
		gosnowflake.ErrFailedToRenewSession:
		return true
	default:
		return false
	}
}
