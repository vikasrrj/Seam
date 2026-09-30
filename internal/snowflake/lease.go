package snowflake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// SinkLease is a monotonic Snowflake writer term. OwnerID identifies one
// process incarnation; Epoch changes at every takeover and fences an expired
// process even if it continues running after losing connectivity.
type SinkLease struct {
	OwnerID string
	Epoch   int64
}

// LeaseHeldError reports the current owner without treating an expected
// active lease as a retryable Snowflake service failure.
type LeaseHeldError struct {
	OwnerID string
	Epoch   int64
	Expires time.Time
}

func (err *LeaseHeldError) Error() string {
	return fmt.Sprintf("Snowflake stream sink lease is held by %q at epoch %d until %s", err.OwnerID, err.Epoch, err.Expires.UTC().Format(time.RFC3339Nano))
}

func (s *Store) AcquireSinkLease(ctx context.Context, ownerID string, duration time.Duration) (*SinkLease, error) {
	ownerID = strings.TrimSpace(ownerID)
	if ownerID == "" {
		return nil, fmt.Errorf("Snowflake sink owner ID is required")
	}
	if duration <= 0 {
		return nil, fmt.Errorf("Snowflake sink lease duration must be positive")
	}
	result, execErr := s.db.ExecContext(ctx, "UPDATE "+s.cfg.internal("SINK_LEASES")+" SET OWNER_ID = ?, OWNER_EPOCH = OWNER_EPOCH + 1, LEASE_EXPIRES = DATEADD(millisecond, ?, CURRENT_TIMESTAMP()), UPDATED_AT = CURRENT_TIMESTAMP() WHERE STREAM_ID = ? AND LEASE_EXPIRES <= CURRENT_TIMESTAMP()", ownerID, duration.Milliseconds(), s.cfg.StreamID)
	acquired := false
	if execErr == nil {
		if affected, err := result.RowsAffected(); err != nil {
			return nil, fmt.Errorf("read Snowflake sink lease acquisition: %w", err)
		} else if affected > 1 {
			return nil, fmt.Errorf("Snowflake sink lease acquisition affected %d rows; duplicate stream metadata exists", affected)
		} else {
			acquired = affected == 1
		}
	}
	lease, expires, readErr := s.readSinkLease(ctx)
	if readErr != nil {
		if execErr != nil {
			return nil, errors.Join(fmt.Errorf("acquire Snowflake sink lease: %w", execErr), readErr)
		}
		return nil, readErr
	}
	if acquired && lease.OwnerID == ownerID && expires.After(time.Now()) {
		return lease, nil
	}
	// Reading back our unique process-incarnation ID resolves an ambiguous
	// UPDATE error. A clean zero-row result never reuses an existing owner ID.
	if execErr != nil && lease.OwnerID == ownerID && expires.After(time.Now()) {
		return lease, nil
	}
	if execErr != nil {
		return nil, fmt.Errorf("acquire Snowflake sink lease: %w", execErr)
	}
	return nil, &LeaseHeldError{OwnerID: lease.OwnerID, Epoch: lease.Epoch, Expires: expires}
}

func (s *Store) RenewSinkLease(ctx context.Context, lease *SinkLease, duration time.Duration) error {
	if err := validateSinkLease(lease, duration); err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, "UPDATE "+s.cfg.internal("SINK_LEASES")+" SET LEASE_EXPIRES = DATEADD(millisecond, ?, CURRENT_TIMESTAMP()), UPDATED_AT = CURRENT_TIMESTAMP() WHERE STREAM_ID = ? AND OWNER_ID = ? AND OWNER_EPOCH = ? AND LEASE_EXPIRES > CURRENT_TIMESTAMP()", duration.Milliseconds(), s.cfg.StreamID, lease.OwnerID, lease.Epoch)
	if err != nil {
		return fmt.Errorf("renew Snowflake sink lease: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("read Snowflake sink lease renewal: %w", err)
	} else if affected != 1 {
		return fmt.Errorf("Snowflake sink lease lost by owner %q epoch %d", lease.OwnerID, lease.Epoch)
	}
	return nil
}

func (s *Store) ReleaseSinkLease(ctx context.Context, lease *SinkLease) error {
	if lease == nil || strings.TrimSpace(lease.OwnerID) == "" || lease.Epoch <= 0 {
		return fmt.Errorf("invalid Snowflake sink lease")
	}
	result, err := s.db.ExecContext(ctx, "UPDATE "+s.cfg.internal("SINK_LEASES")+" SET LEASE_EXPIRES = CURRENT_TIMESTAMP(), UPDATED_AT = CURRENT_TIMESTAMP() WHERE STREAM_ID = ? AND OWNER_ID = ? AND OWNER_EPOCH = ?", s.cfg.StreamID, lease.OwnerID, lease.Epoch)
	if err != nil {
		return fmt.Errorf("release Snowflake sink lease: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("read Snowflake sink lease release: %w", err)
	} else if affected > 1 {
		return fmt.Errorf("Snowflake sink lease release affected %d rows", affected)
	}
	return nil
}

func (s *Store) fenceSinkLease(ctx context.Context, tx *sql.Tx, lease *SinkLease) error {
	if lease == nil || strings.TrimSpace(lease.OwnerID) == "" || lease.Epoch <= 0 {
		return fmt.Errorf("invalid Snowflake sink lease")
	}
	result, err := tx.ExecContext(ctx, "UPDATE "+s.cfg.internal("SINK_LEASES")+" SET UPDATED_AT = CURRENT_TIMESTAMP() WHERE STREAM_ID = ? AND OWNER_ID = ? AND OWNER_EPOCH = ? AND LEASE_EXPIRES > CURRENT_TIMESTAMP()", s.cfg.StreamID, lease.OwnerID, lease.Epoch)
	if err != nil {
		return fmt.Errorf("fence Snowflake sink transaction: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("read Snowflake sink fence: %w", err)
	} else if affected != 1 {
		return fmt.Errorf("Snowflake sink transaction fenced: owner %q epoch %d is stale or expired", lease.OwnerID, lease.Epoch)
	}
	return nil
}

func (s *Store) readSinkLease(ctx context.Context) (*SinkLease, time.Time, error) {
	var lease SinkLease
	var expires time.Time
	err := s.db.QueryRowContext(ctx, "SELECT OWNER_ID, OWNER_EPOCH, LEASE_EXPIRES FROM "+s.cfg.internal("SINK_LEASES")+" WHERE STREAM_ID = ?", s.cfg.StreamID).Scan(&lease.OwnerID, &lease.Epoch, &expires)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, time.Time{}, fmt.Errorf("Snowflake stream %q has no sink lease metadata", s.cfg.StreamID)
	}
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("read Snowflake sink lease: %w", err)
	}
	return &lease, expires, nil
}

func validateSinkLease(lease *SinkLease, duration time.Duration) error {
	if lease == nil || strings.TrimSpace(lease.OwnerID) == "" || lease.Epoch <= 0 {
		return fmt.Errorf("invalid Snowflake sink lease")
	}
	if duration <= 0 {
		return fmt.Errorf("Snowflake sink lease duration must be positive")
	}
	return nil
}
