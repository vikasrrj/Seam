package resource

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgreSQL two-key advisory locks use a private SEAM namespace. The second
// key divides source-scan and destination-transaction slots.
const (
	seamPermitNamespace int32 = 0x5345414d // "SEAM"
	SourceScanClass     int32 = 1_000_000
	DestinationTxClass  int32 = 2_000_000
)

// PostgresAdvisoryPool is a crash-releasing, process-shared semaphore. Each
// permit is a session advisory lock held by a pooled PostgreSQL connection.
// Normal release unlocks the session for reuse; connection or process death
// also releases the slot. Every participant must use the same slot count.
type PostgresAdvisoryPool struct {
	class        int32
	slots        int
	pollInterval time.Duration
	next         atomic.Uint64
	pool         *pgxpool.Pool
}

func NewPostgresAdvisoryPool(dsn string, class int32, slots int, pollInterval time.Duration) (*PostgresAdvisoryPool, error) {
	if dsn == "" || slots < 1 || pollInterval <= 0 {
		return nil, fmt.Errorf("PostgreSQL advisory permit pool requires a DSN, positive slot count, and positive poll interval")
	}
	if class < 0 || int64(class)+int64(slots) > int64(^uint32(0)>>1) {
		return nil, fmt.Errorf("PostgreSQL advisory permit class %d with %d slots exceeds int32 key space", class, slots)
	}
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse PostgreSQL advisory permit DSN: %w", err)
	}
	poolConfig.MaxConns = int32(slots)
	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL advisory permit pool: %w", err)
	}
	return &PostgresAdvisoryPool{class: class, slots: slots, pollInterval: pollInterval, pool: pool}, nil
}

func (p *PostgresAdvisoryPool) Acquire(ctx context.Context) (func(), error) {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("connect for shared resource permit: %w", err)
	}
	owned := false
	defer func() {
		if !owned {
			conn.Release()
		}
	}()

	start := int(p.next.Add(1)-1) % p.slots
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()
	for {
		for i := 0; i < p.slots; i++ {
			slot := (start + i) % p.slots
			var acquired bool
			if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1::integer, $2::integer)`, seamPermitNamespace, p.class+int32(slot)).Scan(&acquired); err != nil {
				return nil, fmt.Errorf("acquire shared resource permit: %w", err)
			}
			if acquired {
				owned = true
				var once sync.Once
				return func() {
					once.Do(func() {
						releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						var unlocked bool
						err := conn.QueryRow(releaseCtx, `SELECT pg_advisory_unlock($1::integer, $2::integer)`, seamPermitNamespace, p.class+int32(slot)).Scan(&unlocked)
						if err == nil && unlocked {
							conn.Release()
							return
						}
						// Never return a session with uncertain lock state to the
						// pool. Closing it makes PostgreSQL release every session lock.
						raw := conn.Hijack()
						_ = raw.Close(context.Background())
					})
				}, nil
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			start = (start + 1) % p.slots
		}
	}
}

// Close releases idle permit sessions and waits for held sessions to return.
func (p *PostgresAdvisoryPool) Close() {
	if p != nil && p.pool != nil {
		p.pool.Close()
	}
}
