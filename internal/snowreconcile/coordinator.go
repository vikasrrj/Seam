// Package snowreconcile coordinates SEAM's PostgreSQL snapshot algorithm with
// the Snowflake data plane. It deliberately owns orchestration only; durable
// truth and worker fencing live in snowflake.Store.
package snowreconcile

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"example.com/seam/internal/model"
	seamsnowflake "example.com/seam/internal/snowflake"
)

type Scanner interface {
	UpperBound(context.Context) (int64, error)
	NextChunkFrom(context.Context, *int64, int64, int) (model.ChunkRange, bool, error)
	ReadChunk(context.Context, int64, int64) ([]model.Row, error)
}

type MarkerWriter interface {
	WriteLow(context.Context, string, string, model.ChunkRange) (string, error)
	WriteHigh(context.Context, string, string, model.ChunkRange) (string, error)
}

type Warehouse interface {
	PrepareBackfill(context.Context, string, string, string) (*seamsnowflake.BackfillJob, error)
	SealBackfillManifest(context.Context, seamsnowflake.BackfillSpec) (*seamsnowflake.BackfillJob, error)
	LoadBackfill(context.Context, string) (*seamsnowflake.BackfillJob, error)
	LeaseChunk(context.Context, string, string, string, time.Duration) (*seamsnowflake.ChunkLease, error)
	RenewChunkLease(context.Context, *seamsnowflake.ChunkLease, time.Duration) error
	BeginChunkScan(context.Context, *seamsnowflake.ChunkLease, string) error
	StageSnapshot(context.Context, *seamsnowflake.ChunkLease, []model.Row) error
	SealChunkScan(context.Context, *seamsnowflake.ChunkLease, string) error
	WaitForMarker(context.Context, string, time.Duration) error
	FinalizeChunk(context.Context, *seamsnowflake.ChunkLease) error
}

type Config struct {
	JobID       string
	Attempt     string
	ShadowTable string
	ChunkSize   int
	Workers     int
	WorkerID    string
	Lease       time.Duration
	Heartbeat   time.Duration
	Poll        time.Duration
}

func (cfg Config) validate() error {
	if cfg.JobID == "" || cfg.Attempt == "" || cfg.ShadowTable == "" || cfg.WorkerID == "" {
		return fmt.Errorf("job, attempt, shadow table, and worker ID are required")
	}
	if cfg.ChunkSize <= 0 || cfg.Workers <= 0 || cfg.Lease <= 0 {
		return fmt.Errorf("chunk size, worker count, and lease duration must be positive")
	}
	if cfg.Heartbeat <= 0 {
		cfg.Heartbeat = cfg.Lease / 3
	}
	if cfg.Heartbeat <= 0 || cfg.Heartbeat >= cfg.Lease {
		return fmt.Errorf("heartbeat must be positive and shorter than the lease")
	}
	if cfg.Poll <= 0 {
		return fmt.Errorf("poll interval must be positive")
	}
	return nil
}

type Coordinator struct {
	cfg     Config
	store   Warehouse
	scanner Scanner
	markers MarkerWriter
}

func New(cfg Config, store Warehouse, scanner Scanner, markers MarkerWriter) (*Coordinator, error) {
	if cfg.Heartbeat <= 0 && cfg.Lease > 0 {
		cfg.Heartbeat = cfg.Lease / 3
	}
	if cfg.Poll <= 0 {
		cfg.Poll = 250 * time.Millisecond
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if store == nil || scanner == nil || markers == nil {
		return nil, fmt.Errorf("Snowflake coordinator dependencies are required")
	}
	return &Coordinator{cfg: cfg, store: store, scanner: scanner, markers: markers}, nil
}

// Run first activates dual writes, then samples and seals the source manifest,
// then executes leased LOW/scan/HIGH windows. It returns at ready_to_verify;
// validation and promotion are deliberately separate safety gates.
func (c *Coordinator) Run(ctx context.Context) error {
	job, err := c.store.PrepareBackfill(ctx, c.cfg.JobID, c.cfg.Attempt, c.cfg.ShadowTable)
	if err != nil {
		return fmt.Errorf("prepare Snowflake backfill: %w", err)
	}
	if job.State == seamsnowflake.BackfillDiscovering {
		upper, err := c.scanner.UpperBound(ctx)
		if err != nil {
			return fmt.Errorf("sample source upper bound after shadow activation: %w", err)
		}
		chunks, err := DiscoverManifest(ctx, c.scanner, upper, c.cfg.ChunkSize)
		if err != nil {
			return err
		}
		job, err = c.store.SealBackfillManifest(ctx, seamsnowflake.BackfillSpec{
			JobID: c.cfg.JobID, Attempt: c.cfg.Attempt, ShadowTable: c.cfg.ShadowTable,
			ScanUpperBound: upper, Chunks: chunks,
		})
		if err != nil {
			return fmt.Errorf("seal Snowflake backfill manifest: %w", err)
		}
	}
	switch job.State {
	case seamsnowflake.BackfillReadyToVerify, seamsnowflake.BackfillVerifying, seamsnowflake.BackfillReady, seamsnowflake.BackfillPromoting, seamsnowflake.BackfillCompleted:
		return nil
	case seamsnowflake.BackfillFailed:
		return fmt.Errorf("Snowflake backfill %q is failed: %s", c.cfg.JobID, job.ErrorMessage)
	case seamsnowflake.BackfillRunning:
	default:
		return fmt.Errorf("Snowflake backfill %q cannot run from state %s", c.cfg.JobID, job.State)
	}

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, c.cfg.Workers)
	var workers sync.WaitGroup
	for index := 0; index < c.cfg.Workers; index++ {
		workers.Add(1)
		workerID := fmt.Sprintf("%s-%d", c.cfg.WorkerID, index)
		go func() {
			defer workers.Done()
			if err := c.runWorker(workerCtx, workerID); err != nil && !errors.Is(err, context.Canceled) {
				select {
				case errCh <- err:
				default:
				}
				cancel()
			}
		}()
	}
	done := make(chan struct{})
	go func() { workers.Wait(); close(done) }()
	select {
	case <-ctx.Done():
		cancel()
		<-done
		return ctx.Err()
	case err := <-errCh:
		cancel()
		<-done
		return err
	case <-done:
		job, err := c.store.LoadBackfill(ctx, c.cfg.JobID)
		if err != nil {
			return err
		}
		if job.State != seamsnowflake.BackfillReadyToVerify && job.State != seamsnowflake.BackfillReady && job.State != seamsnowflake.BackfillCompleted {
			return fmt.Errorf("Snowflake workers stopped with job in state %s", job.State)
		}
		return nil
	}
}

func (c *Coordinator) runWorker(ctx context.Context, workerID string) error {
	for {
		lease, err := c.store.LeaseChunk(ctx, c.cfg.JobID, c.cfg.Attempt, workerID, c.cfg.Lease)
		if err != nil {
			return err
		}
		if lease == nil {
			job, err := c.store.LoadBackfill(ctx, c.cfg.JobID)
			if err != nil {
				return err
			}
			if job.State != seamsnowflake.BackfillRunning {
				return nil
			}
			if err := wait(ctx, c.cfg.Poll); err != nil {
				return err
			}
			continue
		}
		if err := c.executeLease(ctx, lease); err != nil {
			return fmt.Errorf("Snowflake chunk %s: %w", lease.Range, err)
		}
	}
}

func (c *Coordinator) executeLease(ctx context.Context, lease *seamsnowflake.ChunkLease) error {
	leaseCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	heartbeatErr := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(c.cfg.Heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-leaseCtx.Done():
				return
			case <-ticker.C:
				if err := c.store.RenewChunkLease(leaseCtx, lease, c.cfg.Lease); err != nil {
					select {
					case heartbeatErr <- err:
					default:
					}
					cancel()
					return
				}
			}
		}
	}()
	low, err := c.markers.WriteLow(leaseCtx, lease.JobID, lease.Attempt, lease.Range)
	if err == nil {
		err = c.store.BeginChunkScan(leaseCtx, lease, low)
	}
	var rows []model.Row
	if err == nil {
		rows, err = c.scanner.ReadChunk(leaseCtx, lease.Range.Min, lease.Range.Max)
	}
	if err == nil {
		err = c.store.StageSnapshot(leaseCtx, lease, rows)
	}
	var high string
	if err == nil {
		high, err = c.markers.WriteHigh(leaseCtx, lease.JobID, lease.Attempt, lease.Range)
	}
	if err == nil {
		err = c.store.SealChunkScan(leaseCtx, lease, high)
	}
	if err == nil {
		// HIGH is ordered after LOW and every source change concurrent with the
		// scan. Seeing HIGH in Snowflake therefore proves all collision clocks
		// needed by FinalizeChunk are present.
		err = c.store.WaitForMarker(leaseCtx, high, c.cfg.Poll)
	}
	if err == nil {
		err = c.store.FinalizeChunk(leaseCtx, lease)
	}
	cancel()
	if err != nil {
		select {
		case heartbeat := <-heartbeatErr:
			return fmt.Errorf("lease heartbeat failed: %w", heartbeat)
		default:
			return err
		}
	}
	return nil
}

// DiscoverManifest converts sparse key pages into a gap-free logical manifest
// through the captured upper bound. Deletions during discovery cannot leave a
// hole, and inserts above the bound are owned by the already-active CDC route.
func DiscoverManifest(ctx context.Context, scanner Scanner, upperBound int64, chunkSize int) ([]model.ChunkRange, error) {
	if scanner == nil || chunkSize <= 0 {
		return nil, fmt.Errorf("scanner and positive chunk size are required")
	}
	var manifest []model.ChunkRange
	var cursor *int64
	for {
		page, found, err := scanner.NextChunkFrom(ctx, cursor, upperBound, chunkSize)
		if err != nil {
			return nil, fmt.Errorf("discover Snowflake backfill chunk: %w", err)
		}
		minimum := int64(math.MinInt64)
		if cursor != nil {
			if *cursor == math.MaxInt64 {
				return nil, fmt.Errorf("discovery cursor reached maximum without sealing")
			}
			minimum = *cursor + 1
		}
		maximum := upperBound
		if found {
			if page.Min < minimum || page.Min > page.Max || page.Max > upperBound {
				return nil, fmt.Errorf("invalid source discovery page %s after %d through %d", page, minimum, upperBound)
			}
			maximum = page.Max
		}
		if maximum < minimum {
			return nil, fmt.Errorf("invalid logical discovery range [%d,%d]", minimum, maximum)
		}
		manifest = append(manifest, model.ChunkRange{Min: minimum, Max: maximum})
		if !found || maximum == upperBound {
			return manifest, nil
		}
		value := maximum
		cursor = &value
	}
}

func wait(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
