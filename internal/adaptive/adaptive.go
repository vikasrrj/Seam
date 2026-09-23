// Package adaptive sizes backfill chunks from measured latency. It is a small,
// self-contained controller: no database state, safe for concurrent use.
package adaptive

import (
	"math"
	"sort"
	"sync"
	"time"
)

// warmup is the minimum number of observed chunks before the size starts to
// move. Early estimates from one or two chunks are too noisy.
const warmup = 3

// windowSize is the sliding window of recent chunk durations used to estimate
// the observed cost. Older samples are forgotten so the controller tracks the
// current regime instead of the historical average.
const windowSize = 32

// maxStepRatio bounds how much the chunk size may change per observation
// (half to double). Without this, a single outlier would cause oscillation.
const maxStepRatio = 2.0

// Sizer adjusts the chunk row count toward a target per-chunk duration.
type Sizer struct {
	mu sync.Mutex

	target  time.Duration
	current int
	min     int
	max     int

	durations []time.Duration
}

// NewSizer returns a Sizer that starts every chunk at initialSize rows and
// adapts it so a chunk costs about target. min and max clamp the size; all
// bounds must be >= 1 and min <= max.
func NewSizer(target time.Duration, initialSize, min, max int) *Sizer {
	if target <= 0 {
		target = time.Second
	}
	if min < 1 {
		min = 1
	}
	if max < min {
		max = min
	}
	if initialSize < min {
		initialSize = min
	}
	if initialSize > max {
		initialSize = max
	}
	return &Sizer{
		target:  target,
		current: initialSize,
		min:     min,
		max:     max,
	}
}

// Suggest returns the row count to use for the next chunk. It is a pure read
// and never mutates state.
func (s *Sizer) Suggest() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current
}

// Observe records the wall-clock duration of one completed chunk. Once enough
// chunks have been observed, the size is pushed toward target/observed.
func (s *Sizer) Observe(duration time.Duration, rows int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if duration > 0 {
		s.durations = append(s.durations, duration)
		if len(s.durations) > windowSize {
			// Slide the window: forget the oldest observation.
			s.durations = append(s.durations[:0], s.durations[1:]...)
		}
	}
	// rows is informational for now; duration is the feedback signal.
	_ = rows

	if len(s.durations) < warmup {
		return
	}

	median := s.median()
	if median <= 0 {
		return
	}

	ratio := float64(s.target) / float64(median)
	// Bound the per-step change to avoid oscillation.
	if ratio > maxStepRatio {
		ratio = maxStepRatio
	}
	if ratio < 1/maxStepRatio {
		ratio = 1 / maxStepRatio
	}

	next := float64(s.current) * ratio
	if next > math.MaxInt32 {
		next = math.MaxInt32
	}
	s.current = clamp(int(next), s.min, s.max)
}

// median returns the median observed duration. It must be called with s.mu
// held.
func (s *Sizer) median() time.Duration {
	n := len(s.durations)
	if n == 0 {
		return 0
	}
	// Copy + sort: the window is tiny (bounded only by total chunks, but in
	// practice stays small); a running quantile sketch is overkill here.
	sorted := make([]time.Duration, n)
	copy(sorted, s.durations)
	sortDurations(sorted)
	mid := n / 2
	if n%2 == 1 {
		return sorted[mid]
	}
	return (sorted[mid-1] + sorted[mid]) / 2
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func sortDurations(d []time.Duration) { sort.Slice(d, func(i, j int) bool { return d[i] < d[j] }) }
