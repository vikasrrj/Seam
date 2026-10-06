//go:build integration

package integration

import "testing"

func TestBenchProfileClipsOperationAcrossReset(t *testing.T) {
	p := newBenchProfile()
	finish := p.start()
	p.reset()
	finish("in-flight")
	intervals := p.intervals["in-flight"]
	if len(intervals) != 1 || intervals[0].start.Before(p.measurementStart) {
		t.Fatalf("premeasurement time included: %+v", intervals)
	}
}
