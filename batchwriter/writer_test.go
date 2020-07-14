package batchwriter

import (
	"sync"
	"testing"
	"time"
)

func TestBackoffValues(t *testing.T) {
	expected := []time.Duration{
		0 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
	}
	tolerance := time.Millisecond * 50
	var wg sync.WaitGroup
	for i, e := range expected {
		wg.Add(1)
		go func(retries int, expected time.Duration) {
			defer wg.Done()
			b := NewBackoff(retries)
			start := time.Now()
			if err := b(retries); err != nil {
				t.Errorf("for %d retries, got unepected error: %v", retries, err)
				return
			}
			actual := time.Now().Sub(start)
			if !within(actual, expected, tolerance) {
				t.Errorf("for %d retries, expected %v, got %v", retries, expected, actual)
			}
		}(i, e)
	}
	wg.Wait()
}

func TestBackoffExceeded(t *testing.T) {
	b := NewBackoff(100)
	if err := b(101); err != ErrMaxBackoffReached {
		t.Errorf("expected error, got %v", err)
	}
}

func within(actual, expected, tolerance time.Duration) bool {
	min := expected - tolerance
	max := expected + tolerance
	return actual >= min && actual <= max
}
