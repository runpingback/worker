package db

import (
	"testing"
	"time"
)

func TestNeedsQuotaReset(t *testing.T) {
	t.Run("nil reset time", func(t *testing.T) {
		if !NeedsQuotaReset(nil) {
			t.Error("expected true for nil reset time")
		}
	})

	t.Run("past reset time", func(t *testing.T) {
		past := time.Now().Add(-time.Hour)
		if !NeedsQuotaReset(&past) {
			t.Error("expected true for past reset time")
		}
	})

	t.Run("future reset time", func(t *testing.T) {
		future := time.Now().Add(time.Hour)
		if NeedsQuotaReset(&future) {
			t.Error("expected false for future reset time")
		}
	})
}

func TestNextMonthReset(t *testing.T) {
	got := nextMonthReset()
	now := time.Now().UTC()

	if got.Day() != 1 {
		t.Errorf("expected day 1, got %d", got.Day())
	}
	if got.Before(now) {
		t.Errorf("expected future date, got %v", got)
	}
}
