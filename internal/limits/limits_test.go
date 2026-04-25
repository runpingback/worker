package limits

import "testing"

func TestCanExecute(t *testing.T) {
	tests := []struct {
		name     string
		plan     string
		current  int
		expected bool
	}{
		{"free under limit", "free", 999, true},
		{"free at limit", "free", 1000, false},
		{"free over limit", "free", 1001, false},
		{"pro under limit", "pro", 49999, true},
		{"pro at limit", "pro", 50000, false},
		{"team under limit", "team", 499999, true},
		{"team at limit", "team", 500000, false},
		{"unknown plan", "unknown", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CanExecute(tt.plan, tt.current)
			if got != tt.expected {
				t.Errorf("CanExecute(%q, %d) = %v, want %v", tt.plan, tt.current, got, tt.expected)
			}
		})
	}
}

func TestCapFanOut(t *testing.T) {
	tasks := make([]Task, 150)
	for i := range tasks {
		tasks[i] = Task{Name: "task", Payload: nil}
	}

	tests := []struct {
		name     string
		plan     string
		input    int
		expected int
	}{
		{"free gets 0", "free", 15, 0},
		{"pro capped at 10", "pro", 15, 10},
		{"pro under cap", "pro", 5, 5},
		{"team capped at 100", "team", 150, 100},
		{"team under cap", "team", 50, 50},
		{"unknown plan gets 0", "unknown", 5, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := tasks[:tt.input]
			got := CapFanOut(tt.plan, input)
			if len(got) != tt.expected {
				t.Errorf("CapFanOut(%q, %d tasks) = %d, want %d", tt.plan, tt.input, len(got), tt.expected)
			}
		})
	}
}

func TestBackoffSeconds(t *testing.T) {
	tests := []struct {
		attempt  int
		expected int
	}{
		{1, 2},
		{2, 4},
		{3, 8},
		{4, 16},
		{5, 32},
		{6, 60},
		{7, 60},
		{10, 60},
	}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			got := BackoffSeconds(tt.attempt)
			if got != tt.expected {
				t.Errorf("BackoffSeconds(%d) = %d, want %d", tt.attempt, got, tt.expected)
			}
		})
	}
}

func TestCapRetries(t *testing.T) {
	tests := []struct {
		plan      string
		requested int
		expected  int
	}{
		{"free", 5, 1},
		{"pro", 3, 3},
		{"pro", 10, 5},
		{"team", 10, 10},
		{"team", 15, 10},
	}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			got := CapRetries(tt.plan, tt.requested)
			if got != tt.expected {
				t.Errorf("CapRetries(%q, %d) = %d, want %d", tt.plan, tt.requested, got, tt.expected)
			}
		})
	}
}
