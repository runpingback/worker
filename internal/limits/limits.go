package limits

import "encoding/json"

type PlanLimit struct {
	ExecutionsPerMonth int
	MaxRetries         int
	FanOutPerRun       int
}

type Task struct {
	Name    string          `json:"name"`
	Payload json.RawMessage `json:"payload"`
}

var Plans = map[string]PlanLimit{
	"free": {ExecutionsPerMonth: 1000, MaxRetries: 1, FanOutPerRun: 0},
	"pro":  {ExecutionsPerMonth: 50000, MaxRetries: 5, FanOutPerRun: 10},
	"team": {ExecutionsPerMonth: 500000, MaxRetries: 10, FanOutPerRun: 100},
}

func CanExecute(plan string, currentCount int) bool {
	p, ok := Plans[plan]
	if !ok {
		return false
	}
	return currentCount < p.ExecutionsPerMonth
}

func CapFanOut(plan string, tasks []Task) []Task {
	p, ok := Plans[plan]
	if !ok {
		return nil
	}
	if len(tasks) <= p.FanOutPerRun {
		return tasks
	}
	return tasks[:p.FanOutPerRun]
}

func CapRetries(plan string, requested int) int {
	p, ok := Plans[plan]
	if !ok {
		return 0
	}
	if requested > p.MaxRetries {
		return p.MaxRetries
	}
	return requested
}

func BackoffSeconds(attempt int) int {
	backoff := 1 << attempt // 2^attempt
	if backoff > 60 {
		return 60
	}
	return backoff
}
