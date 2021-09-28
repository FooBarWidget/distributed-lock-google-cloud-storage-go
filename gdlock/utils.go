package gdlock

import (
	"context"
	"sync"
	"time"
)

type retryOptions struct {
	RetryLogger       func(sleepDuration time.Duration)
	BackoffMin        time.Duration
	BackoffMax        time.Duration
	BackoffMultiplier float64
}

type workRegularlyOptions struct {
	Mutex                  *sync.Mutex
	RefreshInterval        time.Duration
	MaxFailures            uint
	Context                context.Context
	ScheduleCalculatedFunc func(timeout time.Duration)
}

type tryResult uint
type workRegularlyFunc func() (result bool, permanentFailure bool)

const (
	tryResultSuccess = iota
	tryResultRetryImmediately
	tryResultError
)

type tryFunc func() (tryResult, error)

func retryWithBackoffUntilSuccess(options retryOptions, f tryFunc) error {

}

func workRegularly(opts workRegularlyOptions, f workRegularlyFunc) (result bool, permanentFailure bool) {

}

func lockMutex(m *sync.Mutex) func() {
	m.Lock()
	alreadyUnlocked := false
	return func() {
		if !alreadyUnlocked {
			alreadyUnlocked = true
			m.Unlock()
		}
	}
}
