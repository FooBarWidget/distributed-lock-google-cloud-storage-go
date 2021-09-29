package gdlock

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type retryOptions struct {
	Context           context.Context
	RetryLogger       func(sleepDuration time.Duration)
	BackoffMin        time.Duration
	BackoffMax        time.Duration
	BackoffMultiplier float64
	FakeSleep         bool
}

type workRegularlyOptions struct {
	Context                context.Context
	Interval               time.Duration
	MaxFailures            uint
	ScheduleCalculatedFunc func(timeout time.Duration)
}

type tryResult uint
type workRegularlyFunc func() (result bool, permanentFailure bool)

const (
	tryResultNone = iota
	tryResultSuccess
	tryResultRetryImmediately
	tryResultRetryLater
)

type tryFunc func() (tryResult, error)

// retryWithBackoffUntilSuccess runs `f` until it returns `tryResultSuccess`,
// until it returns an error, or until the Context is done.
//
// Every time `f` returns `tryResultRetryLater`, we sleep for a duration and
// then we try again. This sleep duration increases exponentially but is subject
// to random jitter.
//
// If `f` returns `tryResultRetryImmediately` then we retry immediately without
// sleeping.
//
// This function returns either nil (if `f` eventually succeeded), or it returns
// the error returned by `f`.
func retryWithBackoffUntilSuccess(options retryOptions, f tryFunc) error {
	sleepDuration := options.BackoffMin

	for {
		result, err := f()
		if err != nil {
			return err
		}

		switch result {
		case tryResultSuccess:
			return nil

		case tryResultRetryImmediately:
			if err := options.Context.Err(); err != nil {
				return err
			}

		case tryResultRetryLater:
			if err := options.Context.Err(); err != nil {
				return err
			}
			if options.RetryLogger != nil {
				options.RetryLogger(sleepDuration)
			}
			if !options.FakeSleep {
				err = sleepWithContext(sleepDuration, options.Context)
				if err != nil {
					return err
				}
			}
			sleepDuration = calcSleepDuration(sleepDuration, options.BackoffMin,
				options.BackoffMax, options.BackoffMultiplier)

		default:
			panic("tryFunc returned unsupported result: " + fmt.Sprintf("%v", result))
		}
	}
}

// See https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
// for a discussion on jittering approaches. The best choices are "Full Jitter"
// (fewest contended calls) and "Decorrelated Jitter" (lowest completion time).
// We choose "Decorrelated Jitter" because we care most about completion time:
// Google can probably handle a slight increase in contended calls.
func calcSleepDuration(lastValue time.Duration, backoffMin time.Duration, backoffMax time.Duration, backoffMultiplier float64) time.Duration {
	max := time.Duration(float64(lastValue) * backoffMultiplier)
	diff := max - backoffMin
	result := backoffMin
	if diff > 0 {
		result += time.Duration(rand.Uint64() % uint64(diff))
	}
	if result > backoffMax {
		result = backoffMax
	}
	return result
}

// sleepWithContext is like time.Sleep(), but is cancellable.
// Returns an error if we were cancelled, nil otherwise.
func sleepWithContext(d time.Duration, ctx context.Context) error {
	deadline, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	<-deadline.Done()
	return ctx.Err()
}

// workRegularly runs `f` every `options.Interval` seconds. It does that until `options.Context`
// is done, until `f` temporarily fails `options.MaxFailures` times in succession, or until `f`
// permanently fails.
//
// Returns `true, false` when it stopped because it the context is done.
// Returns `false, false` when it stopped because `f` failed `options.MaxFailures` times or
// because the context was done.
// Returns `false, true` when it stopped because `f` returned a permant failure.
func workRegularly(options workRegularlyOptions, f workRegularlyFunc) (result bool, permanentFailure bool) {
	var (
		failCount uint = 0
		nextTime       = time.Now().Add(options.Interval)
	)

	for options.Context.Err() == nil && failCount < options.MaxFailures && !permanentFailure {
		timeout := time.Until(nextTime)
		if timeout < 0 {
			timeout = 0
		}
		if options.ScheduleCalculatedFunc != nil {
			options.ScheduleCalculatedFunc(timeout)
		}

		err := sleepWithContext(timeout, options.Context)
		if err != nil {
			// Assert: options.Context.Err() != nil
			break
		}

		// Timed out; perform work now
		nextTime = time.Now().Add(options.Interval)
		result, permanentFailure = f()
		if result {
			failCount = 0
		} else if !permanentFailure {
			failCount++
		}
	}

	if permanentFailure {
		return false, true
	} else {
		return failCount < options.MaxFailures, false
	}
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
