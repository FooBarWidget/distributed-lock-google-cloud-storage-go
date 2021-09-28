package gdlock

import (
	"context"
	"errors"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
)

func (l *Lock) spawnRefresherThread() {
	ctx, cancel := context.WithCancel(context.Background())
	l.refresherWaiter = &sync.WaitGroup{}
	l.refresherCanceller = cancel
	l.refresherWaiter.Add(1)
	go l.refresherGoroutineMain(ctx, l.refresherWaiter, l.refresherGeneration)
}

// shutdownRefresherGoroutine Cancels the refresher goroutine, but does not wait for it to finish shutting down.
// Use the returned WaitGroup to wait.
func (l *Lock) shutdownRefresherGoroutine() *sync.WaitGroup {
	waiter := l.refresherWaiter
	l.refresherCanceller()
	l.refresherCanceller = nil
	l.refresherGeneration++
	l.refresherWaiter = nil
	return waiter
}

func (l *Lock) refresherGoroutineMain(ctx context.Context, waiter *sync.WaitGroup, refresherGeneration uint64) {
	defer func() {
		l.logDebug("Exiting refresher goroutine")
		waiter.Done()
	}()

	opts := workRegularlyOptions{
		Mutex:           &l.stateMutex,
		Context:         ctx,
		RefreshInterval: l.config.RefreshInterval,
		MaxFailures:     l.config.MaxRefreshFails,
		ScheduleCalculatedFunc: func(timeout time.Duration) {
			l.logDebug("Next lock refresh in %.1fs", float64(timeout)/1000000000)
		},
	}

	result, permanent := workRegularly(opts, func() (bool, bool) {
		return l.refreshLock(ctx, refresherGeneration)
	})

	if !result {
		if permanent {
			l.logError("Lock refresh failed permanently. Declaring lock as unhealthy")
		} else {
			l.logError("Lock refresh failed %d times in succession. Declaring lock as unhealthy",
				l.config.MaxRefreshFails)
		}
	}
}

func (l *Lock) refreshLock(ctx context.Context, refresherGeneration uint64) (result bool, permanentFailure bool) {
	unlockStateMutex := lockMutex(&l.stateMutex)
	defer unlockStateMutex()
	if l.refresherGeneration != refresherGeneration {
		return true, false
	}
	metageneration := l.metageneration
	unlockStateMutex()

	l.logInfo("Refreshing lock")
	file := l.bucket.Object(l.config.Path)
	attrs, err := file.
		If(storage.Conditions{GenerationMatch: metageneration}).
		Update(ctx, storage.ObjectAttrsToUpdate{
			Metadata: map[string]string{
				"expires_at": l.ttlTimestampString(),
			},
		})
	if err != nil {
		if gerr, ok := err.(*googleapi.Error); ok {
			if gerr.Code == 412 {
				err = errors.New("Lock object has an unexpected metageneration number")
				permanentFailure = true
			} else if gerr.Code == 404 {
				err = errors.New("Lock object has been unexpectedly deleted")
				permanentFailure = true
			}
		}

		unlockStateMutex = lockMutex(&l.stateMutex)
		if l.refresherGeneration != refresherGeneration {
			l.logDebug("Abort refreshing lock")
			return true, false
		}
		l.refresherError = err
		unlockStateMutex()

		l.logError("Error refreshing lock: %s", err.Error())
		return false, permanentFailure
	}

	unlockStateMutex = lockMutex(&l.stateMutex)
	if l.refresherGeneration != refresherGeneration {
		l.logDebug("Abort refreshing lock")
		return true, false
	}
	metageneration = l.metageneration
	unlockStateMutex()

	l.logDebug("Done refreshing lock. metageneration=%v", attrs.Metageneration)
	return true, false
}

func (l *Lock) handleRefreshLockError(err error, refresherGeneration uint64, permanentFailure bool) (bool, bool) {
	unlockStateMutex := lockMutex(&l.stateMutex)
	defer unlockStateMutex()

	if l.refresherGeneration != refresherGeneration {
		l.logDebug("Abort refreshing lock")
		return true, false
	}

	l.refresherError = err
	unlockStateMutex()

	l.logError("Error refreshing lock: %s", err.Error())
	return false, permanentFailure
}
