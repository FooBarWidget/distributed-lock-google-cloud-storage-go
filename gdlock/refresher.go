package gdlock

import (
	"context"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
)

func (l *Lock) spawnRefresherThread() {
	abortionContext, abort := context.WithCancel(context.Background())
	waitContext, done := context.WithCancel(context.Background())
	l.refresherWaiter = waitContext
	l.refresherAbort = abort

	var alive uint32 = 1
	l.refresherLiveState = &alive

	go l.refresherGoroutineMain(abortionContext, done, l.refresherLiveState, l.refresherGeneration)
}

// shutdownRefresherGoroutine aborts the refresher goroutine, but does not wait for it to finish shutting down.
// Use a Context on which one can wait for the shutting down to finish.
func (l *Lock) shutdownRefresherGoroutine() context.Context {
	waiter := l.refresherWaiter
	l.refresherWaiter = nil
	l.refresherAbort()
	l.refresherAbort = nil
	l.refresherLiveState = nil
	l.refresherGeneration++
	return waiter
}

func (l *Lock) refresherGoroutineMain(ctx context.Context, done context.CancelFunc, refresherLiveState *uint32, refresherGeneration uint64) {
	defer func() {
		atomic.StoreUint32(refresherLiveState, 0)
		l.logDebug("Exiting refresher goroutine")
		done()
	}()

	opts := workRegularlyOptions{
		Context:     ctx,
		Interval:    l.config.RefreshInterval,
		MaxFailures: l.config.MaxRefreshFails,
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
				err = ErrLockObjectChangedUnexpectedly
				permanentFailure = true
			} else if gerr.Code == 404 {
				err = ErrLockObjectDeletedUnexpectedly
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
	l.metageneration = attrs.Metageneration
	unlockStateMutex()

	l.logDebug("Done refreshing lock. metageneration=%v", attrs.Metageneration)
	return true, false
}
