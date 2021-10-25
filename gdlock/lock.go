package gdlock

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/FooBarWidget/distributed-lock-google-cloud-storage-go/log"
	"github.com/thanhpk/randstr"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

type Lock struct {
	config Config
	bucket *storage.BucketHandle

	/****** Read-write variables protected by stateMutex ******/

	stateMutex         sync.Mutex
	owner              string
	metageneration     int64
	refresherWaiter    context.Context
	refresherAbort     context.CancelFunc
	refresherLiveState *uint32
	refresherError     error

	// refresherGeneration is incremented every time we shutdown
	// the refresher goroutine. It allows the refresher thread to know
	// whether it's being shut down (and thus shouldn't access/modify
	// state).
	refresherGeneration uint64
}

type Config struct {
	BucketName             string
	Path                   string
	InstanceIdentityPrefix string
	Logger                 log.Interface
	LoggerMutex            *sync.Mutex
	TTL                    time.Duration
	RefreshInterval        time.Duration
	MaxRefreshFails        uint
	BackoffMin             time.Duration
	BackoffMax             time.Duration
	BackoffMultiplier      float64
	ClientOptions          []option.ClientOption
}

type LockOptions struct {
	Context     context.Context
	GoroutineID uint64
}

type UnlockFunc = func(context.Context) (deleted bool, err error)

var (
	ErrAlreadyLocked                 = errors.New("already locked")
	ErrNotLocked                     = errors.New("not locked")
	ErrLockObjectChangedUnexpectedly = errors.New("lock object has an unexpected metageneration number")
	ErrLockObjectDeletedUnexpectedly = errors.New("lock object has been unexpectedly deleted")

	defaultInstanceIdentityPrefixWithoutPid = randstr.Hex(12)
)

const (
	DefaultTTL               = 5 * time.Minute
	DefaultRefreshInterval   = DefaultTTL / (DefaultMaxRefreshFails * 3)
	DefaultMaxRefreshFails   = 3
	DefaultBackoffMin        = 1 * time.Second
	DefaultBackoffMax        = 30 * time.Second
	DefaultBackoffMultiplier = 2
)

var DefaultConfig = Config{
	Logger:            &log.Default,
	TTL:               DefaultTTL,
	RefreshInterval:   DefaultRefreshInterval,
	MaxRefreshFails:   DefaultMaxRefreshFails,
	BackoffMin:        DefaultBackoffMin,
	BackoffMax:        DefaultBackoffMax,
	BackoffMultiplier: DefaultBackoffMultiplier,
}

func New(ctx context.Context, config Config) (*Lock, error) {
	client, err := createClientHandle(ctx, config)
	if err != nil {
		return &Lock{}, fmt.Errorf("error creating Google Cloud Storage client handle: %w", err)
	}

	bucket, err := createBucketHandle(client, config)
	if err != nil {
		return &Lock{}, fmt.Errorf("error creating Google Cloud Storage bucket handle: %w", err)
	}

	return &Lock{
		config: config,
		bucket: bucket,
	}, nil
}

func (l *Lock) Lock(opts LockOptions) (UnlockFunc, error) {
	if opts.Context == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*l.config.TTL)
		defer cancel()
		opts.Context = ctx
	}

	if l.OwnedAccordingToInternalState(opts.GoroutineID) {
		return nil, ErrAlreadyLocked
	}

	var (
		file  *storage.ObjectHandle
		attrs *storage.ObjectAttrs
		err   error
	)
	var retryOpts = retryOptions{
		Context: opts.Context,
		RetryLogger: func(sleepDuration time.Duration) {
			l.logInfo("Unable to acquire lock. Will try again in %.1f seconds", sleepDuration)
		},
		BackoffMin:        l.config.BackoffMin,
		BackoffMax:        l.config.BackoffMax,
		BackoffMultiplier: l.config.BackoffMultiplier,
	}

	err = retryWithBackoffUntilSuccess(retryOpts, func() (tryResult, error) {
		l.logDebug("Acquiring lock")
		file, err = l.createLockObject(opts.Context, opts.GoroutineID)
		if err != nil {
			return tryResultNone, err
		}
		if file != nil {
			l.logDebug("Successfully acquired lock")
			return tryResultSuccess, nil
		}

		l.logDebug("Error acquiring lock. Investigating why...")
		file = l.bucket.Object(l.config.Path)
		attrs, err = file.Attrs(opts.Context)
		if (err != nil && attrs == nil) || err == storage.ErrObjectNotExist {
			l.logWarn("Lock was deleted right after having created it. Retrying.")
			return tryResultRetryImmediately, nil
		}
		if err != nil {
			return tryResultNone, err
		}

		if attrs.Metadata["identity"] == l.identity(opts.GoroutineID) {
			l.logWarn("Lock was already owned by this instance, but was abandoned. Resetting lock")
			_, err = l.deleteLockObject(opts.Context, attrs.Metageneration)
			return tryResultRetryImmediately, err
		}

		if l.isLockStale(attrs) {
			l.logWarn("Lock is stale. Resetting lock")
			_, err = l.deleteLockObject(opts.Context, attrs.Metageneration)
			if err != nil {
				return tryResultNone, err
			}
		} else {
			l.logDebug("Lock was already acquired, and is not stale")
		}
		return tryResultRetryLater, nil
	})
	if err != nil {
		return nil, err
	}

	refresherGeneration := l.initializeLockedState(opts, attrs)
	l.logDebug("Locked. refresher_generation=%v, metageneration=%v", refresherGeneration, attrs.Metageneration)

	unlocker := func(ctx context.Context) (bool, error) {
		return l.unlock(ctx, opts.GoroutineID)
	}
	return unlocker, nil
}

func (l *Lock) Abandon(ctx context.Context) error {
	unlockStateMutex := lockMutex(&l.stateMutex)
	defer unlockStateMutex()

	if l.lockedAccordingToInternalState() {
		refresherGeneration := l.refresherGeneration
		waiter := l.shutdownRefresherGoroutine()
		l.owner = ""
		l.metageneration = 0
		unlockStateMutex()

		l.logDebug("Abandoning locked lock")
		select {
		case <-waiter.Done():
			// Do nothing
		case <-ctx.Done():
			return ctx.Err()
		}
		l.logDebug("Done abandoning locked lock. refresher_generation=%s", refresherGeneration)
	} else {
		unlockStateMutex()
		l.logDebug("Abandoning unlocked lock")
	}

	return nil
}

func (l *Lock) unlock(ctx context.Context, goroutineID uint64) (deleted bool, err error) {
	unlockStateMutex := lockMutex(&l.stateMutex)
	defer unlockStateMutex()

	if !l.lockedAccordingToInternalState() {
		return false, ErrNotLocked
	}

	metageneration := l.metageneration
	refresherGeneration := l.refresherGeneration
	waiter := l.shutdownRefresherGoroutine()

	l.owner = ""
	l.metageneration = 0
	unlockStateMutex()

	select {
	case <-waiter.Done():
		// Do nothing
	case <-ctx.Done():
		return false, ctx.Err()
	}

	result, err := l.deleteLockObject(ctx, metageneration)
	if err != nil {
		l.logDebug("Unlocked. refresher_generation=%v, metageneration=%v", refresherGeneration, metageneration)
	}
	return result, err
}

func (l *Lock) initializeLockedState(opts LockOptions, attrs *storage.ObjectAttrs) uint64 {
	l.stateMutex.Lock()
	defer l.stateMutex.Unlock()

	l.owner = l.identity(opts.GoroutineID)
	l.metageneration = attrs.Metageneration
	l.spawnRefresherThread()
	return l.refresherGeneration
}

// createLockObject creates the lock object in Cloud Storage. Returns the object handle
// on success, nil if object already exists, or an error when any other error occurs.
func (l *Lock) createLockObject(ctx context.Context, goroutineID uint64) (*storage.ObjectHandle, error) {
	object := l.bucket.
		Object(l.config.Path).
		If(storage.Conditions{DoesNotExist: true})
	writer := object.NewWriter(ctx)
	writer.ObjectAttrs.CacheControl = "no-store"
	writer.ObjectAttrs.Metadata = map[string]string{
		"expires_at": l.ttlTimestampString(),
		"identity":   l.identity(goroutineID),
	}

	err := writer.Close()
	if err != nil {
		gerr, isGoogleAPIError := err.(*googleapi.Error)
		if isGoogleAPIError && gerr.Code == 412 {
			return nil, nil
		} else {
			return nil, err
		}
	} else {
		return object, nil
	}
}

func (l *Lock) deleteLockObject(ctx context.Context, expectedMetageneration int64) (bool, error) {
	object := l.bucket.
		Object(l.config.Path).
		If(storage.Conditions{MetagenerationMatch: expectedMetageneration})
	err := object.Delete(ctx)
	if err != nil {
		gerr, isGoogleAPIError := err.(*googleapi.Error)
		if isGoogleAPIError && (gerr.Code == 404 || gerr.Code == 412) {
			return false, nil
		} else {
			return false, err
		}
	} else {
		return true, nil
	}
}

func createClientHandle(ctx context.Context, config Config) (*storage.Client, error) {
	return storage.NewClient(ctx, config.ClientOptions...)
}

func createBucketHandle(client *storage.Client, config Config) (*storage.BucketHandle, error) {
	return client.Bucket(config.BucketName), nil
}
