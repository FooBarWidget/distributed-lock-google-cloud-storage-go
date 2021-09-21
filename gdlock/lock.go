package gdlock

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

type Lock struct {
	config     Config
	bucket     *storage.BucketHandle
	stateMutex sync.Mutex
}

type Config struct {
	BucketName             string
	Path                   string
	InstanceIdentityPrefix string
	Logger                 interface{}
	LoggerMutex            *sync.Mutex
	TTL                    time.Duration
	RefreshInterval        time.Duration
	MaxRefreshFails        uint
	BackoffMin             time.Duration
	BackoffMax             time.Duration
	BackoffMultiplier      float64
	BucketOption           option.ClientOption
}

type LockOptions struct {
	Context     context.Context
	GoroutineID uint
}

type UnlockFunc = func() error

func New(ctx context.Context, config Config) (*Lock, error) {
	client, err := createClientHandle(ctx, config)
	if err != nil {
		return &Lock{}, fmt.Errorf("Error creating Google Cloud Storage client handle: %w", err)
	}

	bucket, err := createBucketHandle(client, config)
	if err != nil {
		return &Lock{}, fmt.Errorf("Error creating Google Cloud Storage bucket handle: %w", err)
	}

	return &Lock{
		config: config,
		bucket: bucket,
	}, nil
}

func (l *Lock) Lock(opts LockOptions) (UnlockFunc, error) {
	if opts.Context == nil {
		ctx, cancel := context.WithTimeout(context.Background(), l.config.TTL)
		defer cancel()
		opts.Context = ctx
	}

	unlocker := func() error {
		return l.unlock(opts)
	}
	return unlocker, nil
}

func (l *Lock) unlock(opts LockOptions) error {

}

func createClientHandle(ctx context.Context, config Config) (*storage.Client, error) {
	return storage.NewClient(ctx, config.BucketOption)
}

func createBucketHandle(client *storage.Client, config Config) (*storage.BucketHandle, error) {
	return client.Bucket(config.BucketName), nil
}
