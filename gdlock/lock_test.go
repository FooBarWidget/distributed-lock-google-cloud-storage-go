package gdlock

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/FooBarWidget/distributed-lock-google-cloud-storage-go/log"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/thanhpk/randstr"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
)

var _ = Describe("Lock", func() {
	const (
		GoroutineID1 = 10
		GoroutineID2 = 20
	)

	var (
		lockPath               string
		ctx                    context.Context
		cancelCtx              context.CancelFunc
		logOutput1, logOutput2 bytes.Buffer
		logger1, logger2       log.Logger
		loggerMutex            sync.Mutex
		lock, lock2            *Lock
		err                    error
	)

	requireEnvvar := func(name string) string {
		result := os.Getenv(name)
		if len(result) == 0 {
			panic("Environment variable required: " + name)
		}
		return result
	}

	defaultConfig := func() Config {
		var config = DefaultConfig
		config.BucketName = requireEnvvar("TEST_GCLOUD_BUCKET")
		config.Path = lockPath
		config.Logger = &logger1
		config.LoggerMutex = &loggerMutex
		config.ClientOptions = []option.ClientOption{option.WithCredentialsFile(requireEnvvar("TEST_GCLOUD_CREDENTIALS_PATH"))}
		return config
	}

	gcloudBucket := func() *storage.BucketHandle {
		client, err := storage.NewClient(ctx, option.WithCredentialsFile(requireEnvvar("TEST_GCLOUD_CREDENTIALS_PATH")))
		Expect(err).ToNot(HaveOccurred())
		return client.Bucket(requireEnvvar("TEST_GCLOUD_BUCKET"))
	}

	forceEraseLockObject := func() {
		err = gcloudBucket().Object(lockPath).Delete(ctx)
		Expect(err).To(
			Or(
				Not(HaveOccurred()),
				MatchError(storage.ErrObjectNotExist),
			),
		)
	}

	BeforeSuite(func() {
		lockPath = fmt.Sprintf("go-lock.%s.%d.%d", randstr.Hex(16), os.Getpid(), GinkgoParallelNode())
	})

	BeforeEach(func() {
		ctx, cancelCtx = context.WithTimeout(context.Background(), 25*time.Second)
		logOutput1 = bytes.Buffer{}
		logOutput2 = bytes.Buffer{}
		logger1 = log.NewLogger(&logOutput1)
		logger1.SetLevel(log.Debug)
		logger2 = log.NewLogger(&logOutput2)
		logger2.SetLevel(log.Debug)
		loggerMutex = sync.Mutex{}
	})

	AfterEach(func() {
		if cancelCtx != nil {
			cancelCtx()
		}
		if CurrentGinkgoTestDescription().Failed {
			loggerMutex.Lock()
			defer loggerMutex.Unlock()
			if logOutput1.Len() > 0 || logOutput2.Len() > 0 {
				fmt.Fprintf(os.Stderr, "\n----------- Diagnostics for test: %s ----------\n", CurrentGinkgoTestDescription().FullTestText)
				if logOutput1.Len() > 0 {
					fmt.Fprintln(os.Stderr, "Lock debug logs 1:")
					fmt.Fprintln(os.Stderr, logOutput1.String())
				}
				if logOutput1.Len() > 0 && logOutput2.Len() > 0 {
					fmt.Fprint(os.Stderr, "\n\n")
				}
				if logOutput2.Len() > 0 {
					fmt.Fprintln(os.Stderr, "Lock debug logs 2:")
					fmt.Fprintln(os.Stderr, logOutput2.String())
				}
				fmt.Fprintln(os.Stderr, "---------------------")
			}
		}
	})

	Describe("initial state", func() {
		BeforeEach(func() {
			lock, err = New(ctx, defaultConfig())
			Expect(err).ToNot(HaveOccurred())
		})

		It("is not locked", func() {
			Expect(lock.LockedAccordingToInternalState()).To(BeFalse())

			val, err := lock.LockedAccordingToServer(ctx)
			Expect(err).ToNot(HaveOccurred())
			Expect(val).To(BeFalse())
		})

		It("is not owned", func() {
			Expect(lock.OwnedAccordingToInternalState(GoroutineID1)).To(BeFalse())

			val, err := lock.OwnedAccordingToServer(ctx, GoroutineID1)
			Expect(err).ToNot(HaveOccurred())
			Expect(val).To(BeFalse())
		})

		It("has no refresh error", func() {
			Expect(lock.LastRefreshError()).To(BeNil())
		})

		Specify("checking for health is not possible due to being unlocked", func() {
			_, err = lock.Healthy()
			Expect(err).To(HaveOccurred())
			Expect(errors.Is(err, ErrNotLocked)).To(BeTrue())
		})
	})

	Describe(".Lock", func() {
		AfterEach(func() {
			if lock != nil {
				err = lock.Abandon(ctx)
				Expect(err).ToNot(HaveOccurred())
			}
			if lock2 != nil {
				err = lock.Abandon(ctx)
				Expect(err).ToNot(HaveOccurred())
			}
		})

		FIt("works", func() {
			forceEraseLockObject()
			lock, err = New(ctx, defaultConfig())
			Expect(err).ToNot(HaveOccurred())

			_, err := lock.Lock(LockOptions{
				Context:     ctx,
				GoroutineID: GoroutineID1,
			})
			Expect(err).ToNot(HaveOccurred())

			Expect(lock.LockedAccordingToInternalState()).To(BeTrue())
			Expect(lock.OwnedAccordingToInternalState(GoroutineID1)).To(BeTrue())

			val, err := lock.LockedAccordingToServer(ctx)
			Expect(err).ToNot(HaveOccurred())
			Expect(val).To(BeTrue())

			val, err = lock.OwnedAccordingToServer(ctx, GoroutineID1)
			Expect(err).ToNot(HaveOccurred())
			Expect(val).To(BeTrue())

			val, err = lock.Healthy()
			Expect(err).ToNot(HaveOccurred())
			Expect(val).To(BeTrue())

			Expect(lock.LastRefreshError()).ToNot(HaveOccurred())
		})
	})
})
