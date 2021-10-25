package gdlock

import (
	"errors"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/storage"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/thanhpk/randstr"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
)

var _ = FDescribe("Lock", func() {
	const (
		GoroutineID1 = 10
		GoroutineID2 = 20
	)

	var (
		lockPath = fmt.Sprintf("go-lock.%s.%d", randstr.Hex(16), os.Getpid())

		ctx         context.Context
		cancelCtx   context.CancelFunc
		lock, lock2 *Lock
		err         error
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
		config.ClientOptions = []option.ClientOption{option.WithCredentialsFile(requireEnvvar("TEST_GCLOUD_CREDENTIALS_PATH"))}
		return config
	}

	gcloudBucket := func() *storage.BucketHandle {
		client, err := storage.NewClient(ctx, option.WithCredentialsFile(requireEnvvar("TEST_GCLOUD_CREDENTIALS_PATH")))
		Expect(err).ToNot(HaveOccurred())
		return client.Bucket(requireEnvvar("TEST_GCLOUD_BUCKET"))
	}

	forceEraseLockObject := func() {
		writer := gcloudBucket().Object(lockPath).NewWriter(ctx)
		writer.ObjectAttrs.CacheControl = "no-store"
		err = writer.Close()
		Expect(err).ToNot(HaveOccurred())
	}

	BeforeEach(func() {
		ctx, cancelCtx = context.WithTimeout(context.Background(), 30*time.Second)
	})

	AfterEach(func() {
		if cancelCtx != nil {
			cancelCtx()
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

		It("works", func() {
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
