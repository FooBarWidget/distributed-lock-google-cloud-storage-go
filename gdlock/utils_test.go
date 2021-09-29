package gdlock

import (
	"context"
	"errors"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("retryWithBackoffUntilSuccess", func() {
	Describe("when f returns tryResultSuccess", func() {
		It("returns immediately", func() {
			iteration := 0
			options := retryOptions{
				Context:           context.Background(),
				BackoffMin:        100 * time.Millisecond,
				BackoffMax:        1 * time.Second,
				BackoffMultiplier: 2,
			}

			err := retryWithBackoffUntilSuccess(options, func() (tryResult, error) {
				iteration++
				return tryResultSuccess, nil
			})

			Expect(iteration).To(BeNumerically("==", 1))
			Expect(err).To(BeNil())
		})
	})

	Describe("when f returns tryResultRetryImmediately", func() {
		It("retries immediately without sleeping", func() {
			iteration := 0
			slept := 0
			options := retryOptions{
				Context:           context.Background(),
				RetryLogger:       func(_ time.Duration) { slept++ },
				BackoffMin:        100 * time.Millisecond,
				BackoffMax:        1 * time.Second,
				BackoffMultiplier: 2,
				FakeSleep:         true,
			}

			err := retryWithBackoffUntilSuccess(options, func() (tryResult, error) {
				iteration++
				if iteration < 3 {
					return tryResultRetryImmediately, nil
				} else {
					return tryResultSuccess, nil
				}
			})

			Expect(iteration).To(BeNumerically("==", 3))
			Expect(slept).To(BeNumerically("==", 0))
			Expect(err).To(BeNil())
		})

		It("returns an error when the context is done", func() {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			options := retryOptions{
				Context:           ctx,
				BackoffMin:        100 * time.Millisecond,
				BackoffMax:        1 * time.Second,
				BackoffMultiplier: 2,
				FakeSleep:         true,
			}

			err := retryWithBackoffUntilSuccess(options, func() (tryResult, error) {
				return tryResultRetryImmediately, nil
			})

			Expect(err).To(Equal(context.Canceled))
		})
	})

	Describe("when f returns tryResultRetryLater", func() {
		It("retries after sleeping", func() {
			iteration := 0
			slept := 0
			options := retryOptions{
				Context:           context.Background(),
				RetryLogger:       func(_ time.Duration) { slept++ },
				BackoffMin:        100 * time.Millisecond,
				BackoffMax:        1 * time.Second,
				BackoffMultiplier: 2,
				FakeSleep:         true,
			}

			err := retryWithBackoffUntilSuccess(options, func() (tryResult, error) {
				iteration++
				if iteration < 3 {
					return tryResultRetryLater, nil
				} else {
					return tryResultSuccess, nil
				}
			})

			Expect(iteration).To(BeNumerically("==", 3))
			Expect(slept).To(BeNumerically("==", 2))
			Expect(err).To(BeNil())
		})

		It("returns an error when the context is done", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			slept := 0
			options := retryOptions{
				Context:           ctx,
				RetryLogger:       func(_ time.Duration) { slept++ },
				BackoffMin:        100 * time.Millisecond,
				BackoffMax:        1 * time.Second,
				BackoffMultiplier: 2,
				FakeSleep:         true,
			}

			err := retryWithBackoffUntilSuccess(options, func() (tryResult, error) {
				cancel()
				return tryResultRetryLater, nil
			})

			Expect(slept).To(BeNumerically("==", 0))
			Expect(err).To(Equal(context.Canceled))
		})
	})

	Describe("when f returns an error", func() {
		It("returns immediately with that error", func() {
			iteration := 0
			options := retryOptions{
				Context:           context.Background(),
				BackoffMin:        100 * time.Millisecond,
				BackoffMax:        1 * time.Second,
				BackoffMultiplier: 2,
				FakeSleep:         true,
			}

			err := retryWithBackoffUntilSuccess(options, func() (tryResult, error) {
				iteration++
				return tryResultNone, errors.New("oh no")
			})

			Expect(iteration).To(BeNumerically("==", 1))
			Expect(err).To(HaveOccurred())
		})
	})
})

var _ = Describe("workRegularly", func() {
	It("calls f until the Context is done", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		called := 0
		options := workRegularlyOptions{
			Context:     ctx,
			Interval:    0,
			MaxFailures: 3,
		}

		result, _ := workRegularly(options, func() (result bool, permanentFailure bool) {
			called++
			if called >= 2 {
				cancel()
			}
			return true, false
		})

		Expect(called).To(BeNumerically("==", 2))
		Expect(result).To(BeTrue())
	})

	It("calls f until it temporarily fails too many times in succession", func() {
		called := 0
		options := workRegularlyOptions{
			Context:     context.Background(),
			Interval:    0,
			MaxFailures: 3,
		}

		result, permanentFailure := workRegularly(options, func() (result bool, permanentFailure bool) {
			called++
			return false, false
		})

		Expect(called).To(BeNumerically("==", 3))
		Expect(result).To(BeFalse())
		Expect(permanentFailure).To(BeFalse())
	})

	It("calls f until it permanently fails", func() {
		called := 0
		options := workRegularlyOptions{
			Context:     context.Background(),
			Interval:    0,
			MaxFailures: 3,
		}

		result, permanentFailure := workRegularly(options, func() (result bool, permanentFailure bool) {
			called++
			if called >= 2 {
				return false, true
			} else {
				return false, false
			}
		})

		Expect(called).To(BeNumerically("==", 2))
		Expect(result).To(BeFalse())
		Expect(permanentFailure).To(BeTrue())
	})

	It("resets the failure counter when f succeeds", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		called := 0
		options := workRegularlyOptions{
			Context:     ctx,
			Interval:    0,
			MaxFailures: 3,
		}

		result, _ := workRegularly(options, func() (result bool, permanentFailure bool) {
			called++
			if called >= 10 {
				cancel()
			}
			return called%2 == 0, false
		})

		Expect(called).To(BeNumerically("==", 10))
		Expect(result).To(BeTrue())
	})
})
