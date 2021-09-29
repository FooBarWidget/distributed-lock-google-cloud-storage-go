package gdlock

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"cloud.google.com/go/storage"
)

func (l *Lock) LockedAccordingToInternalState() bool {
	l.stateMutex.Lock()
	defer l.stateMutex.Unlock()
	return l.lockedAccordingToInternalState()
}

func (l *Lock) OwnedAccordingToInternalState(goroutineID uint64) bool {
	l.stateMutex.Lock()
	defer l.stateMutex.Unlock()
	return l.ownedAccordingToInternalState(goroutineID)
}

func (l *Lock) lockedAccordingToInternalState() bool {
	return len(l.owner) > 0
}

func (l *Lock) ownedAccordingToInternalState(goroutineID uint64) bool {
	return l.owner == l.identity(goroutineID)
}

func (l *Lock) isLockStale(attrs *storage.ObjectAttrs) bool {
	expiresAtStr := attrs.Metadata["expires_at"]
	expiresAt, err := strconv.ParseFloat(expiresAtStr, 64)
	if err != nil {
		expiresAt = 0
	}
	return time.Now().After(time.UnixMilli(int64(expiresAt * 1000)))
}

func (l *Lock) identity(goroutineID uint64) string {
	var result string
	if len(l.config.InstanceIdentityPrefix) == 0 {
		result = defaultInstanceIdentityPrefix()
	} else {
		result = l.config.InstanceIdentityPrefix
	}
	if goroutineID != 0 {
		result += fmt.Sprintf("/thr-%d", goroutineID)
	}
	return result
}

func (l *Lock) ttlTimestampString() string {
	return fmt.Sprintf("%f", float64(time.Now().Add(l.config.TTL).UnixMilli())/1000)
}

func defaultInstanceIdentityPrefix() string {
	return fmt.Sprintf("%s-%d", defaultInstanceIdentityPrefixWithoutPid, os.Getpid())
}
