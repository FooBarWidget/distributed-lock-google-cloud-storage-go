package gdlock

func (l *Lock) logError(format string, args ...interface{}) {
	l.config.LoggerMutex.Lock()
	defer l.config.LoggerMutex.Unlock()
	// TODO
}

func (l *Lock) logWarn(format string, args ...interface{}) {
	l.config.LoggerMutex.Lock()
	defer l.config.LoggerMutex.Unlock()
	// TODO
}

func (l *Lock) logInfo(format string, args ...interface{}) {
	l.config.LoggerMutex.Lock()
	defer l.config.LoggerMutex.Unlock()
	// TODO
}

func (l *Lock) logDebug(format string, args ...interface{}) {
	l.config.LoggerMutex.Lock()
	defer l.config.LoggerMutex.Unlock()
	// TODO
}
