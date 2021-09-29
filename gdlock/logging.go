package gdlock

func (l *Lock) logError(format string, v ...interface{}) {
	if l.config.LoggerMutex != nil {
		l.config.LoggerMutex.Lock()
		defer l.config.LoggerMutex.Unlock()
	}
	l.config.Logger.Error(format, v...)
}

func (l *Lock) logWarn(format string, v ...interface{}) {
	if l.config.LoggerMutex != nil {
		l.config.LoggerMutex.Lock()
		defer l.config.LoggerMutex.Unlock()
	}
	l.config.Logger.Warn(format, v...)
}

func (l *Lock) logInfo(format string, v ...interface{}) {
	if l.config.LoggerMutex != nil {
		l.config.LoggerMutex.Lock()
		defer l.config.LoggerMutex.Unlock()
	}
	l.config.Logger.Info(format, v...)
}

func (l *Lock) logDebug(format string, v ...interface{}) {
	if l.config.LoggerMutex != nil {
		l.config.LoggerMutex.Lock()
		defer l.config.LoggerMutex.Unlock()
	}
	l.config.Logger.Debug(format, v...)
}
