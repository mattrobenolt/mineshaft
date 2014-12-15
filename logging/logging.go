package logging

import (
	log "code.google.com/p/log4go"

	"flag"
	"fmt"
	"os"
	"strings"
)

var (
	loglevel = flag.String("l", "error", "Log level")
	logger   log.Logger
)

func SetupLogging() {
	flag.Parse()

	level := log.ERROR

	switch strings.ToLower(*loglevel) {
	case "debug":
		level = log.DEBUG
	case "trace":
		level = log.TRACE
	case "info":
		level = log.INFO
	case "warning":
		level = log.WARNING
	case "error":
		level = log.ERROR
	case "critical":
		level = log.CRITICAL
	default:
		panic(fmt.Sprintf("Unknown log level: %v", *loglevel))
	}

	logger = log.NewDefaultLogger(level)
}

func GetDefaultLogger() log.Logger {
	if logger == nil {
		SetupLogging()
	}
	return logger
}

func Debug(arg0 interface{}, args ...interface{})    { GetDefaultLogger().Debug(arg0, args...) }
func Trace(arg0 interface{}, args ...interface{})    { GetDefaultLogger().Trace(arg0, args...) }
func Info(arg0 interface{}, args ...interface{})     { GetDefaultLogger().Info(arg0, args...) }
func Warn(arg0 interface{}, args ...interface{})     { GetDefaultLogger().Warn(arg0, args...) }
func Error(arg0 interface{}, args ...interface{})    { GetDefaultLogger().Error(arg0, args...) }
func Critical(arg0 interface{}, args ...interface{}) { GetDefaultLogger().Critical(arg0, args...) }

// For compatability
func Println(arg0 interface{}, args ...interface{}) { Info(arg0, args...) }
func Fatal(arg0 interface{}, args ...interface{}) {
	Critical(arg0, args...)
	os.Exit(1)
}
