/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package log

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var once sync.Once
var logger *zap.Logger
var zapConfigs *zap.Config

// LoggerHandle is used to efficiently look up logger references
type LoggerHandle struct {
	id   int
	name string
}

func (h LoggerHandle) String() string {
	return h.name
}

// Logger constants for configuration
const (
	nullLogger  = ""
	defaultLog  = "log.level"
	logPrefix   = "log."
	levelSuffix = ".level"
)

// Defined loggers: when adding new loggers, ids must be sequential, and all must be added to the loggers slice in the same order
var (
	Shim                 = &LoggerHandle{id: 0, name: "shim"}
	Kubernetes           = &LoggerHandle{id: 1, name: "kubernetes"}
	Test                 = &LoggerHandle{id: 2, name: "test"}
	Admission            = &LoggerHandle{id: 3, name: "admission"}
	AdmissionClient      = &LoggerHandle{id: 4, name: "admission.client"}
	AdmissionConf        = &LoggerHandle{id: 5, name: "admission.conf"}
	AdmissionWebhook     = &LoggerHandle{id: 6, name: "admission.webhook"}
	AdmissionUtils       = &LoggerHandle{id: 7, name: "admission.utils"}
	ShimContext          = &LoggerHandle{id: 8, name: "shim.context"}
	ShimFSM              = &LoggerHandle{id: 9, name: "shim.fsm"}
	ShimCacheApplication = &LoggerHandle{id: 10, name: "shim.cache.application"}
	ShimCacheAppMgmt     = &LoggerHandle{id: 11, name: "shim.cache.appmgmt"}
	ShimCacheNode        = &LoggerHandle{id: 12, name: "shim.cache.node"}
	ShimCacheTask        = &LoggerHandle{id: 13, name: "shim.cache.task"}
	ShimCacheExternal    = &LoggerHandle{id: 14, name: "shim.cache.external"}
	ShimCachePlaceholder = &LoggerHandle{id: 15, name: "shim.cache.placeholder"}
	ShimRMCallback       = &LoggerHandle{id: 16, name: "shim.rmcallback"}
	ShimClient           = &LoggerHandle{id: 17, name: "shim.client"}
	ShimResources        = &LoggerHandle{id: 18, name: "shim.resources"}
	ShimUtils            = &LoggerHandle{id: 19, name: "shim.utils"}
	ShimConfig           = &LoggerHandle{id: 20, name: "shim.config"}
	ShimDispatcher       = &LoggerHandle{id: 21, name: "shim.dispatcher"}
	ShimScheduler        = &LoggerHandle{id: 22, name: "shim.scheduler"}
	ShimSchedulerPlugin  = &LoggerHandle{id: 23, name: "shim.scheduler.plugin"}
	ShimPredicates       = &LoggerHandle{id: 24, name: "shim.predicates"}
	ShimFramework        = &LoggerHandle{id: 25, name: "shim.framework"}
)

// this tracks all the known logger handles, used to preallocate the real logger instances when configuration changes
var loggers = []*LoggerHandle{
	Shim, Kubernetes, Test,
	Admission, AdmissionClient, AdmissionConf, AdmissionWebhook, AdmissionUtils, ShimContext, ShimFSM,
	ShimCacheApplication, ShimCacheAppMgmt, ShimCacheNode, ShimCacheTask, ShimCacheExternal, ShimCachePlaceholder,
	ShimRMCallback, ShimClient, ShimResources, ShimUtils, ShimConfig, ShimDispatcher,
	ShimScheduler, ShimSchedulerPlugin, ShimPredicates, ShimFramework,
}

// structure to hold all current logger configuration state
type loggerConfig struct {
	loggers []*zap.Logger
}

// tracks the currently used set of loggers; replaced completely whenever configuration changes
var currentLoggerConfig = atomic.Pointer[loggerConfig]{}

// RootLogger retrieves the root logger, used to pass the configured logger to the scheduler core on startup
func RootLogger() *zap.Logger {
	once.Do(initLogger)
	return logger
}

// Log retrieves a named logger
func Log(handle *LoggerHandle) *zap.Logger {
	once.Do(initLogger)
	conf := currentLoggerConfig.Load()
	if handle == nil {
		return conf.loggers[0]
	}
	return conf.loggers[handle.id]
}

// createLogger creates a new, named logger that has log levels filtered based on the given configuration.
// levelMap contains a mapping of fully-qualified logger names to log levels
func createLogger(levelMap map[string]zapcore.Level, name string) *zap.Logger {
	level := loggerLevel(levelMap, name)
	return logger.Named(name).WithOptions(zap.WrapCore(func(inner zapcore.Core) zapcore.Core {
		return filteredCore{inner: inner, level: level}
	}))
}

// loggerLevel computes the log level that should be associated with an arbitrarily named logger.
// The levelMap is used to look up the level. If not found, the parent logger is looked up until no further
// parents can be tried, consulting levelMap for each possible match. For example, given a logger named
// k8shim.context.cache, the following keys will be looked up in levelMap:
//
//	"k8shim.context.cache"
//	"k8shim.context"
//	"k8shim"
//	"" (default logger)
//
// The first key that returns a match determines the log level returned. This allows a level of
// "k8shim" to control all the child loggers as well if there is not a more specific override.
// If no configuration can be found, even for the default empty logger, InfoLevel will be returned.
func loggerLevel(levelMap map[string]zapcore.Level, name string) zapcore.Level {
	for ; name != nullLogger; name = parentLogger(name) {
		if level, ok := levelMap[name]; ok {
			return level
		}
	}
	if level, ok := levelMap[nullLogger]; ok {
		return level
	}
	return zapcore.InfoLevel
}

// parentLogger returns the name of the parent logger or the empty string "" if no parent exists.
// Loggers are named with periods (.) as separators; i.e. the parent logger of "a.b.c" is "a.b", the parent of
// "a.b" is "a", and the parent of "a" is "".
func parentLogger(name string) string {
	i := strings.LastIndex(name, ".")
	if i < 0 {
		// no remaining periods; parent is the null logger
		return nullLogger
	}
	return name[0:i]
}

func initLogger() {
	outputPaths := []string{"stdout"}

	zapConfigs = &zap.Config{
		Level:             zap.NewAtomicLevelAt(zapcore.Level(0)),
		Development:       false,
		DisableCaller:     false,
		DisableStacktrace: false,
		Sampling:          nil,
		Encoding:          "console",
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey:    "message",
			LevelKey:      "level",
			TimeKey:       "time",
			NameKey:       "logger",
			CallerKey:     "caller",
			StacktraceKey: "stacktrace",
			LineEnding:    zapcore.DefaultLineEnding,
			// note: https://godoc.org/go.uber.org/zap/zapcore#EncoderConfig
			// only EncodeName is optional all others must be set
			EncodeLevel:    zapcore.CapitalLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
		OutputPaths:      outputPaths,
		ErrorOutputPaths: []string{"stderr"},
	}

	var err error
	logger, err = zapConfigs.Build()
	// this should really not happen so just write to stdout and set a Nop logger
	if err != nil {
		fmt.Printf("Logging disabled, logger init failed with error: %v", err)
		logger = zap.NewNop()
	}

	// initialize sub-loggers
	initLoggingConfig(nil)

	// make sure logs are flushed
	//nolint:errcheck
	defer logger.Sync()
}

func GetZapConfigs() *zap.Config {
	// force init
	_ = Log(Shim)
	return zapConfigs
}

// UpdateLoggingConfig is used to reconfigure logging. This uses config keys of the form log.{logger}.level={level}.
// The default level is set by log.level={level}. The {level} value can be either numeric (-1 through 5), or
// textual (DEBUG, INFO, WARN, ERROR, DPANIC, PANIC, or ERROR). See zapcore documentation for more details.
func UpdateLoggingConfig(config map[string]string) {
	once.Do(initLogger)
	initLoggingConfig(config)
}

// initLoggingConfig replaces the existing set of loggers with new ones configured according to the given
// configuration. All keys of the form "log.{name}.level" will be parsed and a map of logger name -> logger level
// will be created. For each defined logger handle (see above), a new logger instance is created using the
// most specific configuration found. For example, a logger named "a.b.c" will be configured by "log.a.b.c.level".
// If this key does not exist, "a.b" will be consulted, then "a", and finally "". If no configuration is found for a
// given key, Info will be used. Finally, the finest log level specified in any configuration will be used to set the
// zap log level of the root logger. So if two loggers (one INFO, and one DEBUG) are configured, the root logger will
// be set to DEBUG level. If both loggers were at INFO level, the root logger would be set to INFO.
// Each configured logger will filter log messages at its own level by wrapping the underlying zap.Core implementation
// with one that checks the enabled log level first.
func initLoggingConfig(config map[string]string) {
	levelMap := make(map[string]zapcore.Level)
	levelMap[nullLogger] = zapcore.InfoLevel
	zapLoggers := make([]*zap.Logger, len(loggers))

	// override default level if found (log.level key)
	if defaultLevel, ok := config[defaultLog]; ok {
		if levelRef := parseLevel(defaultLevel); levelRef != nil {
			levelMap[nullLogger] = *levelRef
		}
	}

	// parse out log entries and build level map
	for k, v := range config {
		// disallow spaces and periods
		if strings.Contains(k, "..") || strings.Contains(k, " ") {
			continue
		}
		// ensure config key starts with "log."
		name, ok := strings.CutPrefix(k, logPrefix)
		if !ok {
			continue
		}
		// ensure config key ends with ".level"
		name, ok = strings.CutSuffix(name, levelSuffix)
		if !ok {
			continue
		}
		// if value is a valid log level, store it in the level map
		if levelRef := parseLevel(v); levelRef != nil {
			levelMap[name] = *levelRef
		}
	}

	// compute the finest log level necessary to allow all loggers to succeed
	minLevel := zapcore.InvalidLevel - 1
	for _, v := range levelMap {
		minLevel = min(v, minLevel)
	}

	// create each configured logger and initialize the overall configuration
	for i := 0; i < len(loggers); i++ {
		zapLoggers[i] = createLogger(levelMap, loggers[i].name)
	}
	newLoggerConfig := loggerConfig{loggers: zapLoggers}

	// update the root zap logger level
	zapConfigs.Level.SetLevel(minLevel)

	// atomically update the set of loggers
	currentLoggerConfig.Store(&newLoggerConfig)
}

// parseLevel parses a textual (or numeric) log level into a zapcore.Level instance. Both numeric (-1 <= level <= 5)
// and textual (DEBUG, INFO, WARN, ERROR, DPANIC, PANIC, FATAL) are supported.
func parseLevel(level string) *zapcore.Level {
	// parse text
	zapLevel, err := zapcore.ParseLevel(level)
	if err == nil {
		return &zapLevel
	}

	// parse numeric
	levelNum, err := strconv.ParseInt(level, 10, 8)
	if err == nil {
		// Validate levelNum is within valid zapcore.Level range before conversion
		switch {
		case levelNum < int64(zapcore.DebugLevel):
			zapLevel = zapcore.DebugLevel
		case levelNum >= int64(zapcore.InvalidLevel):
			zapLevel = zapcore.InvalidLevel - 1
		default:
			zapLevel = zapcore.Level(levelNum)
		}
		return &zapLevel
	}

	// parse failed
	return nil
}
