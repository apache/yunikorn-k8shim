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
	defaultLog  = "log.level"
	logPrefix   = "log."
	levelSuffix = ".level"
)

// Predefined loggers: when adding new loggers, ids must be sequential, and all must be added to the loggers slice in the same order

var K8Shim = &LoggerHandle{id: 1, name: "k8shim"}
var Kubernetes = &LoggerHandle{id: 2, name: "kubernetes"}
var Admission = &LoggerHandle{id: 3, name: "admission"}
var Test = &LoggerHandle{id: 4, name: "test"}

var loggers = []*LoggerHandle{
	K8Shim,
	Kubernetes,
	Admission,
	Test,
}

type loggerConfig struct {
	loggers []*zap.Logger
}

var currentLoggerConfig = atomic.Pointer[loggerConfig]{}
var defaultLogger = atomic.Pointer[LoggerHandle]{}

// Logger retrieves the global logger
func Logger() *zap.Logger {
	once.Do(initLogger)
	return Log(defaultLogger.Load())
}

// RootLogger retrieves the root logger
func RootLogger() *zap.Logger {
	once.Do(initLogger)
	return logger
}

// Log retrieves a standard logger
func Log(handle *LoggerHandle) *zap.Logger {
	once.Do(initLogger)
	if handle == nil || handle.id == 0 {
		handle = defaultLogger.Load()
	}
	conf := currentLoggerConfig.Load()
	return conf.loggers[handle.id-1]
}

func createLogger(levelMap map[string]zapcore.Level, name string) *zap.Logger {
	level := loggerLevel(levelMap, name)
	return logger.Named(name).WithOptions(zap.WrapCore(func(inner zapcore.Core) zapcore.Core {
		return filteredCore{inner: inner, level: level}
	}))
}

func loggerLevel(levelMap map[string]zapcore.Level, name string) zapcore.Level {
	for ; name != ""; name = parentLogger(name) {
		if level, ok := levelMap[name]; ok {
			return level
		}
	}
	if level, ok := levelMap[""]; ok {
		return level
	}
	return zapcore.InfoLevel
}

func parentLogger(name string) string {
	i := strings.LastIndex(name, ".")
	if i < 0 {
		return ""
	}
	return name[0:i]
}

func initLogger() {
	defaultLogger.Store(K8Shim)
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
	_ = Logger()
	return zapConfigs
}

// SetDefaultLogger allows customization of the default logger
func SetDefaultLogger(handle *LoggerHandle) {
	once.Do(initLogger)
	defaultLogger.Store(handle)
}

// UpdateLoggingConfig is used to reconfigure logging.
// This uses config keys of the form log.{logger}.level={level}.
// The default level is set by log.level={level}
func UpdateLoggingConfig(config map[string]string) {
	once.Do(initLogger)
	initLoggingConfig(config)
}

func initLoggingConfig(config map[string]string) {
	levelMap := make(map[string]zapcore.Level)
	levelMap[""] = zapcore.InfoLevel
	zapLoggers := make([]*zap.Logger, len(loggers))

	// override default level if found
	if defaultLevel, ok := config[defaultLog]; ok {
		if levelRef := parseLevel(defaultLevel); levelRef != nil {
			levelMap[""] = *levelRef
		}
	}

	// parse out log entries and build level map
	for k, v := range config {
		if strings.Contains(k, "..") || strings.Contains(k, " ") {
			// disallow spaces and periods
			continue
		}
		name, ok := strings.CutPrefix(k, logPrefix)
		if !ok {
			continue
		}
		name, ok = strings.CutSuffix(name, levelSuffix)
		if !ok {
			continue
		}
		if levelRef := parseLevel(v); levelRef != nil {
			levelMap[name] = *levelRef
		}
	}

	minLevel := zapcore.InvalidLevel - 1
	for _, v := range levelMap {
		if minLevel > v {
			minLevel = v
		}
	}

	for i := 0; i < len(loggers); i++ {
		zapLoggers[i] = createLogger(levelMap, loggers[i].name)
	}
	newLoggerConfig := loggerConfig{loggers: zapLoggers}
	zapConfigs.Level.SetLevel(minLevel)
	currentLoggerConfig.Store(&newLoggerConfig)
}

func parseLevel(level string) *zapcore.Level {
	// parse text
	zapLevel, err := zapcore.ParseLevel(level)
	if err == nil {
		return &zapLevel
	}

	// parse numeric
	levelNum, err := strconv.ParseInt(level, 10, 31)
	if err == nil {
		zapLevel = zapcore.Level(levelNum)
		if zapLevel < zapcore.DebugLevel {
			zapLevel = zapcore.DebugLevel
		}
		if zapLevel >= zapcore.InvalidLevel {
			zapLevel = zapcore.InvalidLevel - 1
		}
		return &zapLevel
	}

	// parse failed
	return nil
}
