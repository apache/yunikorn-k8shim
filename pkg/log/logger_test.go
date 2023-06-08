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
	"os"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gotest.tools/v3/assert"
)

var logDir string
var logFile string

var iterations = 100000

func TestLegacyLoggerPerformance(t *testing.T) {
	_ = Logger() // make sure logger is initialized once
	initTestLogger(t)
	defer resetTestLogger(t)

	debugStart := time.Now()
	for i := 0; i < iterations; i++ {
		RootLogger().Debug("test", zap.String("foo", "bar"))
	}
	debugNsOp := (time.Since(debugStart).Nanoseconds()) / int64(iterations)

	infoStart := time.Now()
	for i := 0; i < iterations; i++ {
		RootLogger().Info("test", zap.String("foo", "bar"))
	}
	infoNsOp := (time.Since(infoStart).Nanoseconds()) / int64(iterations)

	warnStart := time.Now()
	for i := 0; i < iterations; i++ {
		RootLogger().Warn("test", zap.String("foo", "bar"))
	}
	warnNsOp := (time.Since(warnStart).Nanoseconds()) / int64(iterations)

	resetTestLogger(t)

	RootLogger().Info("log.Logger() performance",
		zap.Int64("debug (ns/op)", debugNsOp),
		zap.Int64("info (ns/op)", infoNsOp),
		zap.Int64("warn (ns/op)", warnNsOp))
}

func TestLoggerPerformance(t *testing.T) {
	_ = RootLogger() // make sure logger is initialized once
	initTestLogger(t)
	defer resetTestLogger(t)

	debugStart := time.Now()
	for i := 0; i < iterations; i++ {
		Log(K8Shim).Debug("test", zap.String("foo", "bar"))
	}
	debugNsOp := (time.Since(debugStart).Nanoseconds()) / int64(iterations)

	infoStart := time.Now()
	for i := 0; i < iterations; i++ {
		Log(K8Shim).Info("test", zap.String("foo", "bar"))
	}
	infoNsOp := (time.Since(infoStart).Nanoseconds()) / int64(iterations)

	warnStart := time.Now()
	for i := 0; i < iterations; i++ {
		Log(K8Shim).Warn("test", zap.String("foo", "bar"))
	}
	warnNsOp := (time.Since(warnStart).Nanoseconds()) / int64(iterations)

	resetTestLogger(t)

	Log(Test).Info("log.Log(...) performance (root=INFO)",
		zap.Int64("debug (ns/op)", debugNsOp),
		zap.Int64("info (ns/op)", infoNsOp),
		zap.Int64("warn (ns/op)", warnNsOp))
}

func TestLoggerPerformanceWithEnabledDebug(t *testing.T) {
	_ = RootLogger() // make sure logger is initialized once
	initTestLogger(t)
	UpdateLoggingConfig(map[string]string{
		"log.test.level": "DEBUG",
	})
	defer resetTestLogger(t)

	writtenDebugStart := time.Now()
	for i := 0; i < iterations; i++ {
		Log(Test).Debug("test", zap.String("foo", "bar"))
	}
	writtenDebugNsOp := (time.Since(writtenDebugStart).Nanoseconds()) / int64(iterations)

	debugStart := time.Now()
	for i := 0; i < iterations; i++ {
		Log(K8Shim).Debug("test", zap.String("foo", "bar"))
	}
	debugNsOp := (time.Since(debugStart).Nanoseconds()) / int64(iterations)

	infoStart := time.Now()
	for i := 0; i < iterations; i++ {
		Log(K8Shim).Info("test", zap.String("foo", "bar"))
	}
	infoNsOp := (time.Since(infoStart).Nanoseconds()) / int64(iterations)

	warnStart := time.Now()
	for i := 0; i < iterations; i++ {
		Log(K8Shim).Warn("test", zap.String("foo", "bar"))
	}
	warnNsOp := (time.Since(warnStart).Nanoseconds()) / int64(iterations)

	resetTestLogger(t)

	Log(Test).Info("log.Log(...) performance (root=DEBUG)",
		zap.Int64("debug (written) (ns/op)", writtenDebugNsOp),
		zap.Int64("debug (filtered) (ns/op)", debugNsOp),
		zap.Int64("info (ns/op)", infoNsOp),
		zap.Int64("warn (ns/op)", warnNsOp))
}

func resetTestLogger(t *testing.T) {
	// flush log
	logger.Sync() //nolint:errcheck

	// init default logger
	initLogger()

	// update logger config to defaults
	UpdateLoggingConfig(map[string]string{})

	// stat log file
	if logFile != "" {
		stat, err := os.Stat(logFile)
		assert.NilError(t, err, "log file not found")
		Log(Test).Info("Wrote logs of size", zap.Int64("size", stat.Size()))
		logFile = ""
	}
	// remove original log dir
	if logDir != "" {
		err := os.RemoveAll(logDir)
		assert.NilError(t, err, "failed to remove temp dir")
	}
}

// initTestLogger is basically the same as the default initLogger() function but uses a temporary file.
// this ensures that the logging API is actually used, while allowing us to avoid massive log spam to stdout
func initTestLogger(t *testing.T) {
	path, err := os.MkdirTemp("", "log*")
	assert.NilError(t, err, "failed to create temp dir")
	logDir = path
	logFile = fmt.Sprintf("%s/log.stdout", logDir)
	outputPaths := []string{logFile}
	fmt.Printf("Logging to %s", logFile)
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

	logger, err = zapConfigs.Build()
	assert.NilError(t, err, "failed to create logger")
	defer logger.Sync() //nolint:errcheck
}
