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
)

var logDir string
var logFile string

var iterations = 100000

func BenchmarkLegacyLoggerDebug(b *testing.B) {
	benchmarkLegacyLoggerDebug(b.N)
}

func TestLegacyLoggerDebug(t *testing.T) {
	nsOp := benchmarkLegacyLoggerDebug(iterations)
	RootLogger().Info("log.Logger() performance", zap.Int64("debug (ns/op)", nsOp))
}

func benchmarkLegacyLoggerDebug(iterations int) int64 {
	_ = Logger()
	initTestLogger()
	defer resetTestLogger()
	start := time.Now()
	for i := 0; i < iterations; i++ {
		RootLogger().Debug("test", zap.String("foo", "bar"))
	}
	return (time.Since(start).Nanoseconds()) / int64(iterations)
}

func BenchmarkLegacyLoggerInfo(b *testing.B) {
	benchmarkLegacyLoggerInfo(b.N)
}

func TestLegacyLoggerInfo(t *testing.T) {
	nsOp := benchmarkLegacyLoggerInfo(iterations)
	RootLogger().Info("log.Logger() performance", zap.Int64("info (ns/op)", nsOp))
}

func benchmarkLegacyLoggerInfo(iterations int) int64 {
	_ = Logger()
	initTestLogger()
	defer resetTestLogger()
	start := time.Now()
	for i := 0; i < iterations; i++ {
		RootLogger().Info("test", zap.String("foo", "bar"))
	}
	return (time.Since(start).Nanoseconds()) / int64(iterations)
}

func BenchmarkScopedLoggerDebug(b *testing.B) {
	benchmarkScopedLoggerDebug(b.N)
}

func TestScopedLoggerDebug(t *testing.T) {
	nsOp := benchmarkScopedLoggerDebug(iterations)
	Log(Test).Info("log.Log(...) performance (root=INFO)", zap.Int64("debug (ns/op)", nsOp))
}

func benchmarkScopedLoggerDebug(iterations int) int64 {
	_ = Logger()
	initTestLogger()
	defer resetTestLogger()
	UpdateLoggingConfig(map[string]string{
		"log.level": "INFO",
	})
	start := time.Now()
	for i := 0; i < iterations; i++ {
		Log(K8Shim).Debug("test", zap.String("foo", "bar"))
	}
	return (time.Since(start).Nanoseconds()) / int64(iterations)
}

func BenchmarkScopedLoggerInfo(b *testing.B) {
	benchmarkScopedLoggerInfo(b.N)
}

func TestScopedLoggerInfo(t *testing.T) {
	nsOp := benchmarkScopedLoggerInfo(iterations)
	Log(Test).Info("log.Log(...) performance (root=INFO)", zap.Int64("info (ns/op)", nsOp))
}

func benchmarkScopedLoggerInfo(iterations int) int64 {
	_ = Logger()
	initTestLogger()
	defer resetTestLogger()
	UpdateLoggingConfig(map[string]string{
		"log.level": "INFO",
	})
	start := time.Now()
	for i := 0; i < iterations; i++ {
		Log(K8Shim).Info("test", zap.String("foo", "bar"))
	}
	return (time.Since(start).Nanoseconds()) / int64(iterations)
}

func BenchmarkScopedLoggerDebugEnabled(b *testing.B) {
	benchmarkScopedLoggerDebugEnabled(b.N)
}

func TestScopedLoggerDebugEnabled(t *testing.T) {
	nsOp := benchmarkScopedLoggerDebugEnabled(iterations)
	Log(Test).Info("log.Log(...) performance (root=DEBUG)", zap.Int64("debug (ns/op)", nsOp))
}

func benchmarkScopedLoggerDebugEnabled(iterations int) int64 {
	_ = Logger()
	initTestLogger()
	defer resetTestLogger()
	UpdateLoggingConfig(map[string]string{
		"log.test.level": "DEBUG",
	})
	start := time.Now()
	for i := 0; i < iterations; i++ {
		Log(Test).Debug("test", zap.String("foo", "bar"))
	}
	return (time.Since(start).Nanoseconds()) / int64(iterations)
}

func BenchmarkScopedLoggerInfoFiltered(b *testing.B) {
	benchmarkScopedLoggerInfoFiltered(b.N)
}

func TestScopedLoggerInfoFiltered(t *testing.T) {
	nsOp := benchmarkScopedLoggerInfoFiltered(iterations)
	Log(Test).Info("log.Log(...) performance (root=DEBUG)", zap.Int64("info (ns/op)", nsOp))
}

func benchmarkScopedLoggerInfoFiltered(iterations int) int64 {
	_ = Logger()
	initTestLogger()
	defer resetTestLogger()
	UpdateLoggingConfig(map[string]string{
		"log.test.level": "DEBUG",
	})
	start := time.Now()
	for i := 0; i < iterations; i++ {
		Log(K8Shim).Info("test", zap.String("foo", "bar"))
	}
	return (time.Since(start).Nanoseconds()) / int64(iterations)
}

func resetTestLogger() {
	// flush log
	logger.Sync() //nolint:errcheck

	// init default logger
	initLogger()

	// update logger config to defaults
	UpdateLoggingConfig(map[string]string{})

	if logFile != "" {
		logFile = ""
	}
	if logDir != "" {
		if err := os.RemoveAll(logDir); err != nil {
			fmt.Printf("Error removing log dir: %s", err.Error())
		}
	}
}

// initTestLogger is basically the same as the default initLogger() function but uses a temporary file.
// this ensures that the logging API is actually used, while allowing us to avoid massive log spam to stdout
func initTestLogger() {
	path, err := os.MkdirTemp("", "log*")
	if err != nil {
		panic(err)
	}
	logDir = path
	logFile = fmt.Sprintf("%s/log.stdout", logDir)
	outputPaths := []string{logFile}
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
	if err != nil {
		panic(err)
	}
	defer logger.Sync()
}
