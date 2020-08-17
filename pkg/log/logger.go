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
	"encoding/json"
	"fmt"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/conf"
)

var once sync.Once
var logger *zap.Logger

func Logger() *zap.Logger {
	once.Do(initLogger)
	return logger
}

func initLogger() {
	configs := conf.GetSchedulerConf()

	outputPaths := []string{"stdout"}
	if configs.LogFile != "" {
		outputPaths = append(outputPaths, configs.LogFile)
	}

	zapConfigs := zap.Config{
		Level:             zap.NewAtomicLevelAt(zapcore.Level(configs.LoggingLevel)),
		Development:       false,
		DisableCaller:     false,
		DisableStacktrace: false,
		Sampling:          nil,
		Encoding:          configs.LogEncoding,
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey:    "message",
			LevelKey:      "level",
			TimeKey:       "time",
			NameKey:       "name",
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

	// set as global logging
	// when k8s-shim runs with core, core side can directly reuse this logger,
	// this way we are making consistent logging configs in shim and core.
	zap.ReplaceGlobals(logger)

	// dump configuration
	var c []byte
	c, err = json.MarshalIndent(&configs, "", " ")
	if err != nil {
		logger.Info("scheduler configuration, json conversion failed", zap.Any("configs", configs))
	} else {
		logger.Info("scheduler configuration, pretty print", zap.ByteString("configs", c))
	}

	// make sure logs are flushed
	//nolint:errcheck
	defer logger.Sync()
}

