/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

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
	"github.com/cloudera/yunikorn-k8shim/pkg/conf"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"strings"
)

var Logger *zap.Logger

func init() {

	configs := conf.GetSchedulerConf()

	var outputPaths []string
	if strings.Compare(configs.LogFile, "") == 0 {
		outputPaths = []string {"stdout"}
	} else {
		outputPaths = []string {"stdout", configs.LogFile}
	}

	zapConfigs := zap.Config{
		Level:             zap.NewAtomicLevelAt(zapcore.Level(configs.LoggingLevel)),
		Development:       false,
		DisableCaller:     false,
		DisableStacktrace: false,
		Sampling:          nil,
		Encoding:          configs.LogEncoding,
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey:     "message",
			LevelKey:       "level",
			TimeKey:        "time",
			NameKey:        "name",
			CallerKey:      "caller",
			StacktraceKey:  "stacktrace",
			EncodeLevel:    zapcore.CapitalLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: nil,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
		OutputPaths:      outputPaths,
		ErrorOutputPaths: []string{"stderr"},
	}

	if logger, err := zapConfigs.Build(); err == nil {
		Logger = logger
		// zap.ReplaceGlobals(Logger)
	} else {
		panic(fmt.Sprintf("failed to init logger, reason: %s", err.Error()))
	}

	// set as global logging
	// when k8s-shim runs with core, core side can directly reuse this logger,
	// this way we are making consistent logging configs in shim and core.
	zap.ReplaceGlobals(Logger)

	// dump configuration
	c, _ := json.MarshalIndent(&configs, "", " ")
	Logger.Info("scheduler configuration", zap.String("configs", string(c)))

	// make sure logs are flushed
	defer Logger.Sync()
}