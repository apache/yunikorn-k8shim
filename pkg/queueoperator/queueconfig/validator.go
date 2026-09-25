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

package queueconfig

import (
	"errors"
	"fmt"

	ykconfigs "github.com/apache/yunikorn-core/pkg/common/configs"
)

// ErrInvalidConfig is returned when the merged scheduler config fails to parse
// or validate against YuniKorn's own rules. Callers should inspect the wrapped
// error for the precise rule that failed.
var ErrInvalidConfig = errors.New("yunikorn scheduler config is invalid")

// ValidationError wraps a YuniKorn validation failure with the YAML payload
// that triggered it. The YAML is included to make debugging admission webhook
// rejections trivial — the operator/user can see exactly what was about to be
// shipped to the scheduler.
type ValidationError struct {
	// Err is the underlying error returned by yunikorn-core's validator
	// (or its YAML decoder, in which case the config never even parsed).
	Err error
	// YAML is the rendered scheduler-config YAML that failed validation.
	// May be empty if the failure occurred before YAML rendering.
	YAML string
	// Stage indicates which step failed: "render", "parse", or "validate".
	// "render"  : we could not marshal the SchedulerConfig to YAML.
	// "parse"   : YuniKorn rejected the YAML during decoding (strict-field).
	// "validate": YuniKorn parsed the YAML but its semantic rules failed.
	Stage string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("scheduler config %s failed: %v", e.Stage, e.Err)
}

func (e *ValidationError) Unwrap() error { return e.Err }

// Is reports whether target is ErrInvalidConfig. This lets callers use
// errors.Is(err, queueconfig.ErrInvalidConfig) without caring about the stage.
func (e *ValidationError) Is(target error) bool { return target == ErrInvalidConfig }

// Validate runs the merged SchedulerConfig through YuniKorn's own validator
// (configs.LoadSchedulerConfigFromByteArray), which is the exact code path the
// scheduler executes at startup and on hot-refresh.
//
// On success it returns the rendered YAML (so callers don't need to marshal
// again) and the parsed YuniKorn SchedulerConfig.
//
// On failure it returns a *ValidationError whose Stage tells you whether the
// failure was YAML rendering, YAML parsing (e.g. unknown field), or a semantic
// rule (e.g. duplicate queue name, child resources exceeding parent).
func Validate(cfg *SchedulerConfig) ([]byte, *ykconfigs.SchedulerConfig, error) {
	yamlBytes, err := MarshalYAML(cfg)
	if err != nil {
		return nil, nil, &ValidationError{Err: err, Stage: "render"}
	}

	parsed, err := ykconfigs.LoadSchedulerConfigFromByteArray(yamlBytes)
	if err != nil {
		// LoadSchedulerConfigFromByteArray bundles parse + validate.
		// We don't have a clean way to distinguish parse vs validate from the
		// returned error alone — yunikorn returns yaml.TypeError for parse
		// failures and a fmt.Errorf for semantic failures. Sniff by type.
		stage := "validate"
		if isYAMLParseError(err) {
			stage = "parse"
		}
		return yamlBytes, nil, &ValidationError{Err: err, Stage: stage, YAML: string(yamlBytes)}
	}

	return yamlBytes, parsed, nil
}

// ValidateBytes validates an already-rendered YAML payload. Useful for
// reconciler "defense in depth": if some other code path produced YAML, we
// still want to gate ConfigMap writes through YuniKorn's validator.
func ValidateBytes(yamlBytes []byte) (*ykconfigs.SchedulerConfig, error) {
	parsed, err := ykconfigs.LoadSchedulerConfigFromByteArray(yamlBytes)
	if err != nil {
		stage := "validate"
		if isYAMLParseError(err) {
			stage = "parse"
		}
		return nil, &ValidationError{Err: err, Stage: stage, YAML: string(yamlBytes)}
	}
	return parsed, nil
}

// isYAMLParseError reports whether err looks like a YAML decoding failure.
//
// yunikorn-core returns the raw decoder error from go.yaml.in/yaml/v3, whose
// strict-field mode produces *yaml.TypeError or errors that mention "yaml:".
// We use a string-prefix check to stay type-version-agnostic — the alternative
// (importing yaml/v3 just to errors.As() a *yaml.TypeError) would couple us to
// a specific YAML library version.
func isYAMLParseError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if len(msg) >= 5 && msg[:5] == "yaml:" {
		return true
	}
	// yaml.TypeError wraps multi-line errors; the first non-prefixed form
	// "line N: ..." is also produced by the decoder.
	return false
}
