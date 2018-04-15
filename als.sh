#!/usr/bin/env bash
#
# Copyright (C) 2017 Seoul National University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
./bin/run.sh \
	-job_id als \
	-executor_json `pwd`/examples/resources/sample_executor_resources.json \
 	-user_main edu.snu.nemo.examples.beam.AlternatingLeastSquare \
 	-optimization_policy edu.snu.nemo.compiler.optimizer.policy.PadoPolicy \
 	-dag_dir "./dag/als" \
 	-user_args "`pwd`/examples/resources/sample_input_als 10 3"
