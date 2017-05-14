# Copyright 2017 StreamSets Inc.
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

import pytest

from testframework import sdc_api

logger = logging.getLogger(__name__)

# Stage precondition: CONTAINER_0050 - The stage requires records to include the following required fields.
ERROR_CODE_STAGE_REQUIRED_FIELDS = 'CONTAINER_0050'
# Stage precondition: CONTAINER_0051 - Unsatisfied precondition.
ERROR_CODE_UNSATISFIED_PRECONDITION = 'CONTAINER_0051'

def test_error_records_stop_pipeline_on_required_field(random_expression_pipeline_builder, sdc_executor):
    random_expression_pipeline_builder.expression_evaluator.stage_on_record_error = 'STOP_PIPELINE'
    random_expression_pipeline_builder.expression_evaluator.stage_required_fields = ['/b']
    pipeline = random_expression_pipeline_builder.pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    with pytest.raises(sdc_api.RunError) as exception_info:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert(ERROR_CODE_STAGE_REQUIRED_FIELDS in exception_info.value.message)


def test_error_records_stop_pipeline_on_record_precondition(random_expression_pipeline_builder, sdc_executor):
    random_expression_pipeline_builder.expression_evaluator.stage_on_record_error = 'STOP_PIPELINE'
    random_expression_pipeline_builder.expression_evaluator.stage_record_preconditions = ['${1 == 2}']
    pipeline = random_expression_pipeline_builder.pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    with pytest.raises(sdc_api.RunError) as exception_info:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
    assert(ERROR_CODE_UNSATISFIED_PRECONDITION in exception_info.value.message)


def test_error_records_to_error_on_required_field(random_expression_pipeline_builder, sdc_executor):
    random_expression_pipeline_builder.expression_evaluator.stage_on_record_error = 'TO_ERROR'
    random_expression_pipeline_builder.expression_evaluator.stage_required_fields = ['/b']
    pipeline = random_expression_pipeline_builder.pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
    sdc_executor.stop_pipeline(pipeline)

    # All records should go to error stream.
    input_records = snapshot[random_expression_pipeline_builder.dev_data_generator.instance_name].output
    stage = snapshot[random_expression_pipeline_builder.expression_evaluator.instance_name]
    assert len(stage.output) == 0
    assert len(stage.error_records) == len(input_records)


def test_error_records_to_error_on_record_precondition(random_expression_pipeline_builder, sdc_executor):
    random_expression_pipeline_builder.expression_evaluator.stage_on_record_error = 'TO_ERROR'
    random_expression_pipeline_builder.expression_evaluator.stage_record_preconditions = ['${1 == 2}']
    pipeline = random_expression_pipeline_builder.pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
    sdc_executor.stop_pipeline(pipeline)

    # All records should go to error stream.
    input_records = snapshot[random_expression_pipeline_builder.dev_data_generator.instance_name].output
    stage = snapshot[random_expression_pipeline_builder.expression_evaluator.instance_name]
    assert len(stage.output) == 0
    assert len(stage.error_records) == len(input_records)


def test_error_records_discard_on_required_field(random_expression_pipeline_builder, sdc_executor):
    random_expression_pipeline_builder.expression_evaluator.stage_on_record_error = 'DISCARD'
    random_expression_pipeline_builder.expression_evaluator.stage_required_fields = ['/b']
    pipeline = random_expression_pipeline_builder.pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
    sdc_executor.stop_pipeline(pipeline)

    # Output of the stage should be empty as all records were discarded (doesn't fit the condition).
    stage = snapshot[random_expression_pipeline_builder.expression_evaluator.instance_name]
    assert len(stage.output) == 0
    assert len(stage.error_records) == 0


def test_error_records_discard_on_record_precondition(random_expression_pipeline_builder, sdc_executor):
    random_expression_pipeline_builder.expression_evaluator.stage_on_record_error = 'DISCARD'
    random_expression_pipeline_builder.expression_evaluator.stage_record_preconditions = ['${1 == 2}']
    pipeline = random_expression_pipeline_builder.pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
    sdc_executor.stop_pipeline(pipeline)

    # Output of the stage should be empty as all records were discarded (doesn't fit the condition).
    stage = snapshot[random_expression_pipeline_builder.expression_evaluator.instance_name]
    assert len(stage.output) == 0
    assert len(stage.error_records) == 0
