# Copyright 2017 StreamSets Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import logging
import string
import tempfile

import pytest
from streamsets.sdk import sdc_api
from streamsets.testframework.markers import sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

# Stage precondition: CONTAINER_0050 - The stage requires records to include the following required fields.
ERROR_CODE_STAGE_REQUIRED_FIELDS = 'CONTAINER_0050'
# Stage precondition: CONTAINER_0051 - Unsatisfied precondition.
ERROR_CODE_UNSATISFIED_PRECONDITION = 'CONTAINER_0051'
# Port for SDC RPC stages to exchange error records
SDC_RPC_LISTENING_PORT = 20000


def test_error_records_stop_pipeline_on_required_field(random_expression_pipeline_builder, sdc_executor):
    random_expression_pipeline_builder.expression_evaluator.on_record_error = 'STOP_PIPELINE'
    random_expression_pipeline_builder.expression_evaluator.required_fields = ['/b']
    pipeline = random_expression_pipeline_builder.pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.dump_log_on_error = False
    with pytest.raises(sdc_api.RunError) as exception_info:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
    sdc_executor.dump_log_on_error = True

    assert(ERROR_CODE_STAGE_REQUIRED_FIELDS in exception_info.value.message)


def test_error_records_stop_pipeline_on_record_precondition(random_expression_pipeline_builder, sdc_executor):
    random_expression_pipeline_builder.expression_evaluator.on_record_error = 'STOP_PIPELINE'
    random_expression_pipeline_builder.expression_evaluator.preconditions = ['${1 == 2}']
    pipeline = random_expression_pipeline_builder.pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.dump_log_on_error = False
    with pytest.raises(sdc_api.RunError) as exception_info:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
    sdc_executor.dump_log_on_error = True

    assert(ERROR_CODE_UNSATISFIED_PRECONDITION in exception_info.value.message)


def test_error_records_to_error_on_required_field(random_expression_pipeline_builder, sdc_executor):
    random_expression_pipeline_builder.expression_evaluator.on_record_error = 'TO_ERROR'
    random_expression_pipeline_builder.expression_evaluator.required_fields = ['/b']
    pipeline = random_expression_pipeline_builder.pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)

    # All records should go to error stream.
    input_records = snapshot[random_expression_pipeline_builder.dev_data_generator.instance_name].output
    stage = snapshot[random_expression_pipeline_builder.expression_evaluator.instance_name]
    assert len(stage.output) == 0
    assert len(stage.error_records) == len(input_records)


def test_error_records_to_error_on_record_precondition(random_expression_pipeline_builder, sdc_executor):
    random_expression_pipeline_builder.expression_evaluator.on_record_error = 'TO_ERROR'
    random_expression_pipeline_builder.expression_evaluator.preconditions = ['${1 == 2}']
    pipeline = random_expression_pipeline_builder.pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)

    # All records should go to error stream.
    input_records = snapshot[random_expression_pipeline_builder.dev_data_generator.instance_name].output
    stage = snapshot[random_expression_pipeline_builder.expression_evaluator.instance_name]
    assert len(stage.output) == 0
    assert len(stage.error_records) == len(input_records)


def test_error_records_discard_on_required_field(random_expression_pipeline_builder, sdc_executor):
    random_expression_pipeline_builder.expression_evaluator.on_record_error = 'DISCARD'
    random_expression_pipeline_builder.expression_evaluator.required_fields = ['/b']
    pipeline = random_expression_pipeline_builder.pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)

    # Output of the stage should be empty as all records were discarded (doesn't fit the condition).
    stage = snapshot[random_expression_pipeline_builder.expression_evaluator.instance_name]
    assert len(stage.output) == 0
    assert len(stage.error_records) == 0


def test_error_records_discard_on_record_precondition(random_expression_pipeline_builder, sdc_executor):
    random_expression_pipeline_builder.expression_evaluator.on_record_error = 'DISCARD'
    random_expression_pipeline_builder.expression_evaluator.preconditions = ['${1 == 2}']
    pipeline = random_expression_pipeline_builder.pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)

    # Output of the stage should be empty as all records were discarded (doesn't fit the condition).
    stage = snapshot[random_expression_pipeline_builder.expression_evaluator.instance_name]
    assert len(stage.output) == 0
    assert len(stage.error_records) == 0


@pytest.fixture(scope='function')
def policy_write_builder(sdc_builder, sdc_executor):
    builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = builder.add_stage('Dev Data Generator')

    expression_evaluator = builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'changed',
                                                          'headerAttributeExpression': 'yes'}]

    to_error = builder.add_stage('To Error')

    dev_data_generator >> expression_evaluator >> to_error

    error = builder.add_error_stage('Write to Another Pipeline')
    error.sdc_rpc_connection = ['{}:{}'.format(sdc_executor.server_host, SDC_RPC_LISTENING_PORT)]
    error.sdc_rpc_id = 'error_policy'

    yield builder


@pytest.fixture(scope='function')
def policy_read_builder(sdc_builder):
    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('SDC RPC', type='origin')
    origin.sdc_rpc_listening_port = SDC_RPC_LISTENING_PORT
    origin.sdc_rpc_id = 'error_policy'

    trash = builder.add_stage('Trash')

    origin >> trash

    yield builder


def test_error_record_policy_original_record(policy_write_builder, policy_read_builder, sdc_executor):
    """ Validate ORIGINAL_RECORD error policy.

    The error record viewed in the snapshot pipeline should be as it was seen by the origin - without
    any changes to the record that happened inside the pipeline.

    Two pipeline setup - one pipeline with origin and mutating evaluator that sends all records
    to error stream. Second pipeline listens for incoming error records from first pipeline.
    """
    write_pipeline = policy_write_builder.build()
    snapshot_pipeline = policy_read_builder.build()

    write_pipeline.configuration['errorRecordPolicy'] = 'ORIGINAL_RECORD'
    sdc_executor.add_pipeline(write_pipeline, snapshot_pipeline)

    try:
        sdc_executor.start_pipeline(snapshot_pipeline)
        snapshot_command = sdc_executor.capture_snapshot(snapshot_pipeline, wait=False)
        sdc_executor.start_pipeline(write_pipeline)

        snapshot = snapshot_command.wait_for_finished().snapshot
        record = snapshot[snapshot_pipeline.origin_stage].output[0]

        # Expecting KeyError as the header shouldn't exist
        with pytest.raises(KeyError):
            record.header['values']['changed']

    finally:
        sdc_executor.stop_pipeline(write_pipeline)
        sdc_executor.stop_pipeline(snapshot_pipeline)


def test_error_record_policy_stage_record(policy_write_builder, policy_read_builder, sdc_executor):
    """ Validate STAGE_RECORD error policy.

    The error record viewed in the snapshot pipeline should be as it was seen by the stage that sent
    it to error stream - including all changes done to that record inside the pipeline.

    Two pipeline setup - one pipeline with origin and mutating evaluator that sends all records
    to error stream. Second pipeline listens for incoming error records from first pipeline.
    """
    write_pipeline = policy_write_builder.build()
    snapshot_pipeline = policy_read_builder.build()

    write_pipeline.configuration['errorRecordPolicy'] = 'STAGE_RECORD'
    sdc_executor.add_pipeline(write_pipeline, snapshot_pipeline)

    try:
        sdc_executor.start_pipeline(snapshot_pipeline)
        snapshot_command = sdc_executor.capture_snapshot(snapshot_pipeline, wait=False)
        sdc_executor.start_pipeline(write_pipeline)

        snapshot = snapshot_command.wait_for_finished().snapshot
        record = snapshot[snapshot_pipeline.origin_stage].output[0]

        assert record.header['values']['changed'] == 'yes'

    finally:
        sdc_executor.stop_pipeline(write_pipeline)
        sdc_executor.stop_pipeline(snapshot_pipeline)


def test_write_to_file_error_records(sdc_builder, sdc_executor):
    """Test Write to File Error records. To achieve testing this, we have two pipelines. The 1st one will
    write required errors to a file using Error stage and 2nd will read those files using Directory origin. We then
    snapshot the 2nd pipeline to assert error data. The pipelines looks like:

        dev_raw_data_source >> to_error
    and
        directory >> trash
    """
    raw_data = 'Hello!'
    directory_to_write = tempfile.gettempdir()
    files_prefix = 'sdc-{}'.format(get_random_string(string.ascii_letters, 6))
    # with below setting, there should only be one file generated
    file_wait_time_in_secs = "300"
    max_file_size_in_mb = 100

    # build and add error stage pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()
    write_to_file = pipeline_builder.add_error_stage('Write to File')
    write_to_file.set_attributes(directory=directory_to_write, file_wait_time_in_secs=file_wait_time_in_secs,
                                 files_prefix=files_prefix, max_file_size_in_mb=max_file_size_in_mb)
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data)
    to_error = pipeline_builder.add_stage('To Error')

    dev_raw_data_source >> to_error
    err_stage_pipeline = pipeline_builder.build('Write to file error stage pipeline')
    sdc_executor.add_pipeline(err_stage_pipeline)

    # build and add directory read pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='TEXT', files_directory=directory_to_write,
                             file_name_pattern=f'{files_prefix}*')
    trash = pipeline_builder.add_stage('Trash')

    directory >> trash
    directory_pipeline = pipeline_builder.build('Read error files pipeline')
    sdc_executor.add_pipeline(directory_pipeline)

    # run error stage pipeline and wait till some errors are generated
    sdc_executor.start_pipeline(err_stage_pipeline)
    sdc_executor.stop_pipeline(err_stage_pipeline)

    # read from directory origin for the errors which were written
    snapshot = sdc_executor.capture_snapshot(directory_pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(directory_pipeline)

    # assert file content's error data has our raw_data
    record = snapshot[directory.instance_name].output[0].field['text'].value
    # remove special ASCII characters in the output. Note: 1st record of Error file has special ASCII character.
    record_json = json.loads(record.encode('ascii', 'ignore').decode())
    assert raw_data == record_json['value']['value']['text']['value']


@sdc_min_version('3.18.0')
def test_error_records_with_job_info(random_expression_pipeline_builder, sdc_executor):
    random_expression_pipeline_builder.expression_evaluator.on_record_error = 'TO_ERROR'
    random_expression_pipeline_builder.expression_evaluator.required_fields = ['/b']
    pipeline = random_expression_pipeline_builder.pipeline_builder.build()
    pipeline.add_parameters(JOB_ID='stfJobId', JOB_NAME='stfJobName')
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)

    # All records should go to error stream.
    input_records = snapshot[random_expression_pipeline_builder.dev_data_generator.instance_name].output
    stage = snapshot[random_expression_pipeline_builder.expression_evaluator.instance_name]
    assert len(stage.output) == 0
    assert len(stage.error_records) == len(input_records)
    for error_record in stage.error_records:
        assert 'stfJobId' == error_record.header['errorJobId']
        assert 'stfJobName' == error_record.header['errorJobName']
