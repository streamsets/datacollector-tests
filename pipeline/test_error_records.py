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
from streamsets.sdk.exceptions import RunError, RunningError
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

# Stage precondition: CONTAINER_0050 - The stage requires records to include the following required fields.
ERROR_CODE_STAGE_REQUIRED_FIELDS = 'CONTAINER_0050'
# Stage precondition: CONTAINER_0051 - Unsatisfied precondition.
ERROR_CODE_UNSATISFIED_PRECONDITION = 'CONTAINER_0051'


def test_error_records_stop_pipeline_on_required_field(random_expression_pipeline_builder, sdc_executor):
    random_expression_pipeline_builder.expression_evaluator.on_record_error = 'STOP_PIPELINE'
    random_expression_pipeline_builder.expression_evaluator.required_fields = ['/b']
    pipeline = random_expression_pipeline_builder.pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.dump_log_on_error = False
    with pytest.raises((RunError, RunningError)) as exception_info:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
    sdc_executor.dump_log_on_error = True

    assert (ERROR_CODE_STAGE_REQUIRED_FIELDS in exception_info.value.message)


def test_error_records_stop_pipeline_on_record_precondition(random_expression_pipeline_builder, sdc_executor):
    random_expression_pipeline_builder.expression_evaluator.on_record_error = 'STOP_PIPELINE'
    random_expression_pipeline_builder.expression_evaluator.preconditions = ['${1 == 2}']
    pipeline = random_expression_pipeline_builder.pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.dump_log_on_error = False
    with pytest.raises(RunError) as exception_info:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
    sdc_executor.dump_log_on_error = True

    assert (ERROR_CODE_UNSATISFIED_PRECONDITION in exception_info.value.message)


def test_error_records_to_error_on_required_field(random_expression_pipeline_builder, sdc_executor):
    random_expression_pipeline_builder.expression_evaluator.on_record_error = 'TO_ERROR'
    random_expression_pipeline_builder.expression_evaluator.required_fields = ['/b']
    pipeline = random_expression_pipeline_builder.pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 10)
    sdc_executor.stop_pipeline(pipeline)
    history = sdc_executor.get_pipeline_history(pipeline)
    input_record_count = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count

    # All records should go to error stream.
    assert len(random_expression_pipeline_builder.wiretap.output_records) == 0
    assert len(random_expression_pipeline_builder.wiretap.error_records) == input_record_count


def test_error_records_to_error_on_record_precondition(random_expression_pipeline_builder, sdc_executor):
    random_expression_pipeline_builder.expression_evaluator.on_record_error = 'TO_ERROR'
    random_expression_pipeline_builder.expression_evaluator.preconditions = ['${1 == 2}']
    pipeline = random_expression_pipeline_builder.pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 10)
    sdc_executor.stop_pipeline(pipeline)
    history = sdc_executor.get_pipeline_history(pipeline)
    input_record_count = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count

    # All records should go to error stream.
    assert len(random_expression_pipeline_builder.wiretap.output_records) == 0
    assert len(random_expression_pipeline_builder.wiretap.error_records) == input_record_count


def test_error_records_discard_on_required_field(random_expression_pipeline_builder, sdc_executor):
    random_expression_pipeline_builder.expression_evaluator.on_record_error = 'DISCARD'
    random_expression_pipeline_builder.expression_evaluator.required_fields = ['/b']
    pipeline = random_expression_pipeline_builder.pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 10)
    metrics = sdc_executor.get_pipeline_metrics(pipeline)
    sdc_executor.stop_pipeline(pipeline)

    # Output of the stage should be empty as all records were discarded (doesn't fit the condition).
    assert len(random_expression_pipeline_builder.wiretap.output_records) == 0
    assert len(random_expression_pipeline_builder.wiretap.error_records) == 0


def test_error_records_discard_on_record_precondition(random_expression_pipeline_builder, sdc_executor):
    random_expression_pipeline_builder.expression_evaluator.on_record_error = 'DISCARD'
    random_expression_pipeline_builder.expression_evaluator.preconditions = ['${1 == 2}']
    pipeline = random_expression_pipeline_builder.pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 10)
    metrics = sdc_executor.get_pipeline_metrics(pipeline)
    sdc_executor.stop_pipeline(pipeline)

    # Output of the stage should be empty as all records were discarded (doesn't fit the condition).
    assert len(random_expression_pipeline_builder.wiretap.output_records) == 0
    assert len(random_expression_pipeline_builder.wiretap.error_records) == 0


@pytest.fixture(scope='function')
def policy_write_builder(sdc_builder, sdc_executor):
    builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(batch_size=1,
                                      records_to_be_generated=10,
                                      delay_between_batches=1000)

    expression_evaluator = builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'changed',
                                                          'headerAttributeExpression': 'yes'}]

    to_error = builder.add_stage('To Error')

    dev_data_generator >> expression_evaluator >> to_error

    error = builder.add_error_stage('Write to File')
    error.directory = "/tmp/"

    yield builder


@pytest.fixture(scope='function')
def policy_read_builder(sdc_builder):
    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Directory', type='origin')

    origin.set_attributes(batch_wait_time_in_secs=1,
                          data_format='SDC_JSON',
                          file_name_pattern='sdc*.sdc',
                          file_post_processing='DELETE',
                          files_directory='/tmp/',
                          process_subdirectories=True)

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    yield builder, wiretap


def test_error_record_policy_original_record(policy_write_builder, policy_read_builder, sdc_executor):
    """ Validate ORIGINAL_RECORD error policy.

    The error record viewed in the wiretap pipeline should be as it was seen by the origin - without
    any changes to the record that happened inside the pipeline.

    Two pipeline setup - one pipeline with origin and mutating evaluator that sends all records
    to error stream. Second pipeline listens for incoming error records from first pipeline.
    """
    write_pipeline = policy_write_builder.build()
    wiretap_pipeline_builder, wiretap = policy_read_builder
    wiretap_pipeline = wiretap_pipeline_builder.build()

    write_pipeline.configuration['errorRecordPolicy'] = 'ORIGINAL_RECORD'
    sdc_executor.add_pipeline(write_pipeline, wiretap_pipeline)

    sdc_executor.start_pipeline(wiretap_pipeline)

    sdc_executor.start_pipeline(write_pipeline).wait_for_finished()
    sdc_executor.wait_for_pipeline_metric(wiretap_pipeline, 'input_record_count', 10)
    sdc_executor.stop_pipeline(wiretap_pipeline)

    record = wiretap.output_records[0]
    # Expecting KeyError as the header shouldn't exist
    with pytest.raises(KeyError):
        record.header['values']['changed']


def test_error_record_policy_stage_record(policy_write_builder, policy_read_builder, sdc_executor):
    """ Validate STAGE_RECORD error policy.
    The error record viewed in the wiretap pipeline should be as it was seen by the stage that sent
    it to error stream - including all changes done to that record inside the pipeline.

    Two pipeline setup - one pipeline with origin and mutating evaluator that sends all records
    to error stream. Second pipeline listens for incoming error records from first pipeline.
    """
    write_pipeline = policy_write_builder.build()
    wiretap_pipeline_builder, wiretap = policy_read_builder
    wiretap_pipeline = wiretap_pipeline_builder.build()

    write_pipeline.configuration['errorRecordPolicy'] = 'STAGE_RECORD'
    sdc_executor.add_pipeline(write_pipeline, wiretap_pipeline)

    sdc_executor.start_pipeline(wiretap_pipeline)

    sdc_executor.start_pipeline(write_pipeline).wait_for_finished()
    sdc_executor.wait_for_pipeline_metric(wiretap_pipeline, 'input_record_count', 10)
    sdc_executor.stop_pipeline(wiretap_pipeline)

    record = wiretap.output_records[0]
    assert record.header['values']['changed'] == 'yes'


@pytest.mark.parametrize('error_directory_exists', [False, True])
def test_write_to_file_error_records(sdc_builder, sdc_executor, error_directory_exists):
    """Test Write to File Error records. To achieve testing this, we have two pipelines. The 1st one will
    write required errors to a file using Error stage and 2nd will read those files using Directory origin. We then
    use wiretap on the 2nd pipeline to assert error data. The pipelines looks like:

        dev_raw_data_source >> to_error
    and
        directory >> wiretap
    """
    raw_data = 'Hello!'
    directory_to_write = tempfile.gettempdir()
    directory_to_write += "" if error_directory_exists else "/" + get_random_string()
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
    wiretap = pipeline_builder.add_wiretap()

    directory >> wiretap.destination
    directory_pipeline = pipeline_builder.build('Read error files pipeline')
    sdc_executor.add_pipeline(directory_pipeline)

    # run error stage pipeline and wait till some errors are generated
    if error_directory_exists or Version(sdc_executor.version) >= Version("5.8.0"):
        sdc_executor.start_pipeline(err_stage_pipeline)
        sdc_executor.stop_pipeline(err_stage_pipeline)

        # read from directory origin for the errors which were written
        sdc_executor.start_pipeline(directory_pipeline)
        sdc_executor.wait_for_pipeline_metric(directory_pipeline, 'input_record_count', 10)
        sdc_executor.stop_pipeline(directory_pipeline)

        # assert file content's error data has our raw_data
        record = wiretap.output_records[0].field['text'].value
        # remove special ASCII characters in the output. Note: 1st record of Error file has special ASCII character.
        record_json = json.loads(record.encode('ascii', 'ignore').decode())
        assert raw_data == record_json['value']['value']['text']['value']

    else:
        with pytest.raises(Exception) as error:
            sdc_executor.start_pipeline(err_stage_pipeline).wait_for_finished()
        assert "RECORDFS_01" in error.value.message, f'Expected a RECORDFS_01 error, got "{error.value.message}" instead'


def test_write_to_file_error_records_time_rotation(sdc_builder, sdc_executor):
    """Test Write to File Error records with time rotation. The pipeline looks like:

        source >> to_error
    """
    directory_to_write = tempfile.gettempdir()
    files_prefix = f'sdc-{get_random_string(string.ascii_letters, 6)}'
    error_record_files = directory_to_write + '/' + files_prefix + '*'
    # with below setting, there should be more than one file generated
    file_wait_time_in_secs = "1"

    # build and add error stage pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()
    write_to_file = pipeline_builder.add_error_stage('Write to File')
    write_to_file.set_attributes(directory=directory_to_write,
                                 file_wait_time_in_secs=file_wait_time_in_secs,
                                 files_prefix=files_prefix)

    source = pipeline_builder.add_stage('Dev Random Record Source')
    source.set_attributes(fields_to_generate='a,b', delay_between_batches=1100)

    to_error = pipeline_builder.add_stage('To Error')

    source >> to_error
    err_stage_pipeline = pipeline_builder.build('Write to file error records time rotation pipeline')

    try:
        sdc_executor.add_pipeline(err_stage_pipeline)

        # run error stage pipeline and wait till some errors are generated
        sdc_executor.start_pipeline(err_stage_pipeline)
        sdc_executor.wait_for_pipeline_metric(err_stage_pipeline, 'input_record_count', 4)
        sdc_executor.stop_pipeline(err_stage_pipeline)

        # assert number of files generated
        assert len(sorted(sdc_executor.execute_shell(f'ls {error_record_files}').stdout.split())) >= 2
    finally:
        if sdc_executor.get_pipeline_status(err_stage_pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(err_stage_pipeline)
        sdc_executor.remove_pipeline(err_stage_pipeline)

        # Clean up error record files created
        logger.info(f'Delete files with prefix {files_prefix} in {directory_to_write}...')
        sdc_executor.execute_shell(f'rm -f {error_record_files}')


def test_write_to_file_error_records_size_rotation(sdc_builder, sdc_executor):
    """Test Write to File Error records with size rotation. The pipeline looks like:

        source >> to_error
    """
    directory_to_write = tempfile.gettempdir()
    files_prefix = f'sdc-{get_random_string(string.ascii_letters, 6)}'
    error_record_files = directory_to_write + '/' + files_prefix + '*'
    # with below setting, there should be more than one file generated
    max_file_size_in_mb = 3

    # build and add error stage pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()
    write_to_file = pipeline_builder.add_error_stage('Write to File')
    write_to_file.set_attributes(directory=directory_to_write,
                                 max_file_size_in_mb=max_file_size_in_mb,
                                 files_prefix=files_prefix)

    source = pipeline_builder.add_stage('Dev Random Record Source')
    source.set_attributes(fields_to_generate='a,b', delay_between_batches=1100)

    to_error = pipeline_builder.add_stage('To Error')

    source >> to_error
    err_stage_pipeline = pipeline_builder.build('Write to file error records size rotation pipeline')

    try:
        sdc_executor.add_pipeline(err_stage_pipeline)

        # run error stage pipeline and wait till some errors are generated
        sdc_executor.start_pipeline(err_stage_pipeline)
        sdc_executor.wait_for_pipeline_metric(err_stage_pipeline, 'input_record_count', 5000)
        sdc_executor.stop_pipeline(err_stage_pipeline)

        # assert number of files generated
        assert len(sorted(sdc_executor.execute_shell(f'ls {error_record_files}').stdout.split())) >= 2
    finally:
        if sdc_executor.get_pipeline_status(err_stage_pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(err_stage_pipeline)
        sdc_executor.remove_pipeline(err_stage_pipeline)

        # Clean up error record files created
        logger.info(f'Delete files with prefix {files_prefix} in {directory_to_write}...')
        sdc_executor.execute_shell(f'rm -f {error_record_files}')


def test_write_to_file_error_records_time_size_rotation(sdc_builder, sdc_executor):
    """Test Write to File Error records with both time and size rotation. The pipeline looks like:

        source >> to_error
    """
    directory_to_write = tempfile.gettempdir()
    files_prefix = f'sdc-{get_random_string(string.ascii_letters, 6)}'
    error_record_files = directory_to_write + '/' + files_prefix + '*'
    # with below setting, there should be more than two file generated
    file_wait_time_in_secs = "2"
    max_file_size_in_mb = 3

    # build and add error stage pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()
    write_to_file = pipeline_builder.add_error_stage('Write to File')
    write_to_file.set_attributes(directory=directory_to_write,
                                 file_wait_time_in_secs=file_wait_time_in_secs,
                                 max_file_size_in_mb=max_file_size_in_mb,
                                 files_prefix=files_prefix)

    source = pipeline_builder.add_stage('Dev Random Record Source')
    source.set_attributes(fields_to_generate='a,b', delay_between_batches=1001)

    to_error = pipeline_builder.add_stage('To Error')

    source >> to_error
    err_stage_pipeline = pipeline_builder.build('Write to file error records time and size rotation pipeline')
    try:
        sdc_executor.add_pipeline(err_stage_pipeline)

        # run error stage pipeline and wait till some errors are generated
        sdc_executor.start_pipeline(err_stage_pipeline)
        sdc_executor.wait_for_pipeline_metric(err_stage_pipeline, 'input_record_count', 5000)
        sdc_executor.stop_pipeline(err_stage_pipeline)

        # assert number of files generated
        assert len(sorted(sdc_executor.execute_shell(f'ls {error_record_files}').stdout.split())) > 2
    finally:
        if sdc_executor.get_pipeline_status(err_stage_pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(err_stage_pipeline)
        sdc_executor.remove_pipeline(err_stage_pipeline)

        # Clean up error record files created
        logger.info(f'Delete files with prefix {files_prefix} in {directory_to_write}...')
        sdc_executor.execute_shell(f'rm -f {error_record_files}')


@sdc_min_version('3.18.0')
def test_error_records_with_job_info(random_expression_pipeline_builder, sdc_executor):
    random_expression_pipeline_builder.expression_evaluator.on_record_error = 'TO_ERROR'
    random_expression_pipeline_builder.expression_evaluator.required_fields = ['/b']
    pipeline = random_expression_pipeline_builder.pipeline_builder.build()
    pipeline.add_parameters(JOB_ID='stfJobId', JOB_NAME='stfJobName')
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 10)
    sdc_executor.stop_pipeline(pipeline)
    history = sdc_executor.get_pipeline_history(pipeline)
    input_record_count = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count

    # All records should go to error stream.
    assert len(random_expression_pipeline_builder.wiretap.output_records) == 0
    assert len(random_expression_pipeline_builder.wiretap.error_records) == input_record_count
    for error_record in random_expression_pipeline_builder.wiretap.error_records:
        assert error_record.header['errorJobId'] == 'stfJobId'
        assert error_record.header['errorJobName'] == 'stfJobName'
