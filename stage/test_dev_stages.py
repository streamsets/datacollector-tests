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

import logging
from time import sleep

import pytest
from streamsets.sdk import sdc_api
from streamsets.sdk.exceptions import ValidationError
from streamsets.testframework.markers import aster_authentication, rpmpackaging, sdc_min_version
from streamsets.testframework.utils import Version

logger = logging.getLogger(__name__)

pytestmark = [aster_authentication, rpmpackaging]


@pytest.fixture(scope='module')
def pipeline(sdc_builder, sdc_executor):
    """Create pipeline for the tests. """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = '{"emp_id" :"123456"}'

    trash = pipeline_builder.add_stage('Trash')

    # Wire up the stages.
    dev_raw_data_source >> trash

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)
    yield pipeline

@pytest.fixture(scope='module')
def pipeline_with_events(sdc_builder, sdc_executor):
    """Create pipeline for the tests. """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    generator = pipeline_builder.add_stage(label='Dev Data Generator')
    trash = pipeline_builder.add_stage('Trash')
    event_trash = pipeline_builder.add_stage('Trash')

    # Wire up the stages.
    generator >> trash
    generator >= event_trash

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)
    yield pipeline


def test_pipeline_status(sdc_executor, pipeline):
    """For a running and a stopped pipeline,
       confirm that status returns appropriate values in both cases."""
    sdc_executor.start_pipeline(pipeline)

    # Verify running pipeline's status
    current_status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
    assert current_status == 'RUNNING'

    # Stop the pipeline and verify pipeline's status
    sdc_executor.stop_pipeline(pipeline)
    current_status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
    assert current_status == 'STOPPED'


def test_pipeline_definitions(sdc_executor, pipeline):
    """For a running pipeline, confirm that definitions returns some values.
       Stop the pipeline and confirm that definitions return same values as before."""
    sdc_executor.start_pipeline(pipeline)

    running_pipeline_definitions = sdc_executor.api_client.get_definitions()
    assert running_pipeline_definitions is not None

    # Stop the pipeline and verify definitions do not change
    sdc_executor.stop_pipeline(pipeline)
    stopped_pipeline_definitions = sdc_executor.api_client.get_definitions()
    assert stopped_pipeline_definitions is not None
    assert running_pipeline_definitions == stopped_pipeline_definitions


def test_pipeline_metrics(sdc_executor, pipeline):
    """For a running pipeline, confirm that metrics endpoint returns some values,
       which change after some time when again metrics are received,
       Stop the pipeline and confirm that metrics endpoint return empty."""
    sdc_executor.start_pipeline(pipeline)
    metrics_1 = sdc_executor.get_pipeline_metrics(pipeline)
    sleep(15)
    metrics_2 = sdc_executor.get_pipeline_metrics(pipeline)
    assert metrics_1 and metrics_2 and metrics_1 != metrics_2

    sdc_executor.stop_pipeline(pipeline)
    assert not sdc_executor.get_pipeline_metrics(pipeline)


def test_pipeline_snapshot(sdc_executor, pipeline):
    """For a running pipeline, confirm that snapshot returns expected values."""
    sdc_executor.start_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline).snapshot
    assert snapshot is not None
    snap_data = snapshot[pipeline.origin_stage.instance_name]
    assert len(snap_data.output) == 1
    assert snap_data.output[0].field['emp_id'].value == '123456'

    sdc_executor.stop_pipeline(pipeline)


def test_pipeline_preview(sdc_executor, pipeline):
    """Run preview and confirm that preview returns expected values
       and no issues are reported."""
    preview = sdc_executor.run_pipeline_preview(pipeline).preview
    assert preview is not None
    assert preview.issues.issues_count == 0
    preview_data = preview[pipeline.origin_stage.instance_name]
    assert len(preview_data.output) == 1
    assert preview_data.output[0].field['emp_id'].value == '123456'


@sdc_min_version('3.16.0')
def test_delimited_data(sdc_executor, sdc_builder):
    """Test delimited data format with 2 records, the first one containing an extra, unexpected column.
    We verify that the first one is sent to error and the second one is processed correctly so we have recovered
    properly from the ParserException

    The pipeline looks like:
    dev_raw_data_source >> trash
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    data = 'Name,Position\nAlex,Developer,1\nXavi,Developer'
    expected = {'Name': 'Xavi', 'Position': 'Developer'}
    expected_error = {'columns': ['Alex', 'Developer', '1'], 'headers': ['Name', 'Position']}

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data=data,
                                       stop_after_first_batch=True)
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> wiretap.destination

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 1
    assert len(wiretap.error_records) == 1

    assert [record.field for record in wiretap.output_records] == [expected]
    assert [record.field for record in wiretap.error_records] == [expected_error]


@sdc_min_version('5.3.0')
def test_field_attributes(sdc_executor, sdc_builder):
    """Test Dev Data Generator with and without generation of field attributes

    The pipeline looks like:
    dev_raw_data_source >> wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    expected_fattrs_dict = {'fattr1': 'fvalue1', 'fattr2': 'fvalue2'}
    expected_fattrs_list = [{'key': key, 'value': value} for key, value in expected_fattrs_dict.items()]

    dev_raw_data_source = pipeline_builder.add_stage('Dev Data Generator')
    dev_raw_data_source.set_attributes(records_to_be_generated=1,
                                       root_field_type='MAP',
                                       fields_to_generate=[
                                           {'field': 'f1', 'type': 'LONG'},
                                           {'field': 'f2', 'type': 'LONG', 'fieldAttributes': expected_fattrs_list},
                                           {'field': 'f3', 'type': 'LONG', 'fieldAttributes': []},
                                           {'field': 'f4', 'type': 'STRING', 'fieldAttributes': expected_fattrs_list}])
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> wiretap.destination

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 1
    assert len(wiretap.error_records) == 0
    for record in wiretap.output_records:
        assert record is not None
        assert record.get_field_attributes('/f1') is None
        assert record.get_field_attributes('/f2') == expected_fattrs_dict
        assert record.get_field_attributes('/f3') is None
        assert record.get_field_attributes('/f4') == expected_fattrs_dict
        assert type(record.get_field_data('/f1').value) == int
        assert type(record.get_field_data('/f2').value) == int
        assert type(record.get_field_data('/f3').value) == int
        assert type(record.get_field_data('/f4').value) == str


@sdc_min_version('3.5.1')
def test_validate(sdc_executor, pipeline_with_events):
    """Validate pipeline with events on origin side."""
    sdc_executor.validate_pipeline(pipeline_with_events)


def test_invalid_execution_mode(sdc_executor, pipeline):
    """Set executionMode to invalid value for a pipeline,
       try starting it and confirm that it raises expected exception."""
    pipeline.configuration['executionMode'] = 'Invalid_Execution_Mode'
    pipeline.id = 'Invalid_Execution_Mode Pipeline'

    try:
        sdc_executor.add_pipeline(pipeline)
        # Do a version check since execution_mode handling changed starting in the 2.7.0.0 version.
        if Version(sdc_executor.version) >= Version('2.7.0.0'):
            with pytest.raises(ValidationError):
                sdc_executor.dump_log_on_error = False
                sdc_executor.start_pipeline(pipeline)
        else:
            with pytest.raises(sdc_api.StartError):
                sdc_executor.dump_log_on_error = False
                sdc_executor.start_pipeline(pipeline)
    finally:
        sdc_executor.dump_log_on_error = True
