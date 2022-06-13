# Copyright 2020 StreamSets Inc.
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

import pytest
from streamsets.sdk import sdc_api
from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import sdc_min_version
from streamsets.sdk.utils import Version
from time import sleep


@pytest.mark.parametrize('stage_attributes', [{'action': 'KEEP'},
                                              {'action': 'REMOVE'},
                                              {'action': 'REMOVE_CONSTANT'},
                                              {'action': 'REMOVE_EMPTY'},
                                              {'action': 'REMOVE_NULL'},
                                              {'action': 'REMOVE_NULL_EMPTY'}])
def test_action(sdc_builder, sdc_executor, stage_attributes):
    if stage_attributes['action'] == 'REMOVE_CONSTANT' and Version(sdc_builder.version) < Version('3.7.0'):
        pytest.skip('REMOVE_CONSTANT have been added only in 3.7.0')

    DATA = dict(name='Al Gore', birthplace='Washington, D.C.', winningYears=None, internetPatents='')

    # We'll keep the /name field.
    EXPECTED_KEEP_DATA = dict(name='Al Gore')
    # We'll remove the /name field.
    EXPECTED_REMOVE_DATA = dict(birthplace='Washington, D.C.',
                                winningYears=None,
                                internetPatents='')
    # We'll ask to remove all fields but set constant to his name.
    EXPECTED_REMOVE_CONSTANT_DATA = dict(birthplace='Washington, D.C.',
                                         winningYears=None,
                                         internetPatents='')
    # We'll ask to remove all fields, but only the ones that have empty string values (/internetPatents) will.
    EXPECTED_REMOVE_EMPTY_DATA = dict(name='Al Gore',
                                      birthplace='Washington, D.C.',
                                      winningYears=None)
    # We'll ask to remove all fields, but only the ones that have null values (/winningYears) will.
    EXPECTED_REMOVE_NULL_DATA = dict(name='Al Gore',
                                     birthplace='Washington, D.C.',
                                     internetPatents='')

    # We'll ask to remove all fields, but only the ones that have empty string or null values will.
    EXPECTED_REMOVE_NULL_EMPTY_DATA = dict(name='Al Gore',
                                           birthplace='Washington, D.C.')

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = json.dumps(DATA)
    dev_raw_data_source.stop_after_first_batch = True

    field_remover = pipeline_builder.add_stage('Field Remover').set_attributes(**stage_attributes)
    if field_remover.action in ('KEEP', 'REMOVE'):
        field_remover.fields = ['/name']
    else:
        field_remover.fields = ['/name', '/birthplace', '/winningYears', '/internetPatents']
    if field_remover.action == 'REMOVE_CONSTANT':
        field_remover.constant = 'Al Gore'

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_remover >> wiretap.destination
    pipeline = pipeline_builder.build()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    record = wiretap.output_records[0]
    assert record.field == locals()[f"EXPECTED_{field_remover.action}_DATA"]


@pytest.mark.parametrize('stage_attributes', [{'action': 'REMOVE_CONSTANT'}])
@sdc_min_version('3.7.0')
def test_constant(sdc_builder, sdc_executor, stage_attributes):
    """:py:function:`stage.configuration.test_field_remover_processor.test_action` covers this case
    as we set the remover to remove all fields, but only provide a constant that matches one."""
    test_action(sdc_builder, sdc_executor, stage_attributes)


def test_fields(sdc_builder, sdc_executor):
    """:py:function:`stage.configuration.test_field_remover_processor.test_action` covers this case
    as we alternately set one field (when keeping or removing individual ones) or all of them."""
    test_action(sdc_builder, sdc_executor, dict(action='REMOVE'))


@pytest.mark.parametrize('on_record_error', ['TO_ERROR', 'DISCARD'])
@sdc_min_version('5.1.0')
def test_error_handling(sdc_builder, sdc_executor, on_record_error):
    """
    Tests the error handling for the On Record Error options 'Send to Error' and 'Discard'.
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    raw_data = dict(a='a', b='b')

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = json.dumps(raw_data)
    dev_raw_data_source.stop_after_first_batch = True

    field_remover = pipeline_builder.add_stage('Field Remover').set_attributes(
        preconditions=["${record:value('/a') != 'a'}"],
        on_record_error=on_record_error,
        action='REMOVE',
        fields=['/a']
    )

    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> field_remover >> wiretap.destination
    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline)

        sleep(10)

        assert len(wiretap.output_records) == 0

        if on_record_error == 'TO_ERROR':
            assert len(wiretap.error_records) == 1
        else:
            assert len(wiretap.error_records) == 0

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline, force=True)


@sdc_min_version('5.1.0')
def test_error_handling_stop_pipeline(sdc_builder, sdc_executor):
    """
    Tests the error handling for the On Record Error option 'Stop Pipeline'.
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    raw_data = dict(a='a', b='b')

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = json.dumps(raw_data)
    dev_raw_data_source.stop_after_first_batch = True

    field_remover = pipeline_builder.add_stage('Field Remover').set_attributes(
        preconditions=["${record:value('/a') != 'a'}"],
        on_record_error='STOP_PIPELINE',
        action='REMOVE',
        fields=['/a']
    )

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_remover >> wiretap.destination
    pipeline = pipeline_builder.build()

    sdc_executor.add_pipeline(pipeline)

    # Check the pipeline stops as soon as it receives the first record with a RunError
    sdc_executor.dump_log_on_error = False
    with pytest.raises(sdc_api.RunError) as exception_info:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
    sdc_executor.dump_log_on_error = True

    # Check the error arisen corresponds to "CONTAINER_0051 - Unsatisfied precondition(s)"
    assert("CONTAINER_0051" in exception_info.value.message)


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass

