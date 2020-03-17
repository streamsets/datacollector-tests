# Copyright 2019 StreamSets Inc.
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


import pytest
from streamsets.sdk import sdc_api
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import sdc_min_version


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.add_stage_lib('streamsets-datacollector-jython_2_7-lib')
    return hook


@sdc_min_version('3.10.0')
def test_send_events(sdc_builder, sdc_executor):
    """Test event generation from a jython script. It uses a script that generates 10 events and then checks they
    are correctly sent to the event stream.

    Pipeline: jython >> trash

    """

    builder_api_version = _get_scripting_api_version(sdc_builder.version)
    executor_api_version = _get_scripting_api_version(sdc_executor.version)

    if builder_api_version != executor_api_version:
        pytest.skip(
            f'Scripting API versions are not compatible (builder: {builder_api_version}'
            f',executor: {executor_api_version})'
        )

    if builder_api_version == 'v1':
        script = SCRIPT_SEND_EVENTS_v1
    else:
        script = SCRIPT_SEND_EVENTS_v2

    batch_size = 10
    builder = sdc_builder.get_pipeline_builder()

    # Event generation API for scripting origins changed in SDC 3.12. Addressing it with different testing
    # scripts.

    jython = builder.add_stage('Jython Scripting')
    jython.set_attributes(record_type='NATIVE_OBJECTS',
                          user_script=script,
                          batch_size=batch_size)

    trash1 = builder.add_stage('Trash')
    trash2 = builder.add_stage('Trash')

    jython >> trash1
    jython >= trash2

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)

    # Verify that the stage produced 10 events with values 'event0', ..., 'event9'. This is the expected
    # output in accordance to the script.
    expected_output = [f'event{i}' for i in range(batch_size)]
    actual_output = [e.field.value for e in snapshot[jython.instance_name].event_records]
    assert sorted(actual_output) == expected_output
    assert len(snapshot[jython.instance_name].output) == 0
    assert len(snapshot[jython.instance_name].error_records) == 0


@sdc_min_version('3.10.0')
@pytest.mark.parametrize('stage_attributes', [
    {'on_record_error': 'TO_ERROR'},
    {'on_record_error': 'STOP_PIPELINE'}
])
def test_send_error_records(sdc_builder, sdc_executor, stage_attributes):
    """Test error records generation from a jython script. It uses a script that generates 10 error records and
    then checks they are correctly sent to the error stream.

    Pipeline: jython >> trash

    """

    builder_api_version = _get_scripting_api_version(sdc_builder.version)
    executor_api_version = _get_scripting_api_version(sdc_executor.version)

    if builder_api_version != executor_api_version:
        pytest.skip(
            f'Scripting API versions are not compatible (builder: {builder_api_version}'
            f',executor: {executor_api_version})'
        )

    if builder_api_version == 'v1':
        script = SCRIPT_SEND_ERROR_RECORDS_v1
    else:
        script = SCRIPT_SEND_ERROR_RECORDS_v2

    batch_size = 10
    builder = sdc_builder.get_pipeline_builder()

    jython = builder.add_stage('Jython Scripting')
    jython.set_attributes(record_type='NATIVE_OBJECTS',
                          on_record_error=stage_attributes['on_record_error'],
                          user_script=script,
                          batch_size=batch_size)

    trash = builder.add_stage('Trash')
    jython >> trash

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    try:
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    # SDC-13828 - may raise either RunError or RunningError depending on whether or not
    # the user script has terminated (i.e. the pipeline is stopped) in time
    except (sdc_api.RunError, sdc_api.RunningError) as ex:
        assert stage_attributes['on_record_error'] == 'STOP_PIPELINE', \
            "RunError or RunningError should only occur when ErrorRecords generated with STOP_PIPELINE."

    # if on_record_error is TO_ERROR, then error counts should match generated.
    try:
        if stage_attributes['on_record_error'] == 'TO_ERROR':
            sdc_executor.stop_pipeline(pipeline)

            # Verify that the stage produced 10 error records with values 'error0', ..., 'error9'. This is the
            # expected output in accordance to the script defined.
            expected_output = [f'error{i}' for i in range(batch_size)]
            actual_output = [e.field.value for e in snapshot[jython.instance_name].error_records]
            assert sorted(actual_output) == expected_output
            assert len(snapshot[jython.instance_name].output) == 0
            assert len(snapshot[jython.instance_name].event_records) == 0
    finally:
        sdc_executor.remove_pipeline()


def _get_scripting_api_version(sdc_version):
    """Get the scripting API version as a function of a given SDC version
    :param sdc_version: SDC version to check, in string format
    :return: v1 if version < 3.12.0, v2 otherwise
    """
    # Scripting API version changed with no retrocompatibility
    # in SDC 3.12.0, so if a test is launched as 3.10.0 > 3.13.0 or something similar
    # it won't work
    # This is why the tests that use this function only run when both the builder and
    # the executor have the same version
    if Version(sdc_version) < Version('3.12.0'):
        return 'v1'
    return 'v2'


SCRIPT_SEND_EVENTS_v1 = """
for i in range(sdc.batchSize):
    event = sdc.createEvent('event-type', 1)
    event.value = 'event' + str(i)
    sdc.toEvent(event)
"""


SCRIPT_SEND_EVENTS_v2 = """
entityName = ''
offset = 0
cur_batch = sdc.createBatch()

hasNext = True
while hasNext:
    event = sdc.createEvent('event-type', 1)
    event.value = 'event' + str(offset)
    cur_batch.addEvent(event)
    offset = offset + 1

    # if the batch is full, process it and start a new one
    if cur_batch.eventCount() >= sdc.batchSize:

        # blocks until all records are written to all destinations
        # (or failure) and updates offset
        # in accordance with delivery guarantee
        cur_batch.process(entityName, str(offset))
        cur_batch = sdc.createBatch()
        # if the pipeline has been stopped, we should end the script
        if sdc.isStopped():
            hasNext = False
"""


SCRIPT_SEND_ERROR_RECORDS_v1 = """
for i in range(sdc.batchSize):
    record = sdc.createRecord('record souce id')
    record.value = 'error' + str(i)
    sdc.error.write(record, 'testing error stream')
"""


SCRIPT_SEND_ERROR_RECORDS_v2 = """
entityName = ''
offset = 0
cur_batch = sdc.createBatch()

hasNext = True
while hasNext:
    record = sdc.createRecord('record souce id')
    record.value = 'error' + str(offset)
    cur_batch.addError(record, "error msg")
    offset = offset + 1

    # if the batch is full, process it and start a new one
    if cur_batch.errorCount() >= sdc.batchSize:

        # blocks until all records are written to all destinations
        # (or failure) and updates offset
        # in accordance with delivery guarantee
        cur_batch.process(entityName, str(offset))
        cur_batch = sdc.createBatch()
        # if the pipeline has been stopped, we should end the script
        if sdc.isStopped():
            hasNext = False
"""
