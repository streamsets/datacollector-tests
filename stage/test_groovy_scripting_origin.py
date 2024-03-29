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
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import sdc_min_version


# Definition of various groovy libraries and since when they were included in Data Collector
GROOVY_LIBS = [
  ("2.4", "streamsets-datacollector-groovy_2_4-lib", Version("1.0")),
  ("4.0", "streamsets-datacollector-groovy_4_0-lib", Version("5.2.0"))
]


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
      sdc_version = Version(data_collector.version)

      for info in GROOVY_LIBS:
        if sdc_version >= info[2]:
          data_collector.add_stage_lib(info[1])

    return hook


@sdc_min_version('3.10.0')
@pytest.mark.parametrize('groovy_version,library,min_sdc_version', GROOVY_LIBS, ids=[i[0] for i in GROOVY_LIBS])
def test_send_events(sdc_builder, sdc_executor, groovy_version, library, min_sdc_version):
    """Test event generation from a groovy script. It uses a script that generates 10 events and then checks they
    are correctly sent to the event stream.

    Pipeline: groovy >> wiretap

    """
    if Version(sdc_builder.version) < min_sdc_version:
      pytest.skip(f"Data Collector {sdc_builder.version} doesn't support Groovy {groovy_version}")

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

    groovy = builder.add_stage('Groovy Scripting', library=library)
    groovy.set_attributes(record_type='NATIVE_OBJECTS',
                          user_script=script,
                          batch_size=batch_size)

    records_wiretap = builder.add_wiretap()
    events_wiretap = builder.add_wiretap()

    groovy >> records_wiretap.destination
    groovy >= events_wiretap.destination

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    # Verify that the stage produced 10 events with values 'event0', ..., 'event9'. This is the expected
    # output in accordance to the script defined.
    expected_output = [f'event{i}' for i in range(batch_size)]
    actual_output = [e.field.value for e in events_wiretap.output_records]
    assert sorted(actual_output) == expected_output
    assert len(records_wiretap.output_records) == 0
    assert len(events_wiretap.error_records) == 0


@sdc_min_version('3.10.0')
@pytest.mark.parametrize('groovy_version,library,min_sdc_version', GROOVY_LIBS, ids=[i[0] for i in GROOVY_LIBS])
def test_send_error_records(sdc_builder, sdc_executor, groovy_version, library, min_sdc_version):
    """Test error records generation from a groovy script. It uses a script that generates 10 error records and
    then checks they are correctly sent to the error stream.

    Pipeline: groovy >> wiretap

    """
    if Version(sdc_builder.version) < min_sdc_version:
      pytest.skip(f"Data Collector {sdc_builder.version} doesn't support Groovy {groovy_version}")

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

    groovy = builder.add_stage('Groovy Scripting', library=library)
    groovy.set_attributes(record_type='NATIVE_OBJECTS',
                          user_script=script,
                          batch_size=batch_size)

    records_wiretap = builder.add_wiretap()
    events_wiretap = builder.add_wiretap()

    groovy >> records_wiretap.destination
    groovy >= events_wiretap.destination

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    # Verify that the stage produced 10 error records with values 'error0', ..., 'error9'. This is the
    # expected output in accordance to the script defined.
    expected_output = [f'error{i}' for i in range(batch_size)]
    actual_output = [e.field.value for e in events_wiretap.error_records]
    assert sorted(actual_output) == expected_output
    assert len(records_wiretap.output_records) == 0
    assert len(events_wiretap.output_records) == 0


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
offset = 0

while (offset < sdc.batchSize) {
    event = sdc.createEvent('event-type', 1)
    event.value = 'event' + offset.toString()
    sdc.toEvent(event)
    offset = offset + 1
}
"""


SCRIPT_SEND_EVENTS_v2 = """
// single threaded - no entityName because we need only one offset
entityName = ''

offset = 0
cur_batch = sdc.createBatch()

hasNext = true
while (hasNext) {
    try {
        event = sdc.createEvent('event-type', 1)
        event.value = 'event' + offset.toString()
        cur_batch.addEvent(event)
        offset = offset + 1

        // if the batch is full, process it and start a new one
        if (cur_batch.eventCount() >= sdc.batchSize) {
            // blocks until all records are written to all destinations
            // (or failure) and updates offset
            // in accordance with delivery guarantee
            cur_batch.process(entityName, offset.toString())
            cur_batch = sdc.createBatch()
            if (sdc.isStopped() || offset >= sdc.batchSize) {
                hasNext = false
            }
        }
    } catch (Exception e) {
        cur_batch.addError(event, e.toString())
        cur_batch.process(entityName, offset.toString())
        hasNext = false
    }
}
"""


SCRIPT_SEND_ERROR_RECORDS_v1 = """
offset = 0;

while (offset < sdc.batchSize) {
    record = sdc.createRecord('record source id');
    record.value = 'error' + offset.toString();
    sdc.error.write(record, 'testing error stream');
    offset++;
}
"""


SCRIPT_SEND_ERROR_RECORDS_v2 = """
// single threaded - no entityName because we need only one offset
entityName = "";
offset = 0;

cur_batch = sdc.createBatch();
record = sdc.createRecord('generated data');

hasNext = true;
while (hasNext) {
    try {
        record = sdc.createRecord('record source id');
        record.value = 'error' + offset.toString();
        cur_batch.addError(record, "error msg");
        offset++;

        // if the batch is full, process it and start a new one
        if (cur_batch.errorCount() >= sdc.batchSize) {
            // blocks until all records are written to all destinations
            // (or failure) and updates offset
            // in accordance with delivery guarantee
            cur_batch.process(entityName, offset.toString())
            cur_batch = sdc.createBatch();
            if (sdc.isStopped() || offset >= sdc.batchSize) {
                hasNext = false;
            }
        }
    } catch (Exception e) {
        cur_batch.addError(record, e.toString());
        cur_batch.process(entityName, offset.toString());
        hasNext = false;
    }
}
"""
