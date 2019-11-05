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


from streamsets.testframework.markers import sdc_min_version


@sdc_min_version('3.10.0')
def test_send_events(sdc_builder, sdc_executor):
    """Test event generation from a jython script. It uses a script that generates 10 events and then checks they
    are correctly sent to the event stream.

    Pipeline: jython >> trash

    """
    batch_size = 10
    builder = sdc_builder.get_pipeline_builder()

    jython = builder.add_stage('Jython Scripting')
    jython.set_attributes(produce_events=True,
                          record_type='NATIVE_OBJECTS',
                          user_script=SCRIPT_SEND_EVENTS,
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
    # output in accordance to the script defined in SCRIPT_SEND_EVENTS variable.
    expected_output = [f'event{i}' for i in range(batch_size)]
    actual_output = [e.field.value for e in snapshot[jython.instance_name].event_records]
    assert sorted(actual_output) == expected_output
    assert len(snapshot[jython.instance_name].output) == 0
    assert len(snapshot[jython.instance_name].error_records) == 0


@sdc_min_version('3.10.0')
def test_send_error_records(sdc_builder, sdc_executor):
    """Test error records generation from a jython script. It uses a script that generates 10 error records and
    then checks they are correctly sent to the error stream.

    Pipeline: jython >> trash

    """
    batch_size = 10
    builder = sdc_builder.get_pipeline_builder()

    jython = builder.add_stage('Jython Scripting')
    jython.set_attributes(record_type='NATIVE_OBJECTS',
                          user_script=SCRIPT_SEND_ERROR_RECORDS,
                          batch_size=batch_size)

    trash = builder.add_stage('Trash')
    jython >> trash

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)

    # Verify that the stage produced 10 error records with values 'error0', ..., 'error9'. This is the
    # expected output in accordance to the script defined in SCRIPT_SEND_TO_ERROR variable.
    expected_output = [f'error{i}' for i in range(batch_size)]
    actual_output = [e.field.value for e in snapshot[jython.instance_name].error_records]
    assert sorted(actual_output) == expected_output
    assert len(snapshot[jython.instance_name].output) == 0
    assert len(snapshot[jython.instance_name].event_records) == 0


SCRIPT_SEND_EVENTS = """
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

SCRIPT_SEND_ERROR_RECORDS = """
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
