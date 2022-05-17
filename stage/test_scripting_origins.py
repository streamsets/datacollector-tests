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

import logging

import pytest
from streamsets.testframework.markers import sdc_min_version

logger = logging.getLogger(__name__)


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.add_stage_lib('streamsets-datacollector-jython_2_7-lib')
        data_collector.add_stage_lib('streamsets-datacollector-basic-lib')
        data_collector.add_stage_lib('streamsets-datacollector-groovy_2_4-lib')

    return hook


@sdc_min_version('3.11.0')
@pytest.mark.parametrize('stage_name', ['Groovy Scripting', 'JavaScript Scripting', 'Jython Scripting'])
def test_scripting_origin_default_script(sdc_builder, sdc_executor, stage_name):
    """ Test a scripting origin with the default script. Ensure that it generates records as expected."
    """
    pb = sdc_builder.get_pipeline_builder()
    origin = pb.add_stage(stage_name, type='origin')
    origin.set_attributes(batch_size=7)
    wiretap = pb.add_wiretap()

    origin >> wiretap.destination

    pipeline = pb.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'output_record_count', 1)
    sdc_executor.stop_pipeline(pipeline)
    records = [int(str(record.field)[1:]) for record in wiretap.output_records]
    records.sort()
    assert records == [i+1 for i in range(len(wiretap.output_records))]


@sdc_min_version('5.1.0')
@pytest.mark.parametrize('stage_name', ['Groovy Scripting', 'JavaScript Scripting', 'Jython Scripting'])
def test_scripting_origin_custom_metrics(sdc_builder, sdc_executor, stage_name):
    """Test custom metrics in script origin. It uses a script that generates X records and then checks they
    are correctly sent to the output stream. Then we just check timers and gauges exist and have reasonable values.

    Pipeline: scripting_origin >> wiretap
    """
    number_of_records = 10
    batch_size = 1
    sleep_time = 0.5  # in seconds
    # mean delta error of time executing the script (which is almost the time spent inside the lock)
    delta = 0.1
    execution_time = sleep_time * number_of_records

    builder = sdc_builder.get_pipeline_builder()

    script = ''
    if stage_name == 'Groovy Scripting':
        args = {'number_of_records': number_of_records, 'sleep_time': int(sleep_time * 1000)}
        script = GROOVY_SCRIPT_WITH_IMPORT_LOCK.format(**args)
    elif stage_name == 'JavaScript Scripting':
        args = {'number_of_records': number_of_records, 'sleep_time': int(sleep_time * 1000)}
        script = JAVASCRIPT_SCRIPT_WITH_IMPORT_LOCK.format(**args)
    elif stage_name == 'Jython Scripting':
        args = {'number_of_records': number_of_records, 'sleep_time': sleep_time}
        script = JYTHON_SCRIPT_WITH_IMPORT_LOCK.format(**args)

    scripting_stage = builder.add_stage(stage_name)
    scripting_stage.set_attributes(record_type='NATIVE_OBJECTS',
                                   user_script=script,
                                   batch_size=batch_size)

    records_wiretap = builder.add_wiretap()

    scripting_stage >> records_wiretap.destination

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    # Verify that the stage produced 10 records with values 'record0', ..., 'record9'. This is the expected
    # output in accordance to the script.
    assert len(records_wiretap.output_records) == number_of_records
    expected_output = [f'record{i}' for i in range(number_of_records)]
    actual_output = [record.field.value for record in records_wiretap.output_records]
    assert sorted(actual_output) == expected_output
    history = sdc_executor.get_pipeline_history(pipeline)
    assert_metrics(stage_name, history, execution_time, sleep_time, delta, number_of_records)


def assert_metrics(stage_name, history, execution_time, sleep_time, delta, number_of_records):
    metric_name = stage_name.replace(' ', '') + '_01'
    lock_delta = delta
    script_delta = delta * number_of_records
    assert history.latest.metrics.timer(
        f'custom.{metric_name}.Script Execution Time (in seconds).0.timer').count == 1
    assert (execution_time - script_delta <=
            history.latest.metrics.timer(
                f'custom.{metric_name}.Script Execution Time (in seconds).0.timer')._data.get('mean')
            <= execution_time + script_delta), "Script execution time is not in the expected range."
    assert history.latest.metrics.timer(
        f'custom.{metric_name}.Lock Usage Time (in seconds).0.timer').count == number_of_records
    assert (sleep_time - lock_delta <=
            history.latest.metrics.timer(
                f'custom.{metric_name}.Lock Usage Time (in seconds).0.timer')._data.get('mean')
            <= sleep_time + lock_delta), "Lock usage time is not in the expected range."
    assert history.latest.metrics.timer(
        f'custom.{metric_name}.Lock Wait Time (in seconds).0.timer').count == number_of_records
    # with this scenario there is no time waiting for lock
    assert history.latest.metrics.timer(
        f'custom.{metric_name}.Lock Wait Time (in seconds).0.timer')._data.get(
        'mean') <= 0.01, "Time waiting for lock should be 0 if it's the only pipeline getting executed"
    # And that the gauge exists, and it is actually in unlocked status
    assert history.latest.metrics.gauge(
        f'custom.{metric_name}.Lock Status.0.gauge').value == {'Lock Status': 'Unlocked'}


GROOVY_SCRIPT_WITH_IMPORT_LOCK = """
entityName = "";
offset = 0;
cur_batch = sdc.createBatch();

hasNext = true;
while (hasNext) {{
    try {{
        sdc.importLock();
        // We add a sleep just to test timers
        Thread.sleep({sleep_time});
        sdc.importUnlock();
        record = sdc.createRecord('record created ' + offset.toString());
        value = "record" + offset.toString();
        record.value = value;
        cur_batch.add(record);
        offset++;

        // if the batch is full, process it and start a new one
        if (cur_batch.size() >= sdc.batchSize) {{
            // blocks until all records are written to all destinations
            // (or failure) and updates offset
            // in accordance with delivery guarantee
            cur_batch.process(entityName, offset.toString());
            cur_batch = sdc.createBatch();
            // if the pipeline has been stopped, we should end the script
            if (sdc.isStopped() || offset >= {number_of_records}) {{
                hasNext = false;
            }}
        }}
    }} finally {{}}
}}
"""

JAVASCRIPT_SCRIPT_WITH_IMPORT_LOCK = """
entityName = "";
offset = 0;
cur_batch = sdc.createBatch();

hasNext = true;
while (hasNext) {{
    try {{
        sdc.importLock();
        // We add a sleep just to test timers
        var now = new Date().getTime();
        while(new Date().getTime() < now + {sleep_time}){{ /* do nothing */ }};
        sdc.importUnlock();
        record = sdc.createRecord('record created ' + offset.toString());
        value = "record" + offset.toString();
        record.value = value;
        cur_batch.add(record);
        offset++;

        // if the batch is full, process it and start a new one
        if (cur_batch.size() >= sdc.batchSize) {{
            // blocks until all records are written to all destinations
            // (or failure) and updates offset
            // in accordance with delivery guarantee
            cur_batch.process(entityName, offset.toString());
            cur_batch = sdc.createBatch();
            // if the pipeline has been stopped, we should end the script
            if (sdc.isStopped() || offset >= {number_of_records}) {{
                hasNext = false;
            }}
        }}
    }} finally {{}}
}}
"""

JYTHON_SCRIPT_WITH_IMPORT_LOCK = """
entityName = ''
offset = 0
cur_batch = sdc.createBatch()

hasNext = True
while hasNext:
    sdc.importLock()
    import time
    # We add a sleep just to test timers
    time.sleep({sleep_time})
    sdc.importUnlock()
    record = sdc.createRecord('record created ' + str(offset))
    value = 'record' + str(offset)
    record.value = value
    cur_batch.add(record)
    offset = offset + 1

    # if the batch is full, process it and start a new one
    if cur_batch.size() >= sdc.batchSize:

        # blocks until all records are written to all destinations
        # (or failure) and updates offset
        # in accordance with delivery guarantee
        cur_batch.process(entityName, str(offset))
        cur_batch = sdc.createBatch()
        # if the pipeline has been stopped, we should end the script
        if sdc.isStopped() or offset >= {number_of_records}:
            hasNext = False
"""
