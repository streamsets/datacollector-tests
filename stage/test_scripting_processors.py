# Copyright 2022 StreamSets Inc.
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


@sdc_min_version('5.1.0')
@pytest.mark.parametrize('stage_name', ['Groovy Evaluator', 'JavaScript Evaluator', 'Jython Evaluator'])
def test_scripting_processor_custom_metrics(sdc_builder, sdc_executor, stage_name):
    """Test custom metrics in script origin. It uses a script that generates X records and then checks they
    are correctly sent to the output stream. Then we just check timers and gauges exist and have reasonable values.

    Pipeline: scripting_origin >> wiretap
    """
    raw_company_1 = dict(name='StreamSets', floors=3)
    raw_company_2 = dict(name='Example Inc.', floors=1)
    raw_company_3 = dict(name='ASDF', floors=10)
    raw_company_4 = dict(name='QWER Inc.', floors=2)
    data = [raw_company_1, raw_company_2, raw_company_3, raw_company_4]
    raw_data = json.dumps(data)
    number_of_records = len(data)

    sleep_time = 0.5  # in seconds
    # mean delta error of time executing the script (which is almost the time spent inside the lock)
    delta = 0.1
    execution_time = sleep_time * number_of_records

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data,
                                       stop_after_first_batch=True)

    script = ''
    if stage_name == 'Groovy Evaluator':
        args = {'sleep_time': int(sleep_time * 1000)}
        script = GROOVY_SCRIPT_WITH_IMPORT_LOCK.format(**args)
    elif stage_name == 'JavaScript Evaluator':
        args = {'sleep_time': int(sleep_time * 1000)}
        script = JAVASCRIPT_SCRIPT_WITH_IMPORT_LOCK.format(**args)
    elif stage_name == 'Jython Evaluator':
        args = {'sleep_time': sleep_time}
        script = JYTHON_SCRIPT_WITH_IMPORT_LOCK.format(**args)

    scripting_stage = builder.add_stage(stage_name, type='processor')
    scripting_stage.set_attributes(script=script,
                                   record_processing_mode='BATCH')

    wiretap = builder.add_wiretap()

    dev_raw_data_source >> scripting_stage >> wiretap.destination

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    # search for a record whose 'name' is raw_company_1['name'] and assert new attribute ('officeSpace') is created
    # with expected boolean value (where 'floors' > 2)
    record_1 = next(record for record in wiretap.output_records if record.field['name'] == raw_company_1['name'])
    assert record_1.field['officeSpace'] == (raw_company_1['floors'] > 2)
    record_2 = next(record for record in wiretap.output_records if record.field['name'] == raw_company_2['name'])
    assert record_2.field['officeSpace'] == (raw_company_2['floors'] > 2)
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
state['buildings'] = []
for (record in records) {{
    try {{
        sdc.importLock();
        Thread.sleep({sleep_time});
        record.value['officeSpace'] = (record.value['floors'] > 2) ?: false

        def building = {{}}
        building.name = record.value['name']
        building.floors = record.value['floors']
        building.officeSpace = record.value['officeSpace']
        state['buildings'] << building
        output.write(record)
    }} catch (e) {{
        log.error(e.toString(), e)
        error.write(record, e.toString())
    }} finally {{
        sdc.importUnlock();
    }}
}}
"""

JAVASCRIPT_SCRIPT_WITH_IMPORT_LOCK = """
state["buildings"] = []
for (index in records) {{
    record = records[index]
    try {{
        sdc.importLock();
        var now = new Date().getTime();
        while(new Date().getTime() < now + {sleep_time}){{ /* do nothing */ }};
        record.value["officeSpace"] = (record.value["floors"] > 2)

        var building = {{}}
        building.name = record.value["name"]
        building.floors = record.value["floors"]
        building.officeSpace = record.value["officeSpace"]
        state["buildings"].push(building)
        output.write(record)
    }} catch (e) {{
        log.error(e.toString(), e)
        error.write(record, e.toString())
    }} finally {{
        sdc.importUnlock();
    }}
}}
"""

JYTHON_SCRIPT_WITH_IMPORT_LOCK = """
state['buildings'] = []
for record in records:
    try:
        sdc.importLock();
        import time
        # We add a sleep just to test timers
        time.sleep({sleep_time})
        record.value['officeSpace'] = (record.value['floors'] > 2)

        building = {{}}
        building['name'] = record.value['name']
        building['floors'] = record.value['floors']
        building['officeSpace'] = record.value['officeSpace']

        state['buildings'].append(building)
        output.write(record)
    except Exception as e:
        error.write(record, str(e))
    finally:
        sdc.importUnlock();
"""
