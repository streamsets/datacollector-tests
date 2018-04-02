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
from collections import namedtuple

import pytest

from testframework.sdc_models import DataDriftRule, DataRule
logger = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def basic_rules_pipeline_builder(sdc_builder):
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.number_of_threads = 5
    dev_data_generator.fields_to_generate = [{'type': 'STRING',
                                              'precision': 10,
                                              'scale': 2,
                                              'field': 'random_string'}]

    trash_1 = pipeline_builder.add_stage('Trash')

    trash_2 = pipeline_builder.add_stage('Trash')

    dev_data_generator >> trash_1
    dev_data_generator >= trash_2

    yield namedtuple('PipelineBuilder', ['pipeline_builder',
                                         'dev_data_generator'])(pipeline_builder, dev_data_generator)


def test_basic_data_rules(basic_rules_pipeline_builder, sdc_executor):
    pipeline_builder = basic_rules_pipeline_builder.pipeline_builder
    dev_data_generator = basic_rules_pipeline_builder.dev_data_generator

    dev_data_generator.delay_between_batches = 0
    data_rule_data_lane = DataRule(stream=dev_data_generator.output_lanes[0],
                                   label='data-rule-data-lane',
                                   condition="${!record:exists('/a')}",
                                   alert_text='data-rule-data-lane',
                                   active=True)
    data_rule_event_lane = DataRule(stream=dev_data_generator.event_lanes[0],
                                    label='data-rule-event-lane',
                                    condition="${!record:exists('/a')}",
                                    alert_text='data-rule-event-lane',
                                    active=True)
    pipeline_builder.add_data_rule(data_rule_data_lane, data_rule_event_lane)

    pipeline = pipeline_builder.build('Data rules')
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(100)

    alerts = sdc_executor.get_alerts().for_pipeline(pipeline)
    assert len(alerts) == 2

    # The order of alerts is arbitrary, so clean up.
    data_alert = alerts[0] if alerts[0].label == 'data-rule-data-lane' else alerts[1]
    event_alert = alerts[1] if alerts[1].label == 'data-rule-event-lane' else alerts[0]

    assert data_alert.label == 'data-rule-data-lane'
    assert data_alert.alert_texts == ['data-rule-data-lane']
    assert event_alert.label == 'data-rule-event-lane'
    assert event_alert.alert_texts == ['data-rule-event-lane']

    sdc_executor.stop_pipeline(pipeline)


def test_basic_drift_rules(basic_rules_pipeline_builder, sdc_executor):
    pipeline_builder = basic_rules_pipeline_builder.pipeline_builder
    dev_data_generator = basic_rules_pipeline_builder.dev_data_generator

    dev_data_generator.delay_between_batches = 1000

    drift_rule_data_lane = DataDriftRule(stream=dev_data_generator.output_lanes[0],
                                         label='drift-rule-data-lane',
                                         condition='${1 != 0}',
                                         alert_text='drift-rule-data-lane',
                                         active=True)
    drift_rule_event_lane = DataDriftRule(stream=dev_data_generator.event_lanes[0],
                                          label='drift-rule-event-lane',
                                          condition='${1 != 0}',
                                          alert_text='drift-rule-event-lane',
                                          active=True)
    pipeline_builder.add_data_drift_rule(drift_rule_data_lane, drift_rule_event_lane)

    pipeline = pipeline_builder.build('Drift rules')
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(1)

    alerts = sdc_executor.get_alerts().for_pipeline(pipeline)
    assert len(alerts) == 2

    # The order of alerts is arbitrary, so clean up.
    data_alert = alerts[0] if alerts[0].label == 'drift-rule-data-lane' else alerts[1]
    event_alert = alerts[1] if alerts[1].label == 'drift-rule-event-lane' else alerts[0]

    assert data_alert.label == 'drift-rule-data-lane'
    assert data_alert.alert_texts == ['drift-rule-data-lane']
    assert event_alert.label == 'drift-rule-event-lane'

    assert event_alert.alert_texts == ['drift-rule-event-lane']
    sdc_executor.stop_pipeline(pipeline)
