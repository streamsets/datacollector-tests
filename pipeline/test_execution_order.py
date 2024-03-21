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
import os
import pytest
from streamsets.testframework.markers import sdc_min_version
from streamsets.testframework.utils import get_random_string
import string
import tempfile


logger = logging.getLogger(__name__)


@sdc_min_version('5.6.0')
@pytest.mark.parametrize('with_delay', [True, False])
def test_execution_order(sdc_builder, sdc_executor, with_delay):
    """
    Test execution order with and without a delay stage
       The pipeline looks like:
       dev_raw_data_source >> delay >> expe_eval >> wiretap
       dev_raw_data_source >= expe_eval >= wiretap
       Second line should be executed later
    """

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data='{"f1": "abc", "time": ""}',
                                       event_data='{"f1": "abc", "time": ""}',
                                       stop_after_first_batch=True)

    expression_evaluator_1 = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator_1.field_expressions = [{'fieldToSet': '/f1', 'expression': '1'},
                                                {'fieldToSet': '/time',
                                                 'expression': '${time:nowNanoTimestampString()}'}]

    expression_evaluator_2 = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator_2.field_expressions = [{'fieldToSet': '/f1', 'expression': '2'},
                                                {'fieldToSet': '/time',
                                                 'expression': '${time:nowNanoTimestampString()}'}]

    wiretap_1 = pipeline_builder.add_wiretap()
    wiretap_2 = pipeline_builder.add_wiretap()

    if with_delay:
        delay = pipeline_builder.add_stage('Delay').set_attributes(delay_between_batches=2000,
                                                                   on_record_error='DISCARD')
        dev_raw_data_source >> delay >> expression_evaluator_1 >> wiretap_1.destination
    else:
        dev_raw_data_source >> expression_evaluator_1 >> wiretap_1.destination

    dev_raw_data_source >= expression_evaluator_2
    expression_evaluator_2 >> wiretap_2.destination

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    assert wiretap_1.output_records[0].field['f1'] == '1'
    assert wiretap_2.output_records[0].field['f1'] == '2'

    assert str(wiretap_1.output_records[0].field['time']) < \
           str(wiretap_2.output_records[0].field['time'])


@sdc_min_version('5.6.0')
@pytest.mark.parametrize('with_crossing', [True, False])
def test_execution_order_multiple_lines(sdc_builder, sdc_executor, with_crossing):
    """
    Test execution order
       The pipeline looks like:
       Two lines with dev_raw_data_source >> delay >> expe_eval >> wiretap
       Two lines with dev_raw_data_source >= expe_eval >> identity >> wiretap
       The option with crossing adds connections
       Second two lines should be executed later than first two lines
    """

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data='{"f1": "abc", "time": ""}',
                                       event_data='{"f1": "abc", "time": ""}',
                                       stop_after_first_batch=True)

    expression_evaluator_1 = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator_1.field_expressions = [{'fieldToSet': '/f1', 'expression': '1'},
                                                {'fieldToSet': '/time',
                                                 'expression': '${time:nowNanoTimestampString()}'}]

    expression_evaluator_2 = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator_2.field_expressions = [{'fieldToSet': '/f1', 'expression': '2'},
                                                {'fieldToSet': '/time',
                                                 'expression': '${time:nowNanoTimestampString()}'}]

    expression_evaluator_3 = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator_3.field_expressions = [{'fieldToSet': '/f1', 'expression': '3'},
                                                {'fieldToSet': '/time',
                                                 'expression': '${time:nowNanoTimestampString()}'}]

    expression_evaluator_4 = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator_4.field_expressions = [{'fieldToSet': '/f1', 'expression': '4'},
                                                {'fieldToSet': '/time',
                                                 'expression': '${time:nowNanoTimestampString()}'}]

    wiretap_1 = pipeline_builder.add_wiretap()
    wiretap_2 = pipeline_builder.add_wiretap()
    wiretap_3 = pipeline_builder.add_wiretap()
    wiretap_4 = pipeline_builder.add_wiretap()

    delay_1 = pipeline_builder.add_stage('Delay').set_attributes(delay_between_batches=2000,
                                                                 on_record_error='DISCARD')
    delay_2 = pipeline_builder.add_stage('Delay').set_attributes(delay_between_batches=2000,
                                                                 on_record_error='DISCARD')
    dev_raw_data_source >> [delay_1, delay_2]

    if with_crossing:
        delay_1 >> [expression_evaluator_1, expression_evaluator_2]
        delay_2 >> [expression_evaluator_1, expression_evaluator_2]

        expression_evaluator_1 >> wiretap_1.destination
        expression_evaluator_2 >> wiretap_2.destination

        dev_raw_data_source >= [expression_evaluator_3, expression_evaluator_4]

        identity_1 = pipeline_builder.add_stage('Dev Identity')
        identity_2 = pipeline_builder.add_stage('Dev Identity')

        expression_evaluator_3 >> [identity_1, identity_2]
        expression_evaluator_4 >> [identity_1, identity_2]

        identity_1 >> wiretap_3.destination
        identity_2 >> wiretap_4.destination

    else:
        delay_1 >> expression_evaluator_1 >> wiretap_1.destination
        delay_2 >> expression_evaluator_2 >> wiretap_2.destination

        dev_raw_data_source >= [expression_evaluator_3, expression_evaluator_4]

        expression_evaluator_3 >> wiretap_3.destination
        expression_evaluator_4 >> wiretap_4.destination

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    assert wiretap_1.output_records[0].field['f1'] == '1'
    assert wiretap_2.output_records[0].field['f1'] == '2'
    assert wiretap_3.output_records[0].field['f1'] == '3'
    assert wiretap_4.output_records[0].field['f1'] == '4'

    f1_1 = wiretap_1.output_records[0].field['f1']
    f1_2 = wiretap_2.output_records[0].field['f1']
    f1_3 = wiretap_3.output_records[0].field['f1']
    f1_4 = wiretap_4.output_records[0].field['f1']

    time_1 = wiretap_1.output_records[0].field['time']
    time_2 = wiretap_2.output_records[0].field['time']
    time_3 = wiretap_3.output_records[0].field['time']
    time_4 = wiretap_4.output_records[0].field['time']

    assert str(wiretap_1.output_records[0].field['time']) < \
           str(wiretap_3.output_records[0].field['time'],
               f'Comparing w1 & w3 failed: {f1_1} - {f1_3} :: {time_1} - {time_3}')
    assert str(wiretap_2.output_records[0].field['time']) < \
           str(wiretap_3.output_records[0].field['time'],
               f'Comparing w2 & w3 failed: {f1_2} - {f1_3} :: {time_2} - {time_3}')
    assert str(wiretap_1.output_records[0].field['time']) < \
           str(wiretap_4.output_records[0].field['time'],
               f'Comparing w1 & w4 failed: {f1_1} - {f1_4} :: {time_1} - {time_4}')
    assert str(wiretap_2.output_records[0].field['time']) < \
           str(wiretap_4.output_records[0].field['time'],
               f'Comparing w2 & w4 failed: {f1_2} - {f1_4} :: {time_2} - {time_4}')


@sdc_min_version('5.6.0')
def test_execution_order_crossing_target_multiplex(sdc_builder, sdc_executor, keep_data):
    """
    Test records multiplexion
       The pipeline looks like:
            dev_raw_data_source >> exp_eval >> exp_eval >> [filesystem1, filesystem2]
            dev_raw_data_source >> exp_eval >> exp_eval >> [filesystem1, filesystem2]
       And the exp_eval are crossed too.
       Eight records should be produced.
       File System is used because wiretap can not be crossed.
       Shell executor is used to load the data
    """

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data='{"f1": "0" }',
                                       stop_after_first_batch=True)

    expression_evaluator_1 = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator_1.field_expressions = [{'fieldToSet': '/f1', 'expression': '${record:value("/f1")}1'}]

    expression_evaluator_2 = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator_2.field_expressions = [{'fieldToSet': '/f1', 'expression': '${record:value("/f1")}1'}]

    expression_evaluator_3 = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator_3.field_expressions = [{'fieldToSet': '/f1', 'expression': '${record:value("/f1")}2'}]

    expression_evaluator_4 = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator_4.field_expressions = [{'fieldToSet': '/f1', 'expression': '${record:value("/f1")}2'}]

    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    local_fs_1 = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs_1.set_attributes(data_format='JSON', directory_template=tmp_directory,
                              files_prefix='sdc1-', max_records_in_file=100)

    local_fs_2 = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs_2.set_attributes(data_format='JSON', directory_template=tmp_directory,
                              files_prefix='sdc2-', max_records_in_file=100)

    dev_raw_data_source >> [expression_evaluator_1, expression_evaluator_2]

    expression_evaluator_1 >> [expression_evaluator_3, expression_evaluator_4]
    expression_evaluator_2 >> [expression_evaluator_3, expression_evaluator_4]
    expression_evaluator_3 >> [local_fs_1, local_fs_2]
    expression_evaluator_4 >> [local_fs_1, local_fs_2]

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        shell_result_1 = sdc_executor.execute_shell(f'cat {tmp_directory}/sdc1-*')
        shell_result_2 = sdc_executor.execute_shell(f'cat {tmp_directory}/sdc2-*')
        # Eight records should be produced.
        assert shell_result_1.stdout.split('\n') == ['{"f1":"012"}'] * 4
        assert shell_result_2.stdout.split('\n') == ['{"f1":"012"}'] * 4

    finally:
        if not keep_data:
            sdc_executor.execute_shell(f'rm -r {tmp_directory}')
