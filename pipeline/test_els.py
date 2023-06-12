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
from datetime import datetime, timezone
from dateutil import parser
from dateutil.relativedelta import relativedelta

import pytest
from streamsets.testframework.markers import sdc_min_version

logger = logging.getLogger(__name__)


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.add_user('arvind', roles=['admin'])
        data_collector.add_user('girish', roles=['admin'])

    return hook


def test_pipeline_el_user(random_expression_pipeline_builder, sdc_executor):
    random_expression_pipeline_builder.expression_evaluator.header_attribute_expressions = [
        {'attributeToSet': 'user',
        'headerAttributeExpression': '${pipeline:user()}'}
    ]
    pipeline = random_expression_pipeline_builder.pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    # Run the pipeline as one user.
    sdc_executor.set_user('arvind')
    sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(1)
    sdc_executor.stop_pipeline(pipeline)

    record = random_expression_pipeline_builder.wiretap.output_records[0]
    assert record.header['values']['user'] == 'arvind'

    # And then try different user.
    random_expression_pipeline_builder.wiretap.reset()
    sdc_executor.set_user('girish')
    sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(1)
    sdc_executor.stop_pipeline(pipeline)

    record = random_expression_pipeline_builder.wiretap.output_records[0]
    assert record.header['values']['user'] == 'girish'


def test_pipeline_el_name_title_id_version(random_expression_pipeline_builder, sdc_executor):
    random_expression_pipeline_builder.expression_evaluator.header_attribute_expressions = [
        {'attributeToSet': 'title', 'headerAttributeExpression': '${pipeline:title()}'},
        {'attributeToSet': 'name', 'headerAttributeExpression': '${pipeline:name()}'},
        {'attributeToSet': 'version', 'headerAttributeExpression': '${pipeline:version()}'},
        {'attributeToSet': 'id', 'headerAttributeExpression': '${pipeline:id()}'},
    ]
    pipeline = random_expression_pipeline_builder.pipeline_builder.build(title='Most Pythonic Pipeline')
    pipeline.metadata['dpm.pipeline.version'] = 42

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(1)
    sdc_executor.stop_pipeline(pipeline)

    record = random_expression_pipeline_builder.wiretap.output_records[0]
    assert record.header['values']['name'] == pipeline.id
    assert record.header['values']['id'] == pipeline.id
    assert record.header['values']['title'] == pipeline.title
    assert record.header['values']['version'] == '42'


def test_str_unescape_and_replace_el(sdc_builder, sdc_executor):
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data='here\nis\tsome\ndata',
                                       use_custom_delimiter=True, custom_delimiter='^^^')

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.field_expressions = [
        {'fieldToSet': '/transformed',
         'expression': '${str:replace(record:value("/text"), str:unescapeJava("\\\\n"), "<NEWLINE>")}'},
        {'fieldToSet': '/transformed2',
         'expression': '${str:replace(record:value("/transformed"), str:unescapeJava("\\\\t"), "<TAB>")}'}
    ]

    wiretap_1 = pipeline_builder.add_wiretap()
    wiretap_2 = pipeline_builder.add_wiretap()


    dev_raw_data_source >> [expression_evaluator, wiretap_1.destination]
    expression_evaluator >> wiretap_2.destination

    pipeline = pipeline_builder.build()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(10)
    sdc_executor.stop_pipeline(pipeline)
    input_records = wiretap_1.output_records
    output_records = wiretap_2.output_records
    assert len(output_records) == len(input_records)
    assert input_records[0].field['text'] == 'here\nis\tsome\ndata'
    assert output_records[0].field['text'] == 'here\nis\tsome\ndata'
    assert output_records[0].field['transformed'] == 'here<NEWLINE>is\tsome<NEWLINE>data'
    assert output_records[0].field['transformed2'] == 'here<NEWLINE>is<TAB>some<NEWLINE>data'


def test_record_el(random_expression_pipeline_builder, sdc_executor):
    random_expression_pipeline_builder.expression_evaluator.header_attribute_expressions = [
        {'attributeToSet': 'valueOrDefault', 'headerAttributeExpression': '${record:valueOrDefault("/non-existing", 3)}'},
    ]
    pipeline = random_expression_pipeline_builder.pipeline_builder.build(title='Record ELs')

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(1)
    sdc_executor.stop_pipeline(pipeline)

    record = random_expression_pipeline_builder.wiretap.output_records[0]
    assert record.header['values']['valueOrDefault'] == '3'


@sdc_min_version('3.17.0')
def test_runtime_resources_dir_path_el(random_expression_pipeline_builder, sdc_executor):
    random_expression_pipeline_builder.expression_evaluator.header_attribute_expressions = [
        {'attributeToSet': 'resourcesDirPath', 'headerAttributeExpression': '${runtime:resourcesDirPath()}'},
    ]
    pipeline = random_expression_pipeline_builder.pipeline_builder.build(title='Resources Directory Path EL')

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(1)
    sdc_executor.stop_pipeline(pipeline)

    record = random_expression_pipeline_builder.wiretap.output_records[0]
    assert record.header['values']['resourcesDirPath'] is not None
    assert len(record.header['values']['resourcesDirPath']) > 0


@sdc_min_version('5.4.0')
@pytest.mark.parametrize('expression', ['${job:id()}', '${job:name()}', '${job:user()}', '${job:startTime()}'])
def test_job_els(sdc_executor, sdc_builder, expression):
    """Test job EL with pipeline configurations

    The pipeline looks like:
    dev_raw_data_source >> trash
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT',
                                       raw_data=expression,
                                       stop_after_first_batch=True)
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> wiretap.destination

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 1
    assert len(wiretap.error_records) == 0

    # We can only validate the startTime since others will be UNDEFINED for SDC
    if expression == '${job:startTime()}':
        try:
            datetime.strptime(str(wiretap.output_records[0].field['text']), "%a %b %d %H:%M:%S %Z %Y")
        except ValueError:
            pytest.fail("Invalid start time job")


@pytest.mark.parametrize('date_interval, date_value, expected_delta', [
    # Using ~year interval to avoid timezone daylight saving (it creates lots of flakiness as it's system dependant)
    # e.g.: 52 weeks is a year-ish, so note it can get to be flaky sometime.
    # If the expected delta was in the same units as the interval, and the timezone was always the same, it would be
    # 100% deterministic.
    # It's good to leave the test with random numbers to spot more issues, acknowledging flakiness is possible
    ('YEAR', 1, {'years': 1}),
    ('YEAR', -1, {'years': -1}),
    ('QUARTER', 4, {'years': 1}),
    ('QUARTER', -4, {'years': -1}),
    ('MONTH', 12, {'months': 12}),
    ('MONTH', -12, {'months': -12}),
    ('DAY_OF_YEAR', 365, {'days': 365}),
    ('DAY_OF_YEAR', -365, {'days': -365}),
    ('DAY', 365, {'days': 365}),
    ('DAY', -365, {'days': -365}),
    ('WEEKDAY', 365, {'days': 365}),
    ('WEEKDAY', -365, {'days': -365}),
    ('WEEK', 52, {'weeks': 52}),
    ('WEEK', -52, {'weeks': -52}),
    ('HOUR', 365 * 24, {'hours': 365 * 24}),
    ('HOUR', - 365 * 24, {'hours': - 365 * 24}),
    ('MINUTE', 365 * 24 * 60, {'minutes': 365 * 24 * 60}),
    ('MINUTE', - 365 * 24 * 60, {'minutes': - 365 * 24 * 60}),
    ('SECOND', 365 * 24 * 60 * 60, {'seconds': 365 * 24 * 60 * 60}),
    ('SECOND', - 365 * 24 * 60 * 60, {'seconds': - 365 * 24 * 60 * 60}),
    ('MILLISECOND', 24 * 60 * 60, {'microseconds': 24 * 60 * 60 * 1000}),
    ('MILLISECOND', - 24 * 60 * 60, {'microseconds': - 24 * 60 * 60 * 1000}),
])
def test_date_addition_el(sdc_executor, sdc_builder, date_interval, date_value, expected_delta):
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.records_to_be_generated = 1
    dev_data_generator.fields_to_generate = [
        {'field': 'field', 'precision': 10, 'scale': 2, 'type': 'DATETIME'}
    ]

    expression = f'time:dateAddition("{date_interval}", {date_value}, record:value("/field"))'
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.field_expressions = [
        {'fieldToSet': '/transformed', 'expression': '${' + f'{expression}' + '}'}
    ]

    wiretap_1 = pipeline_builder.add_wiretap()
    wiretap_2 = pipeline_builder.add_wiretap()

    dev_data_generator >> [expression_evaluator, wiretap_1.destination]
    expression_evaluator >> wiretap_2.destination

    pipeline = pipeline_builder.build()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    input_records = wiretap_1.output_records
    output_records = wiretap_2.output_records
    assert len(output_records) == len(input_records)
    assert input_records[0].field['field'] == output_records[0].field['field']
    input_data = parser.isoparse(str(output_records[0].field['field'])).astimezone(timezone.utc)
    transformed_data = parser.isoparse(str(output_records[0].field['transformed'])).astimezone(timezone.utc)
    assert transformed_data == input_data + relativedelta(**expected_delta)


@pytest.mark.parametrize('date_interval, date_value, initial_date, expected_date', [
    # We need fixed dates to test this, as by using decimal precision, it is really difficult to avoid timezone issues
    # Note this test can fail depending on the system due to timezone used
    ('YEAR', 1.5, "2013-01-15 08:11:41.345", "2014-07-15 08:11:41.345"),
    ('QUARTER', 4.5, "2013-01-15 02:11:41.345", "2014-03-01 02:11:41.345"),
    ('MONTH', 12.5, "2013-01-15 02:11:41.345", "2014-01-30 14:11:41.345"),
    ('DAY_OF_YEAR', 1.5, "2013-01-15 02:11:41.345", "2013-01-16 14:11:41.345"),
    ('DAY', 1.5, "2013-01-15 02:11:41.345", "2013-01-16 14:11:41.345"),
    ('WEEKDAY', 1.5, "2013-01-15 02:11:41.345", "2013-01-16 14:11:41.345"),
    ('WEEK', 1.5, "2013-01-15 02:11:41.345", "2013-01-25 14:11:41.345"),
    ('HOUR', 12.5, "2013-01-15 02:11:41.345", "2013-01-15 14:41:41.345"),
    ('MINUTE', 125.5, "2013-01-15 02:11:41.345", "2013-01-15 04:17:11.345"),
    ('SECOND', 1.5, "2013-01-15 02:11:41.345", "2013-01-15 02:11:42.845"),
    ('MILLISECOND', 100.5, "2013-01-15 02:11:41.345", "2013-01-15 02:11:41.445"),  # ignoring further precision (0.5 is ignored)
])
def test_date_addition_decimal_el(sdc_executor, sdc_builder, date_interval, date_value, initial_date, expected_date):
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.records_to_be_generated = 1
    dev_data_generator.fields_to_generate = [
        {'field': 'field', 'precision': 10, 'scale': 2, 'type': 'DATETIME'}
    ]

    expression = f'time:dateAddition("{date_interval}", {date_value},' \
                 f' time:createDateFromStringTZ("{initial_date}", "UTC", "yyyy-MM-dd HH:mm:ss.SSS"))'
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.field_expressions = [
        {'fieldToSet': '/transformed', 'expression': '${' + f'{expression}' + '}'}
    ]

    wiretap_1 = pipeline_builder.add_wiretap()
    wiretap_2 = pipeline_builder.add_wiretap()

    dev_data_generator >> [expression_evaluator, wiretap_1.destination]
    expression_evaluator >> wiretap_2.destination

    pipeline = pipeline_builder.build()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    input_records = wiretap_1.output_records
    output_records = wiretap_2.output_records
    assert len(output_records) == len(input_records)
    assert input_records[0].field['field'] == output_records[0].field['field']
    input_data = parser.isoparse(expected_date).astimezone(timezone.utc)
    transformed_data = parser.isoparse(str(output_records[0].field['transformed'])).astimezone(timezone.utc)
    assert transformed_data == input_data


@pytest.mark.parametrize('date_interval, date_diff_value', [
    ('YEAR', 5),
    ('QUARTER', 3),
    ('MONTH', 10),
    ('DAY_OF_YEAR', 10),
    ('DAY', 365),
    ('WEEKDAY', 7),
    ('WEEK', 20),
    ('HOUR', 18),
    ('MINUTE', 100),
    ('SECOND', 20),
    ('MILLISECOND', 3000),
])
def test_date_difference_el(sdc_executor, sdc_builder, date_interval, date_diff_value):
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.records_to_be_generated = 1
    dev_data_generator.fields_to_generate = [
        {'field': 'field', 'precision': 10, 'scale': 2, 'type': 'DATETIME'}
    ]

    add_expression = f'time:dateAddition("{date_interval}", {date_diff_value}, record:value("/field"))'
    neg_expression = f'time:dateAddition("{date_interval}", -{date_diff_value}, record:value("/field"))'
    diff_expression = f'time:dateDifference("{date_interval}", record:value("/transformed"), record:value("/field"))'
    neg_diff_expression = f'time:dateDifference("{date_interval}", record:value("/transformed2"), record:value("/field"))'
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.field_expressions = [
        {'fieldToSet': '/transformed', 'expression': '${' + f'{add_expression}' + '}'},
        {'fieldToSet': '/transformed2', 'expression': '${' + f'{neg_expression}' + '}'},
        {'fieldToSet': '/transformed3', 'expression': '${' + f'{diff_expression}' + '}'},
        {'fieldToSet': '/transformed4', 'expression': '${' + f'{neg_diff_expression}' + '}'}
    ]

    wiretap_1 = pipeline_builder.add_wiretap()
    wiretap_2 = pipeline_builder.add_wiretap()

    dev_data_generator >> [expression_evaluator, wiretap_1.destination]
    expression_evaluator >> wiretap_2.destination

    pipeline = pipeline_builder.build()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    input_records = wiretap_1.output_records
    output_records = wiretap_2.output_records
    assert len(output_records) == len(input_records)
    assert input_records[0].field['field'] == output_records[0].field['field']
    assert output_records[0].field['transformed3'] == date_diff_value
    assert output_records[0].field['transformed4'] == -date_diff_value
