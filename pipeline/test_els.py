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
from datetime import datetime

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
