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

import pytest
from streamsets.testframework import sdc

logger = logging.getLogger(__name__)


@pytest.fixture(scope='module')
def data_collector():
    data_collector = sdc.DataCollector()
    data_collector.add_user('arvind', roles=['admin'])
    data_collector.add_user('girish', roles=['admin'])
    data_collector.start()
    yield data_collector
    if data_collector.tear_down_on_exit:
        data_collector.tear_down()


def test_pipeline_el_user(random_expression_pipeline_builder, data_collector):
    random_expression_pipeline_builder.expression_evaluator.header_attribute_expressions = [
        {'attributeToSet': 'user',
        'headerAttributeExpression': '${pipeline:user()}'}
    ]
    pipeline = random_expression_pipeline_builder.pipeline_builder.build()
    data_collector.add_pipeline(pipeline)

    # Run the pipeline as one user.
    data_collector.set_user('arvind')
    snapshot = data_collector.capture_snapshot(pipeline, start_pipeline=True).snapshot
    data_collector.stop_pipeline(pipeline)

    record = snapshot[random_expression_pipeline_builder.expression_evaluator.instance_name].output[0]
    assert record.header['user'] == 'arvind'

    # And then try different user.
    data_collector.set_user('girish')
    snapshot = data_collector.capture_snapshot(pipeline, start_pipeline=True).snapshot
    data_collector.stop_pipeline(pipeline)

    record = snapshot[random_expression_pipeline_builder.expression_evaluator.instance_name].output[0]
    assert record.header['user'] == 'girish'


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
    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)

    record = snapshot[random_expression_pipeline_builder.expression_evaluator.instance_name].output[0]
    assert record.header['name'] == pipeline.id
    assert record.header['id'] == pipeline.id
    assert record.header['title'] == pipeline.title
    assert record.header['version'] == '42'


def test_str_unescape_and_replace_el(sdc_builder, sdc_executor):
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'TEXT'
    dev_raw_data_source.raw_data = 'here\nis\tsome\ndata'
    dev_raw_data_source.use_custom_delimiter = True
    dev_raw_data_source.custom_delimiter = '^^^'

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.field_expressions = [
        {'fieldToSet': '/transformed',
         'expression': '${str:replace(record:value("/text"), str:unescapeJava("\\\\n"), "<NEWLINE>")}'},
        {'fieldToSet': '/transformed2',
         'expression': '${str:replace(record:value("/transformed"), str:unescapeJava("\\\\t"), "<TAB>")}'}
    ]

    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> expression_evaluator >> trash
    pipeline = pipeline_builder.build()

    sdc_executor.add_pipeline(pipeline)
    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    input_records = snapshot[dev_raw_data_source.instance_name].output
    output_records = snapshot[expression_evaluator.instance_name].output
    assert len(output_records) == len(input_records)
    assert input_records[0].value['value']['text']['value'] == 'here\nis\tsome\ndata'
    assert output_records[0].value['value']['text']['value'] == 'here\nis\tsome\ndata'
    assert output_records[0].value['value']['transformed']['value'] == 'here<NEWLINE>is\tsome<NEWLINE>data'
    assert output_records[0].value['value']['transformed2']['value'] == 'here<NEWLINE>is<TAB>some<NEWLINE>data'
