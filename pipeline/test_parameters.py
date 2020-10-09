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

logger = logging.getLogger(__name__)


def test_pipeline_start_with_parameters(sdc_builder, sdc_executor):
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_random_record_source = pipeline_builder.add_stage('Dev Random Record Source')
    dev_random_record_source.fields_to_generate = '${fields}'

    field_renamer = pipeline_builder.add_stage('Field Renamer')
    field_renamer.fields_to_rename = [{'fromFieldExpression': '${fromField}',
                                       'toFieldExpression': '${toField}'}]

    trash = pipeline_builder.add_stage('Trash')

    dev_random_record_source >> field_renamer >> trash

    pipeline = pipeline_builder.build()
    pipeline.add_parameters(fields='f1,f2,f3', fromField='/f1', toField='/f1changed')
    sdc_executor.add_pipeline(pipeline)

    runtime_parameters = {'fields': 'x', 'fromField': '/x', 'toField': '/changedField'}
    sdc_executor.start_pipeline(pipeline, runtime_parameters)

    # Verify running pipeline's status.
    pipeline_status = sdc_executor.get_pipeline_status(pipeline).response.json()
    assert pipeline_status['attributes']['RUNTIME_PARAMETERS']['toField'] == '/changedField'

    sdc_executor.stop_pipeline(pipeline)


# SDC-15923: NullPointerException if using str:regExCapture in Pipeline parameters
def test_el_using_variables(sdc_builder, sdc_executor):
    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source')
    source.data_format = 'JSON'
    source.raw_data = '{"id":1}'
    source.stop_after_first_batch = True

    evaluator = builder.add_stage('Expression Evaluator')
    evaluator.field_expressions = [{
        'fieldToSet': '/out',
        'expression': "${PARAM}"
    }]

    wiretap = builder.add_wiretap()

    source >> evaluator >> wiretap.destination

    pipeline = builder.build()
    pipeline.add_parameters(PARAM='${str:regExCapture("argument1 false","f.+",0)}')

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline)

    records = wiretap.output_records
    assert len(records) == 1

    assert records[0].field['out'] == "false"

