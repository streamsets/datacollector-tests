# Copyright 2017 StreamSets Inc.
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The tests in this module follow a pattern of creating pipelines with
:py:obj:`testframework.sdc_models.PipelineBuilder` in one version of SDC and then importing and running them in
another.
"""

import json
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# pylint: disable=pointless-statement, too-many-locals


def test_json_parser(sdc_builder, sdc_executor):
    """Test JSON parser processor. We also test removal of ASCII control characters.
    The pipeline would look like:

        dev_raw_data_source >> json_parser >> trash
    """
    result_field = 'result'
    result_key = 'content'
    raw_data = f'{{ "{result_key}" : "A\\u0001\\r\\n\\u000C B\\r\\n C" }}' # induce some control characters
    # remove ASCII control characters in the expected result
    expected_dict = json.loads(raw_data.encode('ascii', 'ignore').decode())

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data)
    json_parser = pipeline_builder.add_stage('JSON Parser', type='processor')
    json_parser.set_attributes(field_to_parse='/text', ignore_control_chars=True, target_field=f'/{result_field}')
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> json_parser >> trash
    pipeline = pipeline_builder.build('JSON parser pipeline')
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
    sdc_executor.stop_pipeline(pipeline)

    new_value = snapshot[json_parser.instance_name].output[0].value['value'][result_field]['value']
    assert expected_dict[result_key] == new_value[result_key]['value']
