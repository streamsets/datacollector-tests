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

import base64
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# pylint: disable=pointless-statement, too-many-locals


def test_base64_field_decoder(sdc_builder, sdc_executor):
    """Test Base64 Field Decoder processor. Since this processor accepts a Base64 encoded byte array, we use
    intermediate Field Type Converter processor for converting our Base64 string to byte array.
    The pipeline would look like:

        dev_raw_data_source >> field_type_converter >> base64_field_decoder >> trash
    """
    # input raw_data is a Base64 encoded string
    raw_data = base64.b64encode('hello there!'.encode()).decode()

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data)
    field_type_converter = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter.set_attributes(conversion_method='BY_FIELD',
                                        field_type_converter_configs=[{'fields': ['/text'],
                                                                       'targetType': 'BYTE_ARRAY'}])
    base64_field_decoder = pipeline_builder.add_stage('Base64 Field Decoder', type='processor')
    base64_field_decoder.set_attributes(field_to_decode='/text', target_field='/result')
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> field_type_converter >> base64_field_decoder >> trash
    pipeline = pipeline_builder.build('Base64 Decoder pipeline')
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)

    result_data = snapshot[base64_field_decoder.instance_name].output[0].value['value']['result']['value']
    # result data is Base64 encoded for JSON transport, hence we can directly compare to our raw Base64 string
    assert raw_data == result_data


def test_base64_field_encoder(sdc_builder, sdc_executor):
    """Test Base64 Field Encoder processor. Since this processor accepts a byte array, we use a Field Type
    Converter processor which will help convert the raw input string to byte array.
    The pipeline would look like:

        dev_raw_data_source >> field_type_converter >> base64_field_encoder >> trash
    """
    raw_data = 'hello there!'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data)
    field_type_converter = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter.set_attributes(conversion_method='BY_FIELD',
                                        field_type_converter_configs=[
                                            {'fields': ['/text'], 'targetType': 'BYTE_ARRAY'}
                                        ])
    base64_field_encoder = pipeline_builder.add_stage('Base64 Field Encoder', type='processor')
    base64_field_encoder.set_attributes(field_to_encode='/text', target_field='/result', url_safe=True)
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> field_type_converter >> base64_field_encoder >> trash
    pipeline = pipeline_builder.build('Base64 Encoder pipeline')
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)

    result_data = snapshot[base64_field_encoder.instance_name].output[0].value['value']['result']['value']
    # result_data is Base64 encoded by the Base64 encoder stage and for JSON transport it is again encoded, hence
    # we encode our raw_data twice for assertion
    assert base64.b64encode(base64.b64encode(raw_data.encode())).decode() == result_data
