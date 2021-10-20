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
from streamsets.testframework.utils import Version
from streamsets.testframework.markers import sdc_min_version

logger = logging.getLogger(__name__)

# pylint: disable=pointless-statement, too-many-locals


def test_base64_field_decoder(sdc_builder, sdc_executor):
    """Test Base64 Field Decoder processor. Since this processor accepts a Base64 encoded byte array, we use
    intermediate Field Type Converter processor for converting our Base64 string to byte array.
    The pipeline would look like:

        dev_raw_data_source >> field_type_converter >> base64_field_decoder >> wiretap
    """
    # input raw_data is a Base64 encoded string
    normal_string = 'hello there!'.encode()
    raw_data = base64.b64encode(normal_string).decode()

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data, stop_after_first_batch=True)
    field_type_converter = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter.set_attributes(conversion_method='BY_FIELD',
                                        field_type_converter_configs=[{'fields': ['/text'],
                                                                       'targetType': 'BYTE_ARRAY'}])
    base64_field_decoder = pipeline_builder.add_stage('Base64 Field Decoder', type='processor')
    if Version(sdc_builder.version) < Version("4.3.0"):
        base64_field_decoder.set_attributes(field_to_decode='/text', target_field='/result')
    else:
        base64_field_decoder.set_attributes(
            fields_to_decode=[{'originFieldPath': '/text', 'resultFieldPath': '/result'}]
        )

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_type_converter >> base64_field_decoder >> wiretap.destination
    pipeline = pipeline_builder.build('Base64 Decoder pipeline')
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    result_data = wiretap.output_records[0].field['result'].value
    # result data is Base64 encoded for JSON transport, hence we can directly compare to our raw Base64 string
    assert normal_string == result_data


def test_base64_field_encoder(sdc_builder, sdc_executor):
    """Test Base64 Field Encoder processor. Since this processor accepts a byte array, we use a Field Type
    Converter processor which will help convert the raw input string to byte array.
    The pipeline would look like:

        dev_raw_data_source >> field_type_converter >> base64_field_encoder >> wiretap
    """
    raw_data = 'hello there!'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data, stop_after_first_batch=True)
    field_type_converter = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter.set_attributes(conversion_method='BY_FIELD',
                                        field_type_converter_configs=[
                                            {'fields': ['/text'], 'targetType': 'BYTE_ARRAY'}
                                        ])
    base64_field_encoder = pipeline_builder.add_stage('Base64 Field Encoder', type='processor')
    if Version(sdc_builder.version) < Version("4.3.0"):
        base64_field_encoder.set_attributes(field_to_encode='/text', target_field='/result', url_safe=True)
    else:
        base64_field_encoder.set_attributes(
            fields_to_encode=[{'originFieldPath': '/text', 'resultFieldPath': '/result'}],
            url_safe=True
        )

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_type_converter >> base64_field_encoder >> wiretap.destination
    pipeline = pipeline_builder.build('Base64 Encoder pipeline')
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    result_data = wiretap.output_records[0].field['result'].value
    # result_data is Base64 encoded by the Base64 encoder stage and for JSON transport it is again encoded, hence
    # we encode our raw_data twice for assertion
    assert base64.b64encode(raw_data.encode()) == result_data


@sdc_min_version("4.3.0")
def test_base64_field_decoder_multiple_fields(sdc_builder, sdc_executor):
    """ Base64 encode multiple fields """
    f1_val = base64.b64encode('field_val1'.encode()).decode()
    f2_val = base64.b64encode('field_val2'.encode()).decode()
    raw_data = f"{{\"f1\": \"{f1_val}\", \"f2\": \"{f2_val}\"}}"

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)
    field_type_converter = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter.set_attributes(conversion_method='BY_FIELD',
                                        field_type_converter_configs=[
                                            {'fields': ['/f1', '/f2'], 'targetType': 'BYTE_ARRAY'}
                                        ])
    base64_field_decoder = pipeline_builder.add_stage('Base64 Field Decoder', type='processor')
    base64_field_decoder.set_attributes(
        fields_to_decode=[
            {'originFieldPath': '/f1', 'resultFieldPath': '/dec_f1'},
            {'originFieldPath': '/f2', 'resultFieldPath': '/dec_f2'}
        ]
    )

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_type_converter >> base64_field_decoder >> wiretap.destination

    pipeline = pipeline_builder.build('Base64 Decoder pipeline')
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    # result data is Base64 encoded for JSON transport, hence we can directly compare to our raw Base64 string
    assert "field_val1".encode() == wiretap.output_records[0].field['dec_f1'].value
    assert "field_val2".encode() == wiretap.output_records[0].field['dec_f2'].value


@sdc_min_version("4.3.0")
def test_base64_field_encoder_multiple_fields(sdc_builder, sdc_executor):
    """ Base64 decode multiple fields """
    raw_data = "{\"f1\": \"field_val1\", \"f2\": \"field_val2\"}"

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)
    field_type_converter = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter.set_attributes(conversion_method='BY_FIELD',
                                        field_type_converter_configs=[
                                            {'fields': ['/f1', '/f2'], 'targetType': 'BYTE_ARRAY'}
                                        ])
    base64_field_encoder = pipeline_builder.add_stage('Base64 Field Encoder', type='processor')
    base64_field_encoder.set_attributes(
        fields_to_encode=[
            {'originFieldPath': '/f1', 'resultFieldPath': '/enc_f1'},
            {'originFieldPath': '/f2', 'resultFieldPath': '/enc_f2'}
        ],
        url_safe=False
    )

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_type_converter >> base64_field_encoder >> wiretap.destination
    pipeline = pipeline_builder.build('Base64 Encoder pipeline')
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    # result_data is Base64 encoded by the Base64 encoder stage and for JSON transport it is again encoded, hence
    # we encode our raw_data twice for assertion
    assert base64.b64encode("field_val1".encode()) == wiretap.output_records[0].field['enc_f1'].value
    assert base64.b64encode("field_val2".encode()) == wiretap.output_records[0].field['enc_f2'].value
