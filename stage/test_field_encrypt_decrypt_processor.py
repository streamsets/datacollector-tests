# Copyright 2019 StreamSets Inc.
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
import json
import logging

from streamsets.testframework.markers import aws, sdc_min_version

logger = logging.getLogger(__name__)

MESSAGE_TEXT = 'ABCDEF'


@aws('kms')
@sdc_min_version('3.5.0')
def test_field_decrypt(sdc_builder, sdc_executor, aws):
    """Basic test to verify Encrypt and Decrypt Fields processor can decrypt a field.
    An encrypted field is sent and after pipeline is run, verification of decryption is done using snapshot.

    ciphertext is a byte array, but raw data source provides no way to specify a byte array.
    Hence a base64 encoded string of the ciphertext is used.
    Once it has been loaded by the raw data source, it needs to be decoded back into a byte array
    for input to the encryption processor.
    The base64 decode processor requires a byte array to decode instead of a string,
    hence the field type converter.
    (https://streamsets.com/documentation/datacollector/latest/help/datacollector/UserGuide/Processors/Base64Decoder.html#concept_ujj_spy_kv)

    The pipeline looks like:
        dev_raw_data_source >> field_type_converter >> base64_decoder >> field_decrypt >> trash
    """
    expected_plaintext = MESSAGE_TEXT.encode()

    ciphertext, _ = aws.encrypt(expected_plaintext)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=json.dumps({'message': base64.b64encode(ciphertext).decode()}),
                                       stop_after_first_batch=True)

    field_type_converter = pipeline_builder.add_stage('Field Type Converter', type='processor')
    field_type_converter_configs = [{'fields': ['/message'], 'targetType': 'BYTE_ARRAY'}]
    field_type_converter.set_attributes(conversion_method='BY_FIELD',
                                        field_type_converter_configs=field_type_converter_configs)

    base64_decoder = pipeline_builder.add_stage('Base64 Field Decoder', type='processor')
    base64_decoder.set_attributes(field_to_decode='/message',
                                  target_field='/message')

    field_decrypt = pipeline_builder.add_stage('Encrypt and Decrypt Fields', type='processor')
    field_decrypt.set_attributes(cipher='ALG_AES_256_GCM_IV12_TAG16_HKDF_SHA384_ECDSA_P384',
                                 fields=['/message'],
                                 frame_size=4096,
                                 mode='DECRYPT')

    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> field_type_converter >> base64_decoder >> field_decrypt >> trash
    pipeline = pipeline_builder.build('Field Decryption Pipeline').configure_for_environment(aws)
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

    actual_value = snapshot[field_decrypt.instance_name].output[0].get_field_data('/message')
    assert actual_value == expected_plaintext


@aws('kms')
@sdc_min_version('3.5.0')
def test_field_encrypt(sdc_builder, sdc_executor, aws):
    """Baic test to verify Encrypt and Decrypt Fields processor can encrypt.
    Verify by decrypting the field received from pipeline snapshot.

    The pipeline looks like:
        dev_raw_data_source >> field_encrypt >> trash
    """
    expected_plaintext = MESSAGE_TEXT.encode()
    raw_data = json.dumps(dict(message=MESSAGE_TEXT))

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)

    field_encrypt = pipeline_builder.add_stage('Encrypt and Decrypt Fields', type='processor')
    field_encrypt.set_attributes(cipher='ALG_AES_256_GCM_IV12_TAG16_HKDF_SHA384_ECDSA_P384',
                                 data_key_caching=False,
                                 fields=['/message'],
                                 frame_size=4096,
                                 mode='ENCRYPT')
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> field_encrypt >> trash
    pipeline = pipeline_builder.build('Field Encryption Pipeline').configure_for_environment(aws)
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    ciphertext_encoded = snapshot[field_encrypt.instance_name].output[0].get_field_data('/message')

    # Decrypt received value using aws_encryption_sdk for verification purpose.
    actual_value, _ = aws.decrypt(ciphertext_encoded.value)
    assert actual_value == expected_plaintext
