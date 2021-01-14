# Copyright 2021 StreamSets Inc.
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
import pytest

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'master_key_provider': 'AWS_KMS'}])
def test_access_key_id(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'master_key_provider': 'USER'}])
def test_base64_encoded_key(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_key_caching': True, 'mode': 'ENCRYPT'}])
def test_cache_capacity(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'cipher': 'ALG_AES_128_GCM_IV12_TAG16_HKDF_SHA256'},
                                              {'cipher': 'ALG_AES_128_GCM_IV12_TAG16_HKDF_SHA256_ECDSA_P256'},
                                              {'cipher': 'ALG_AES_128_GCM_IV12_TAG16_NO_KDF'},
                                              {'cipher': 'ALG_AES_192_GCM_IV12_TAG16_HKDF_SHA256'},
                                              {'cipher': 'ALG_AES_192_GCM_IV12_TAG16_HKDF_SHA384_ECDSA_P384'},
                                              {'cipher': 'ALG_AES_192_GCM_IV12_TAG16_NO_KDF'},
                                              {'cipher': 'ALG_AES_256_GCM_IV12_TAG16_HKDF_SHA256'},
                                              {'cipher': 'ALG_AES_256_GCM_IV12_TAG16_HKDF_SHA384_ECDSA_P384'},
                                              {'cipher': 'ALG_AES_256_GCM_IV12_TAG16_NO_KDF'}])
def test_cipher(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_key_caching': False, 'mode': 'ENCRYPT'},
                                              {'data_key_caching': True, 'mode': 'ENCRYPT'}])
def test_data_key_caching(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'ENCRYPT'}])
def test_encryption_context_in_aad(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_frame_size(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'master_key_provider': 'USER'}])
def test_key_id_in_optional(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'master_key_provider': 'AWS_KMS'}])
def test_kms_key_arn(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'master_key_provider': 'AWS_KMS'}, {'master_key_provider': 'USER'}])
def test_master_key_provider(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_key_caching': True, 'mode': 'ENCRYPT'}])
def test_max_bytes_per_data_key(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_key_caching': True, 'mode': 'ENCRYPT'}])
def test_max_data_key_age(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_key_caching': True, 'mode': 'ENCRYPT'}])
def test_max_records_per_data_key(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'DECRYPT'}, {'mode': 'ENCRYPT'}])
def test_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'master_key_provider': 'AWS_KMS'}])
def test_secret_access_key(sdc_builder, sdc_executor, stage_attributes):
    pass

