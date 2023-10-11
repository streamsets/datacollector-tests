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

from streamsets.sdk.exceptions import StartError
from streamsets.testframework.decorators import stub


KEYSTORE_FILE_PATH = 'resources/tls/keystore.jks'
KEYSTORE_TYPE = 'JKS'
KEYSTORE_PASSWORD = 'password'


@stub
def test_batch_wait_time_in_secs(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_default_cipher_suites': False, 'use_tls': True}])
def test_cipher_suites(sdc_builder, sdc_executor, stage_attributes):
    pass


@pytest.mark.parametrize('stage_attributes', [{'use_tls': True, 'keystore_file': KEYSTORE_FILE_PATH},
                                              {'use_tls': True, 'keystore_file': 'wrong/path/file.jks'}])
def test_keystore_file(sdc_builder, sdc_executor, stage_attributes):
    """Test "KeyStore path" config parameter. It is tested with two values, one pointing to a real KeyStore file
    and the other to an unexisting file. We check a TLS_01 error is raised for the unexisting file and that
    the pipeline successfully transitions to RUNNING state if the file exists.

    Pipeline:
      sdc_rpc >> trash

    """
    builder = sdc_builder.get_pipeline_builder()
    sdc_rpc = builder.add_stage('Dev SDC RPC with Buffering')
    sdc_rpc.set_attributes(keystore_type=KEYSTORE_TYPE,
                           keystore_password=KEYSTORE_PASSWORD,
                           sdc_rpc_id='admin',  # Whatever value is OK for the purpose of this test.
                           **stage_attributes)
    trash = builder.add_stage('Trash')
    sdc_rpc >> trash

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    if stage_attributes['keystore_file'] == KEYSTORE_FILE_PATH:
        # Expecting SDC loads the KeyStore and successfully starts to run the pipeline.
        sdc_executor.start_pipeline(pipeline).wait_for_status(status='RUNNING')
        sdc_executor.stop_pipeline(pipeline)
    else:
        # Expecting a StartError from SDC due to unexisting KeyStore file (TLS_01 error).
        with pytest.raises(StartError) as e:
            sdc_executor.start_pipeline(pipeline).wait_for_status(status='RUNNING')
        assert e.value.message.startswith('TLS_01')


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True}])
def test_keystore_key_algorithm(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True}])
def test_keystore_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'keystore_type': 'JKS', 'use_tls': True},
                                              {'keystore_type': 'PKCS12', 'use_tls': True}])
def test_keystore_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_max_disk_buffer_in_mb(sdc_builder, sdc_executor):
    pass


@stub
def test_max_fragments_in_memory(sdc_builder, sdc_executor):
    pass


@stub
def test_max_record_size_in_mb(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_sdc_rpc_id(sdc_builder, sdc_executor):
    pass


@stub
def test_sdc_rpc_listening_port(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_default_protocols': False, 'use_tls': True}])
def test_transport_protocols(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_default_cipher_suites': False, 'use_tls': True},
                                              {'use_default_cipher_suites': True, 'use_tls': True}])
def test_use_default_cipher_suites(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_default_protocols': False, 'use_tls': True},
                                              {'use_default_protocols': True, 'use_tls': True}])
def test_use_default_protocols(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': False}, {'use_tls': True}])
def test_use_tls(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_wait_time_for_empty_batches_in_millisecs(sdc_builder, sdc_executor):
    pass
