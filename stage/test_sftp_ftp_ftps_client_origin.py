# Copyright 2024 StreamSets Inc.
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
import os
import string

import pytest
from streamsets.testframework.markers import sftp, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

@sdc_min_version('5.9.0')
@sftp
def test_malformed_private_key(sdc_builder, sdc_executor, sftp):
    """We first create a file on SFTP server and have the SFTP
    origin stage read it using a malformed private key forcing it to fail.
    The pipelines look like:
        sftp_ftp_client >> trash
    """
    # Our origin SFTP file name
    sftp_file_name = get_random_string(string.ascii_letters, 10)
    raw_text_data = 'Hello World!'
    sftp.put_string(os.path.join(sftp.path, sftp_file_name), raw_text_data)

    builder = sdc_builder.get_pipeline_builder()
    sftp_origin = builder.add_stage(name='com_streamsets_pipeline_stage_origin_remote_RemoteDownloadDSource')
    sftp_origin.file_name_pattern = sftp_file_name
    sftp_origin.data_format = 'TEXT'

    trash = builder.add_stage('Trash')

    sftp_origin >> trash

    pipeline = builder.build('SFTP Origin Pipeline - Authentication').configure_for_environment(sftp)
    # Add a malformed private key
    pipeline.stages[0].authentication = 'PRIVATE_KEY'
    pipeline.stages[0].private_key_provider = 'PLAIN_TEXT'
    pipeline.stages[0].private_key = \
        '-----BEGIN OPENSSH PRIVATE KEY-----  -----END OPENSSH PRIVATE KEY-----'

    sdc_executor.add_pipeline(pipeline)

    with pytest.raises(Exception) as error:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(1)
    assert "REMOTE_10" in error.value.message, f'Expected a REMOTE_10 error, got "{error.value.message}" instead'

    # Delete the test SFTP origin file we created
    transport, client = sftp.client
    client.remove(os.path.join(sftp.path, sftp_file_name))
    client.close()
    transport.close()