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

"""The tests in this module follow a pattern of creating pipelines with
:py:obj:`testframework.sdc_models.PipelineBuilder` in one version of SDC and then importing and running them in
another.
"""

import logging
import os
import string
import tempfile

from testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# pylint: disable=pointless-statement, too-many-locals


def test_directory_origin(sdc_builder, sdc_executor):
    """Test File Tail Origin. We test by making sure files are pre-created using Local FS destination stage pipeline
    and then have the File Tail Origin read those files. The pipelines looks like:

        dev_raw_data_source >> local_fs

        file_tail >> trash_1
        file_tail >> trash_2
    """
    raw_data = 'Hello!'
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))

    # 1st pipeline which generates the required files for Directory Origin
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data)
    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='TEXT', directory_template=tmp_directory,
                            files_prefix='sdc-', max_records_in_file=100)

    dev_raw_data_source >> local_fs
    files_pipeline = pipeline_builder.build('Generate files pipeline')
    sdc_executor.add_pipeline(files_pipeline)

    sdc_executor.start_pipeline(files_pipeline).wait_for_pipeline_batch_count(10) # generate some batches/files
    sdc_executor.stop_pipeline(files_pipeline)

    # 2nd pipeline which reads the files using File Tail stage
    pipeline_builder = sdc_builder.get_pipeline_builder()
    file_tail = pipeline_builder.add_stage('File Tail', type='origin')
    file_tail.set_attributes(data_format='TEXT',
                             file_to_tail=[{
                                 'fileRollMode': 'ALPHABETICAL',
                                 'fileFullPath': f'{tmp_directory}/*'
                             }])
    trash_1 = pipeline_builder.add_stage('Trash')
    trash_2 = pipeline_builder.add_stage('Trash')

    file_tail >> trash_1
    file_tail >> trash_2
    file_tail_pipeline = pipeline_builder.build('File Tail Origin pipeline')
    sdc_executor.add_pipeline(file_tail_pipeline)

    snapshot = sdc_executor.capture_snapshot(file_tail_pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(file_tail_pipeline)

    # assert all the data captured have the same raw_data
    # the snapshot output has a dict of {key: Record(s), key: EventRecord} Iterate and assert only Record(s)
    # by checking a Record having a key called 'text'
    for value in snapshot.snapshot_batches[0][file_tail.instance_name].output_lanes.values():
        for record in value:
            if 'text' in record.value['value']:
                assert raw_data == record.value['value']['text']['value']
