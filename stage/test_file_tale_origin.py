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
import os
import string
import tempfile

from streamsets.testframework.utils import get_random_string

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

    sdc_executor.start_pipeline(files_pipeline).wait_for_pipeline_batch_count(10)  # generate some batches/files
    sdc_executor.stop_pipeline(files_pipeline)

    # 2nd pipeline which reads the files using File Tail stage
    pipeline_builder = sdc_builder.get_pipeline_builder()
    file_tail = pipeline_builder.add_stage('File Tail', type='origin')
    file_tail.set_attributes(data_format='TEXT',
                             file_to_tail=[{
                                 'fileRollMode': 'ALPHABETICAL',
                                 'fileFullPath': f'{tmp_directory}/*'
                             }])
    wiretap_1 = pipeline_builder.add_wiretap()
    wiretap_2 = pipeline_builder.add_wiretap()

    file_tail >> wiretap_1.destination
    file_tail >> wiretap_2.destination
    file_tail_pipeline = pipeline_builder.build('File Tail Origin pipeline')
    sdc_executor.add_pipeline(file_tail_pipeline)

    sdc_executor.start_pipeline(file_tail_pipeline)
    sdc_executor.wait_for_pipeline_metric(file_tail_pipeline, 'data_batch_count', 1)
    sdc_executor.stop_pipeline(file_tail_pipeline)

    # assert all the data captured have the same raw_data
    # the snapshot output has a dict of {key: Record(s), key: EventRecord} Iterate and assert only Record(s)
    # by checking a Record having a key called 'text'
    for record in wiretap_1.output_records:
        if 'text' in record.field:
            assert raw_data == record.field['text'].value
    for record in wiretap_2.output_records:
        if 'text' in record.field:
            assert raw_data == record.field['text'].value


def test_file_tale_origin_stop_continue(sdc_builder, sdc_executor):
    """Test File Tail Origin. We test by making sure files are pre-created using Local FS destination stage pipeline
    and then have the File Tail Origin read those files. The pipelines looks like:

        dev_raw_data_source >> local_fs

        file_tail >> trash
    """
    raw_data = 'Hello!\n' * 10
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))

    # 1st pipeline which generates the required files for Directory Origin
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data, stop_after_first_batch=True)
    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='TEXT', directory_template=tmp_directory,
                            files_prefix='sdc-', max_records_in_file=100)

    dev_raw_data_source >> local_fs
    files_pipeline = pipeline_builder.build('Generate file for start-stop')
    sdc_executor.add_pipeline(files_pipeline)
    sdc_executor.start_pipeline(files_pipeline)

    # 2nd pipeline which reads the files using File Tail stage
    pipeline_builder = sdc_builder.get_pipeline_builder()
    file_tail = pipeline_builder.add_stage('File Tail', type='origin')
    file_tail.set_attributes(data_format='TEXT',
                             file_to_tail=[{
                                 'fileRollMode': 'ALPHABETICAL',
                                 'fileFullPath': f'{tmp_directory}/*'
                             }])
    wiretap_1 = pipeline_builder.add_wiretap()
    wiretap_2 = pipeline_builder.add_wiretap()

    file_tail >> wiretap_1.destination
    file_tail >> wiretap_2.destination

    file_tail_pipeline = pipeline_builder.build('File Tail Origin pipeline')
    sdc_executor.add_pipeline(file_tail_pipeline)

    sdc_executor.start_pipeline(file_tail_pipeline)
    sdc_executor.wait_for_pipeline_metric(file_tail_pipeline, 'data_batch_count', 1)
    sdc_executor.stop_pipeline(file_tail_pipeline)

    # assert all the data captured have the same raw_data
    # the snapshot output has a dict of {key: Record(s), key: EventRecord} Iterate and assert only Record(s)
    # by checking a Record having a key called 'text'

    size_output = 0

    for record in wiretap_1.output_records:
        if 'text' in record.field:
            assert 'Hello!' == record.field['text'].value
            size_output += 1
    for record in wiretap_2.output_records:
        if 'text' in record.field:
            assert 'Hello!' == record.field['text'].value
            size_output += 1

    assert size_output == 10

    raw_data = 'Bye!\n' * 10
    # 2n pipeline which generates the required files for Directory Origin
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data, stop_after_first_batch=True)
    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='TEXT', directory_template=tmp_directory,
                            files_prefix='sdc-', max_records_in_file=100)

    dev_raw_data_source >> local_fs
    files_pipeline_2 = pipeline_builder.build('Generate file for start-stop 2')
    sdc_executor.add_pipeline(files_pipeline_2)
    sdc_executor.start_pipeline(files_pipeline_2).wait_for_finished()

    wiretap_1.reset()
    wiretap_2.reset()
    sdc_executor.start_pipeline(file_tail_pipeline)
    sdc_executor.wait_for_pipeline_metric(file_tail_pipeline, 'data_batch_count', 1)
    sdc_executor.stop_pipeline(file_tail_pipeline)

    size_output = 0

    for record in wiretap_1.output_records:
        if 'text' in record.field:
            assert 'Bye!' == record.field['text'].value
            size_output += 1
    for record in wiretap_2.output_records:
        if 'text' in record.field:
            assert 'Bye!' == record.field['text'].value
            size_output += 1

    assert size_output == 10


def test_directory_origin_with_finisher(sdc_builder, sdc_executor):
    """Test File Tail Origin. We test by making sure files are pre-created using Local FS destination stage pipeline
    and then have the File Tail Origin read those files. A finisher stops the pipeline when the first batch is
    processed checking the event triggered is START.
    The pipelines looks like:

        dev_raw_data_source >> local_fs

        file_tail >> trash_1
        file_tail >> trash_2
        file_tail >= finisher
    """
    raw_data = 'Hello!\n' * 10
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))

    # 1st pipeline which generates the required files for Directory Origin
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data, stop_after_first_batch=True)
    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='TEXT', directory_template=tmp_directory,
                            files_prefix='sdc-', max_records_in_file=100)

    dev_raw_data_source >> local_fs

    files_pipeline = pipeline_builder.build('Generate files pipeline')
    sdc_executor.add_pipeline(files_pipeline)
    sdc_executor.start_pipeline(files_pipeline).wait_for_finished()

    # 2nd pipeline which reads the files using File Tail stage
    pipeline_builder = sdc_builder.get_pipeline_builder()
    file_tail = pipeline_builder.add_stage('File Tail', type='origin')
    file_tail.set_attributes(data_format='TEXT',
                             file_to_tail=[{
                                 'fileRollMode': 'ALPHABETICAL',
                                 'fileFullPath': f'{tmp_directory}/*'
                             }])
    wiretap_1 = pipeline_builder.add_wiretap()
    wiretap_2 = pipeline_builder.add_wiretap()
    events_wiretap = pipeline_builder.add_wiretap()

    # The event checked is START
    finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    finisher.set_attributes(stage_record_preconditions=["${record:eventType() == 'START'}"])

    file_tail >> wiretap_1.destination
    file_tail >> wiretap_2.destination
    file_tail >= [finisher, events_wiretap.destination]

    file_tail_pipeline = pipeline_builder.build('File Tail Origin with Finisher')
    sdc_executor.add_pipeline(file_tail_pipeline)

    sdc_executor.start_pipeline(file_tail_pipeline).wait_for_finished()

    # assert all the data captured have the same raw_data
    # the snapshot output has a dict of {key: Record(s), key: EventRecord} Iterate and assert only Record(s)
    # by checking a Record having a key called 'text'
    for record in wiretap_1.output_records:
        if 'text' in record.field:
            assert 'Hello!' == record.field['text'].value
    for record in wiretap_2.output_records:
        if 'text' in record.field:
            assert 'Hello!' == record.field['text'].value

    # assert the event generated is start
    assert len(events_wiretap.output_records) == 1
    assert events_wiretap.output_records[0].get_field_data('/event') == 'START'
    assert str(events_wiretap.output_records[0].get_field_data('/fileName'))[0:20] == f'{tmp_directory}/sdc-'
