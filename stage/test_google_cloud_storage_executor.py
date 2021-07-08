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

import json;
import logging
import pytest
import string
import collections

from streamsets.sdk import sdc_api
from streamsets.testframework.markers import gcp, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

@gcp
@sdc_min_version('4.1.0')
@pytest.mark.parametrize('with_bucket', [True, False])
@pytest.mark.parametrize('with_el', [True, False])
def test_google_cloud_storage_executor_create_object(sdc_builder, sdc_executor, gcp, with_bucket, with_el):
    """Test for Google Cloud Storage Executor stage, operation Create Object.
    """

    bucket_name_prefix = 'stf_gcse_b_'
    bucket_name = f'{bucket_name_prefix}{get_random_string(string.ascii_lowercase, 10)}'

    object_name_prefix = 'stf_gcse_o_'
    object_name = f'{object_name_prefix}{get_random_string(string.ascii_lowercase, 10)}'

    object_content = f'In a hole in the ground there lived a Hobbit'

    metadata = {'Dwarf':   'Gimli',
                'Dunadan': 'Aragorn',
                'Elf':     'Legolas',
                'Hobbit':  'Frodo',
                'Human':   'Boromir',
                'Wizard':  'Gandalf'}
    object_metadata = {}
    i = 1
    for key in metadata:
        object_metadata[f'k{i:0>2d}'] = key
        object_metadata[f'v{i:0>2d}'] = metadata[key]
        i = i + 1

    raw_data_map = {'bucket': bucket_name,
                    'object': object_name,
                    'content': object_content,
                    'metadata': object_metadata}
    raw_data = json.dumps(raw_data_map)

    if with_el:
        bucket_specification = '${record:value("/bucket")}'
        object_specification = '${record:value("/object")}'
        content_specification = '${record:value("/content")}'
        metadata_specification = [{'key': '${record:value("/metadata/k01")}', 'value': '${record:value("/metadata/v01")}'},
                                  {'key': '${record:value("/metadata/k02")}', 'value': '${record:value("/metadata/v02")}'},
                                  {'key': '${record:value("/metadata/k03")}', 'value': '${record:value("/metadata/v03")}'},
                                  {'key': '${record:value("/metadata/k04")}', 'value': '${record:value("/metadata/v04")}'},
                                  {'key': '${record:value("/metadata/k05")}', 'value': '${record:value("/metadata/v05")}'},
                                  {'key': '${record:value("/metadata/k06")}', 'value': '${record:value("/metadata/v06")}'}]
    else:
        bucket_specification = bucket_name
        object_specification = object_name
        content_specification = object_content
        metadata_specification = []
        for key in metadata:
            metadata_specification.append({'key': key, 'value': metadata[key]})

    pipeline_name = f'{get_random_string(string.ascii_letters, 10)}'
    pipeline_title = f'Google Cloud Storage Test Pipeline - Create Object: {pipeline_name}'

    try:
        if with_bucket:
            logger.info(f'Creating temporary bucket {bucket_name}')
            bucket = gcp.retry_429(gcp.storage_client.create_bucket)(bucket_name)
            logger.info(f'Temporary bucket {bucket_name} successfully created')

        pipeline_builder = sdc_builder.get_pipeline_builder()

        dev_raw_data_source_origin = pipeline_builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source_origin.set_attributes(data_format='JSON',
                                                  raw_data=raw_data,
                                                  stop_after_first_batch=True)

        google_cloud_storage_executor = pipeline_builder.add_stage('Google Cloud Storage Executor', type='executor')
        google_cloud_storage_executor.set_attributes(task='CREATE_OBJECT',
                                                     project_id=gcp.project_id,
                                                     bucket=bucket_specification,
                                                     object=object_specification,
                                                     content=content_specification,
                                                     metadata=metadata_specification)

        wiretap = pipeline_builder.add_wiretap()

        dev_raw_data_source_origin >> google_cloud_storage_executor >= wiretap.destination

        pipeline = pipeline_builder.build(title=pipeline_title).configure_for_environment(gcp)
        sdc_executor.add_pipeline(pipeline)

        if with_bucket:
            sdc_executor.start_pipeline(pipeline).wait_for_finished()

            assert len(wiretap.error_records) == 0, \
                'This execution mode was not expected to produce error records'
            assert len(wiretap.output_records) == 1, \
                'Only one input record, so exactly one output even record was expected'
            assert wiretap.output_records[0].header.values['sdc.event.type'] == 'gcs_object_created', \
                'It was expected an event record signaling the object creation'

            existing_blob = bucket.get_blob(object_name)
            assert existing_blob is not None, f'Just created object ({object_name}) does not exist in this bucket'
            existing_object_content = existing_blob.download_as_string().decode('ascii')
            assert existing_object_content == object_content, 'Correct object but with unexpected contents'
            existing_object_metadata = collections.OrderedDict(sorted(existing_blob.metadata.items()))
            injected_object_metadata = collections.OrderedDict(sorted(metadata.items()))
            assert existing_object_metadata == injected_object_metadata, 'Correct object but with unexpected metadata'
        else:
            sdc_executor.start_pipeline(pipeline).wait_for_finished()
            assert len(wiretap.error_records) == 1, \
                'This execution mode was expected to produce error records'
            assert len(wiretap.output_records) == 0, \
                'This execution mode was not expected to produce records'
            assert wiretap.error_records[0].header['errorCode'] == 'GCSE_011', \
                'This execution mode was expected to produce error GCSE_011: <Error creating GCS object>'
    finally:
        if with_bucket:
            logger.info(f'Deleting temporary bucket {bucket_name}')
            gcp.retry_429(bucket.delete)(force=True)
            logger.info(f'Temporary bucket {bucket_name} successfully deleted')

@gcp
@sdc_min_version('4.1.0')
@pytest.mark.parametrize('with_bucket', [True, False])
@pytest.mark.parametrize('with_object', [True, False])
@pytest.mark.parametrize('with_el', [True, False])
def test_google_cloud_storage_executor_copy_object(sdc_builder, sdc_executor, gcp, with_bucket, with_object, with_el):
    """Test for Google Cloud Storage Executor stage, operation Copy Object.
    """

    bucket_name_prefix = 'stf_gcse_b_'
    source_bucket_name = f'{bucket_name_prefix}s_{get_random_string(string.ascii_lowercase, 10)}'
    target_bucket_name = f'{bucket_name_prefix}t_{get_random_string(string.ascii_lowercase, 10)}'

    object_name_prefix = 'stf_gcse_o_'
    source_object_name = f'{object_name_prefix}s_{get_random_string(string.ascii_lowercase, 10)}'
    target_object_name = f'{object_name_prefix}t_{get_random_string(string.ascii_lowercase, 10)}'

    object_content = f'In a hole in the ground there lived a Hobbit'

    source_metadata = {'Dwarf':   'Gimli',
                       'Dunadan': 'Aragorn',
                       'Elf':     'Legolas',
                       'Hobbit':  'Frodo',
                       'Human':   'Boromir',
                       'Wizard':  'Gandalf'}
    source_object_metadata = {}
    i_s = 1
    for key in source_metadata:
        source_object_metadata[f'k{i_s:0>2d}'] = key
        source_object_metadata[f'v{i_s:0>2d}'] = source_metadata[key]
        i_s = i_s + 1
    target_metadata = {'Honinbo': 'Takemiya Masaki',
                       'Judan':   'Kato Masao',
                       'Kisei':   'Cho Chikun',
                       'Meijin':  'Ishida Yoshio'}
    target_object_metadata = {}
    i_t = 1
    for key in target_metadata:
        target_object_metadata[f'k{i_t:0>2d}'] = key
        target_object_metadata[f'v{i_t:0>2d}'] = target_metadata[key]
        i_t = i_t + 1

    raw_data_map = {'source_bucket': source_bucket_name,
                    'source_object': source_object_name,
                    'target_bucket': target_bucket_name,
                    'target_object': target_object_name,
                    'target_metadata': target_object_metadata}
    raw_data = json.dumps(raw_data_map)

    if with_el:
        source_bucket_specification = '${record:value("/source_bucket")}'
        source_object_specification = '${record:value("/source_object")}'
        target_bucket_specification = '${record:value("/target_bucket")}'
        target_object_specification = '${record:value("/target_object")}'
        target_metadata_specification = [{'key': '${record:value("/target_metadata/k01")}', 'value': '${record:value("/target_metadata/v01")}'},
                                         {'key': '${record:value("/target_metadata/k02")}', 'value': '${record:value("/target_metadata/v02")}'},
                                         {'key': '${record:value("/target_metadata/k03")}', 'value': '${record:value("/target_metadata/v03")}'},
                                         {'key': '${record:value("/target_metadata/k04")}', 'value': '${record:value("/target_metadata/v04")}'}]
    else:
        source_bucket_specification = source_bucket_name
        source_object_specification = source_object_name
        target_bucket_specification = target_bucket_name
        target_object_specification = target_object_name
        target_metadata_specification = []
        for key in target_metadata:
            target_metadata_specification.append({'key': key, 'value': target_metadata[key]})

    pipeline_name = f'{get_random_string(string.ascii_letters, 10)}'
    pipeline_title = f'Google Cloud Storage Test Pipeline - Copy Object: {pipeline_name}'

    try:
        if with_bucket:
            logger.info(f'Creating temporary source bucket {source_bucket_name}')
            source_bucket = gcp.retry_429(gcp.storage_client.create_bucket)(source_bucket_name)
            logger.info(f'Temporary source bucket {source_bucket_name} successfully created')

            logger.info(f'Creating temporary target bucket {target_bucket_name}')
            target_bucket = gcp.retry_429(gcp.storage_client.create_bucket)(target_bucket_name)
            logger.info(f'Temporary target bucket {target_bucket_name} successfully created')

            if with_object:
                logger.info(f'Creating temporary source object {source_object_name}')
                existing_source_blob = source_bucket.blob(source_object_name)
                existing_source_blob.upload_from_string(object_content, content_type='text/plain')
                existing_source_blob.metadata = source_metadata
                existing_source_blob.patch()
                logger.info(f'Temporary source object {source_object_name} successfully created')

        pipeline_builder = sdc_builder.get_pipeline_builder()

        dev_raw_data_source_origin = pipeline_builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source_origin.set_attributes(data_format='JSON',
                                                  raw_data=raw_data,
                                                  stop_after_first_batch=True)

        google_cloud_storage_executor = pipeline_builder.add_stage('Google Cloud Storage Executor', type='executor')
        google_cloud_storage_executor.set_attributes(task='COPY_OBJECT',
                                                     project_id=gcp.project_id,
                                                     source_bucket=source_bucket_specification,
                                                     source_object=source_object_specification,
                                                     target_bucket=target_bucket_specification,
                                                     target_object=target_object_specification,
                                                     metadata=target_metadata_specification)

        wiretap = pipeline_builder.add_wiretap()

        dev_raw_data_source_origin >> google_cloud_storage_executor >= wiretap.destination

        pipeline = pipeline_builder.build(title=pipeline_title).configure_for_environment(gcp)
        sdc_executor.add_pipeline(pipeline)

        if with_bucket and with_object:
            sdc_executor.start_pipeline(pipeline).wait_for_finished()

            assert len(wiretap.error_records) == 0, \
                'This execution mode was not expected to produce error records'
            assert len(wiretap.output_records) == 1, \
                'Only one input record, so exactly one output even record was expected'
            assert wiretap.output_records[0].header.values['sdc.event.type'] == 'gcs_object_copied', \
                'It was expected an event record signaling the object creation'

            existing_source_blob = source_bucket.get_blob(source_object_name)
            assert existing_source_blob is not None, f'Just created source object ({source_object_name}) does not exist in this bucket'
            existing_source_object_content = existing_source_blob.download_as_string().decode('ascii')
            assert existing_source_object_content == object_content, 'Correct source object but with unexpected contents'
            existing_source_object_metadata = collections.OrderedDict(sorted(existing_source_blob.metadata.items()))
            injected_source_object_metadata = collections.OrderedDict(sorted(source_metadata.items()))
            assert existing_source_object_metadata == injected_source_object_metadata, 'Correct source object but with unexpected metadata'

            existing_target_blob = target_bucket.get_blob(target_object_name)
            assert existing_target_blob is not None, f'Just created target object ({target_object_name}) does not exist in this bucket'
            existing_target_object_content = existing_target_blob.download_as_string().decode('ascii')
            assert existing_target_object_content == object_content, 'Correct target object but with unexpected contents'
            existing_target_object_metadata = collections.OrderedDict(sorted(existing_target_blob.metadata.items()))
            injected_target_object_metadata = collections.OrderedDict(sorted({**source_metadata, **target_metadata}.items()))
            assert existing_target_object_metadata == injected_target_object_metadata, 'Correct target object but with unexpected metadata'
        else:
            sdc_executor.start_pipeline(pipeline).wait_for_finished()
            assert len(wiretap.error_records) == 1, \
                'This execution mode was expected to produce error records'
            assert len(wiretap.output_records) == 0, \
                'This execution mode was not expected to produce records'
            assert wiretap.error_records[0].header['errorCode'] == 'GCSE_012', \
                'This execution mode was expected to produce error GCSE_012: <Error copying GCS object>'
    finally:
        if with_bucket:
            logger.info(f'Deleting temporary source bucket {source_bucket_name}')
            gcp.retry_429(source_bucket.delete)(force=True)
            logger.info(f'Temporary source bucket {source_bucket_name} successfully deleted')

            logger.info(f'Deleting temporary target bucket {target_bucket_name}')
            gcp.retry_429(target_bucket.delete)(force=True)
            logger.info(f'Temporary target bucket {target_bucket_name} successfully deleted')

@gcp
@sdc_min_version('4.1.0')
@pytest.mark.parametrize('with_bucket', [True, False])
@pytest.mark.parametrize('with_object', [True, False])
@pytest.mark.parametrize('with_el', [True, False])
def test_google_cloud_storage_executor_move_object(sdc_builder, sdc_executor, gcp, with_bucket, with_object, with_el):
    """Test for Google Cloud Storage Executor stage, operation Move Object.
    """

    bucket_name_prefix = 'stf_gcse_b_'
    source_bucket_name = f'{bucket_name_prefix}s_{get_random_string(string.ascii_lowercase, 10)}'
    target_bucket_name = f'{bucket_name_prefix}t_{get_random_string(string.ascii_lowercase, 10)}'

    object_name_prefix = 'stf_gcse_o_'
    source_object_name = f'{object_name_prefix}s_{get_random_string(string.ascii_lowercase, 10)}'
    target_object_name = f'{object_name_prefix}t_{get_random_string(string.ascii_lowercase, 10)}'

    object_content = f'In a hole in the ground there lived a Hobbit'

    source_metadata = {'Dwarf':   'Gimli',
                       'Dunadan': 'Aragorn',
                       'Elf':     'Legolas',
                       'Hobbit':  'Frodo',
                       'Human':   'Boromir',
                       'Wizard':  'Gandalf'}
    source_object_metadata = {}
    i_s = 1
    for key in source_metadata:
        source_object_metadata[f'k{i_s:0>2d}'] = key
        source_object_metadata[f'v{i_s:0>2d}'] = source_metadata[key]
        i_s = i_s + 1
    target_metadata = {'Honinbo': 'Takemiya Masaki',
                       'Judan':   'Kato Masao',
                       'Kisei':   'Cho Chikun',
                       'Meijin':  'Ishida Yoshio'}
    target_object_metadata = {}
    i_t = 1
    for key in target_metadata:
        target_object_metadata[f'k{i_t:0>2d}'] = key
        target_object_metadata[f'v{i_t:0>2d}'] = target_metadata[key]
        i_t = i_t + 1

    raw_data_map = {'source_bucket': source_bucket_name,
                    'source_object': source_object_name,
                    'target_bucket': target_bucket_name,
                    'target_object': target_object_name,
                    'target_metadata': target_object_metadata}
    raw_data = json.dumps(raw_data_map)

    if with_el:
        source_bucket_specification = '${record:value("/source_bucket")}'
        source_object_specification = '${record:value("/source_object")}'
        target_bucket_specification = '${record:value("/target_bucket")}'
        target_object_specification = '${record:value("/target_object")}'
        target_metadata_specification = [{'key': '${record:value("/target_metadata/k01")}', 'value': '${record:value("/target_metadata/v01")}'},
                                         {'key': '${record:value("/target_metadata/k02")}', 'value': '${record:value("/target_metadata/v02")}'},
                                         {'key': '${record:value("/target_metadata/k03")}', 'value': '${record:value("/target_metadata/v03")}'},
                                         {'key': '${record:value("/target_metadata/k04")}', 'value': '${record:value("/target_metadata/v04")}'}]
    else:
        source_bucket_specification = source_bucket_name
        source_object_specification = source_object_name
        target_bucket_specification = target_bucket_name
        target_object_specification = target_object_name
        target_metadata_specification = []
        for key in target_metadata:
            target_metadata_specification.append({'key': key, 'value': target_metadata[key]})

    pipeline_name = f'{get_random_string(string.ascii_letters, 10)}'
    pipeline_title = f'Google Cloud Storage Test Pipeline - Move Object: {pipeline_name}'

    try:
        if with_bucket:
            logger.info(f'Creating temporary source bucket {source_bucket_name}')
            source_bucket = gcp.retry_429(gcp.storage_client.create_bucket)(source_bucket_name)
            logger.info(f'Temporary source bucket {source_bucket_name} successfully created')

            logger.info(f'Creating temporary target bucket {target_bucket_name}')
            target_bucket = gcp.retry_429(gcp.storage_client.create_bucket)(target_bucket_name)
            logger.info(f'Temporary target bucket {target_bucket_name} successfully created')

            if with_object:
                logger.info(f'Creating temporary source object {source_object_name}')
                existing_source_blob = source_bucket.blob(source_object_name)
                existing_source_blob.upload_from_string(object_content, content_type='text/plain')
                existing_source_blob.metadata = source_metadata
                existing_source_blob.patch()
                logger.info(f'Temporary source object {source_object_name} successfully created')

        pipeline_builder = sdc_builder.get_pipeline_builder()

        dev_raw_data_source_origin = pipeline_builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source_origin.set_attributes(data_format='JSON',
                                                  raw_data=raw_data,
                                                  stop_after_first_batch=True)

        google_cloud_storage_executor = pipeline_builder.add_stage('Google Cloud Storage Executor', type='executor')
        google_cloud_storage_executor.set_attributes(task='MOVE_OBJECT',
                                                     project_id=gcp.project_id,
                                                     source_bucket=source_bucket_specification,
                                                     source_object=source_object_specification,
                                                     target_bucket=target_bucket_specification,
                                                     target_object=target_object_specification,
                                                     metadata=target_metadata_specification)

        wiretap = pipeline_builder.add_wiretap()

        dev_raw_data_source_origin >> google_cloud_storage_executor >= wiretap.destination

        pipeline = pipeline_builder.build(title=pipeline_title).configure_for_environment(gcp)
        sdc_executor.add_pipeline(pipeline)

        if with_bucket and with_object:
            sdc_executor.start_pipeline(pipeline).wait_for_finished()

            assert len(wiretap.error_records) == 0, \
                'This execution mode was not expected to produce error records'
            assert len(wiretap.output_records) == 1, \
                'Only one input record, so exactly one output even record was expected'
            assert wiretap.output_records[0].header.values['sdc.event.type'] == 'gcs_object_moved', \
                'It was expected an event record signaling the object creation'

            existing_source_blob = source_bucket.get_blob(source_object_name)
            assert existing_source_blob is None, f'Just deleted source object ({source_object_name}) exists in this bucket'

            existing_target_blob = target_bucket.get_blob(target_object_name)
            assert existing_target_blob is not None, f'Just created target object ({target_object_name}) does not exist in this bucket'
            existing_target_object_content = existing_target_blob.download_as_string().decode('ascii')
            assert existing_target_object_content == object_content, 'Correct target object but with unexpected contents'
            existing_target_object_metadata = collections.OrderedDict(sorted(existing_target_blob.metadata.items()))
            injected_target_object_metadata = collections.OrderedDict(sorted({**source_metadata, **target_metadata}.items()))
            assert existing_target_object_metadata == injected_target_object_metadata, 'Correct target object but with unexpected metadata'
        else:
            sdc_executor.start_pipeline(pipeline).wait_for_finished()
            assert len(wiretap.error_records) == 1, \
                'This execution mode was expected to produce error records'
            assert len(wiretap.output_records) == 0, \
                'This execution mode was not expected to produce records'
            assert wiretap.error_records[0].header['errorCode'] == 'GCSE_013', \
                'This execution mode was expected to produce error GCSE_013: <Error moving GCS object>'
    finally:
        if with_bucket:
            logger.info(f'Deleting temporary source bucket {source_bucket_name}')
            gcp.retry_429(source_bucket.delete)(force=True)
            logger.info(f'Temporary source bucket {source_bucket_name} successfully deleted')

            logger.info(f'Deleting temporary target bucket {target_bucket_name}')
            gcp.retry_429(target_bucket.delete)(force=True)
            logger.info(f'Temporary target bucket {target_bucket_name} successfully deleted')

@gcp
@sdc_min_version('4.1.0')
@pytest.mark.parametrize('with_bucket', [True, False])
@pytest.mark.parametrize('with_object', [True, False])
@pytest.mark.parametrize('with_el', [True, False])
def test_google_cloud_storage_executor_change_object(sdc_builder, sdc_executor, gcp, with_bucket, with_object, with_el):
    """Test for Google Cloud Storage Executor stage, operation Change Object.
    """

    bucket_name_prefix = 'stf_gcse_b_'
    bucket_name = f'{bucket_name_prefix}{get_random_string(string.ascii_lowercase, 10)}'

    object_name_prefix = 'stf_gcse_o_'
    object_name = f'{object_name_prefix}{get_random_string(string.ascii_lowercase, 10)}'

    object_content = f'In a hole in the ground there lived a Hobbit'

    source_metadata = {'Dwarf':   'Gimli',
                       'Dunadan': 'Aragorn',
                       'Elf':     'Legolas',
                       'Hobbit':  'Frodo',
                       'Human':   'Boromir',
                       'Wizard':  'Gandalf'}
    source_object_metadata = {}
    i_s = 1
    for key in source_metadata:
        source_object_metadata[f'k{i_s:0>2d}'] = key
        source_object_metadata[f'v{i_s:0>2d}'] = source_metadata[key]
        i_s = i_s + 1
    target_metadata = {'Honinbo': 'Takemiya Masaki',
                       'Judan':   'Kato Masao',
                       'Kisei':   'Cho Chikun',
                       'Meijin':  'Ishida Yoshio'}
    target_object_metadata = {}
    i_t = 1
    for key in target_metadata:
        target_object_metadata[f'k{i_t:0>2d}'] = key
        target_object_metadata[f'v{i_t:0>2d}'] = target_metadata[key]
        i_t = i_t + 1

    raw_data_map = {'bucket': bucket_name,
                    'object': object_name,
                    'metadata': target_object_metadata}
    raw_data = json.dumps(raw_data_map)

    if with_el:
        bucket_specification = '${record:value("/bucket")}'
        object_specification = '${record:value("/object")}'
        metadata_specification = [{'key': '${record:value("/metadata/k01")}', 'value': '${record:value("/metadata/v01")}'},
                                  {'key': '${record:value("/metadata/k02")}', 'value': '${record:value("/metadata/v02")}'},
                                  {'key': '${record:value("/metadata/k03")}', 'value': '${record:value("/metadata/v03")}'},
                                  {'key': '${record:value("/metadata/k04")}', 'value': '${record:value("/metadata/v04")}'}]
    else:
        bucket_specification = bucket_name
        object_specification = object_name
        metadata_specification = []
        for key in target_metadata:
            metadata_specification.append({'key': key, 'value': target_metadata[key]})

    pipeline_name = f'{get_random_string(string.ascii_letters, 10)}'
    pipeline_title = f'Google Cloud Storage Test Pipeline - Change Object: {pipeline_name}'

    try:
        if with_bucket:
            logger.info(f'Creating temporary bucket {bucket_name}')
            bucket = gcp.retry_429(gcp.storage_client.create_bucket)(bucket_name)
            logger.info(f'Temporary bucket {bucket_name} successfully created')

            if with_object:
                logger.info(f'Creating temporary object {object_name}')
                existing_blob = bucket.blob(object_name)
                existing_blob.upload_from_string(object_content, content_type='text/plain')
                existing_blob.metadata = source_metadata
                existing_blob.patch()
                logger.info(f'Temporary object {object_name} successfully created')

        pipeline_builder = sdc_builder.get_pipeline_builder()

        dev_raw_data_source_origin = pipeline_builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source_origin.set_attributes(data_format='JSON',
                                                  raw_data=raw_data,
                                                  stop_after_first_batch=True)

        google_cloud_storage_executor = pipeline_builder.add_stage('Google Cloud Storage Executor', type='executor')
        google_cloud_storage_executor.set_attributes(task='CHANGE_OBJECT',
                                                     project_id=gcp.project_id,
                                                     bucket=bucket_specification,
                                                     object=object_specification,
                                                     metadata=metadata_specification)

        wiretap = pipeline_builder.add_wiretap()

        dev_raw_data_source_origin >> google_cloud_storage_executor >= wiretap.destination

        pipeline = pipeline_builder.build(title=pipeline_title).configure_for_environment(gcp)
        sdc_executor.add_pipeline(pipeline)

        if with_bucket and with_object:
            sdc_executor.start_pipeline(pipeline).wait_for_finished()

            assert len(wiretap.error_records) == 0, \
                'This execution mode was not expected to produce error records'
            assert len(wiretap.output_records) == 1, \
                'Only one input record, so exactly one output even record was expected'
            assert wiretap.output_records[0].header.values['sdc.event.type'] == 'gcs_object_changed', \
                'It was expected an event record signaling the object creation'

            existing_blob = bucket.get_blob(object_name)
            assert existing_blob is not None, f'Just created object ({object_name}) does not exist in this bucket'
            existing_object_content = existing_blob.download_as_string().decode('ascii')
            assert existing_object_content == object_content, 'Correct object but with unexpected contents'
            existing_object_metadata = collections.OrderedDict(sorted(existing_blob.metadata.items()))
            injected_object_metadata = collections.OrderedDict(sorted({**source_metadata, **target_metadata}.items()))
            assert existing_object_metadata == injected_object_metadata, 'Correct object but with unexpected metadata'
        else:
            sdc_executor.start_pipeline(pipeline).wait_for_finished()
            assert len(wiretap.error_records) == 1, \
                'This execution mode was expected to produce error records'
            assert len(wiretap.output_records) == 0, \
                'This execution mode was not expected to produce records'
            assert wiretap.error_records[0].header['errorCode'] == 'GCSE_014', \
                'This execution mode was expected to produce error GCSE_014: <Error changing GCS object>'
    finally:
        if with_bucket:
            logger.info(f'Deleting temporary bucket {bucket_name}')
            gcp.retry_429(bucket.delete)(force=True)
            logger.info(f'Temporary bucket {bucket_name} successfully deleted')
