# Copyright 2018 StreamSets Inc.
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

import json
import logging
import pytest
import string

from streamsets.sdk.exceptions import ValidationError
from streamsets.testframework.markers import couchbase, sdc_min_version
from streamsets.testframework.utils import get_random_string
from couchbase.management.buckets import CreateBucketSettings
from couchbase.management.collections import CollectionSpec
from couchbase.options import ClusterOptions, GetOptions, UpsertOptions

logger = logging.getLogger(__name__)

pytestmark = [couchbase, sdc_min_version('3.4.0')]

STAGE_NAME = 'com_streamsets_pipeline_stage_destination_couchbase_CouchbaseDTarget'

DEFAULT_SCOPE = '_default'
DEFAULT_COLLECTION = '_default'


def create_bucket(couchbase, bucket_name, scope_name=DEFAULT_SCOPE, collection_name=DEFAULT_COLLECTION,
                  ram_quota_mb=128, create_primary_index=True):
    logger.info(f'Creating {bucket_name} Couchbase bucket...')
    couchbase.bucket_manager.create_bucket(CreateBucketSettings(name=bucket_name,
                                                                bucket_type='couchbase',
                                                                ram_quota_mb=ram_quota_mb))
    couchbase.wait_for_healthy_bucket(bucket_name)
    bucket = couchbase.cluster.bucket(bucket_name)
    if scope_name != DEFAULT_SCOPE:
        logger.info(f'Creating {scope_name} scope in {bucket_name} Couchbase bucket...')
        bucket.collections().create_scope(scope_name)
    if collection_name != DEFAULT_COLLECTION:
        logger.info(
            f'Creating {collection_name} collection in {scope_name} scope in {bucket_name} Couchbase bucket...')
        bucket.collections().create_collection(
            CollectionSpec(collection_name=collection_name, scope_name=scope_name))
    if create_primary_index:
        logger.info(
            f'Creating PRIMARY INDEX on `{bucket_name}`.`{scope_name}`.`{collection_name}` Couchbase bucket ...')
        couchbase.cluster.query(f'CREATE PRIMARY INDEX ON `{bucket_name}`.`{scope_name}`.`{collection_name}`').execute()
    return bucket


def get_value(bucket, document_id, scope_name=DEFAULT_SCOPE, collection_name=DEFAULT_COLLECTION, transcoder=None):
    return bucket.scope(scope_name).collection(collection_name).get(document_id,
                                                                    GetOptions(transcoder=transcoder)).value


def test_basic(sdc_builder, sdc_executor, couchbase):
    """
    Send simple JSON text into Couchbase destination from Dev Raw Data Source and assert Couchbase has received it.

    The pipeline looks like:
        dev_raw_data_source >> couchbase_destination
    """
    bucket_name = get_random_string(string.ascii_letters, 10)
    document_key_field = 'mydocname'
    raw_dict = dict(f1='abc', f2='xyz', f3='lmn')
    raw_dict[document_key_field] = 'mydocid'
    raw_data = json.dumps(raw_dict)
    try:
        bucket = create_bucket(couchbase, bucket_name)

        builder = sdc_builder.get_pipeline_builder()
        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)
        couchbase_destination = builder.add_stage(name=STAGE_NAME)
        couchbase_destination.set_attributes(authentication_mode='USER',
                                             document_key="${record:value('/" + document_key_field + "')}",
                                             bucket=bucket_name)

        dev_raw_data_source >> couchbase_destination

        pipeline = builder.build().configure_for_environment(couchbase)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        doc_value = get_value(bucket, raw_dict[document_key_field])
        assert doc_value == raw_dict
    finally:
        logger.info('Deleting %s Couchbase bucket ...', bucket_name)
        couchbase.bucket_manager.drop_bucket(bucket_name)


def test_invalid_config(sdc_builder, sdc_executor, couchbase):
    bucket_name = get_random_string(string.ascii_letters, 10)

    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Data Generator')
    source.batch_size = 10
    source.fields_to_generate = [{
        "type": "LONG_SEQUENCE",
        "field": "seq"
    }]

    destination = builder.add_stage(name=STAGE_NAME)
    destination.set_attributes(
        authentication_mode=None,
        bucket=bucket_name,
        document_key=None,
        data_format=None,
        connect_timeout_in_ms=-1,
        key_value_timeout_in_ms=-1,
        disconnect_timeout_in_ms=-1
    )

    wiretap = builder.add_wiretap()

    source >> destination
    source >= wiretap.destination

    pipeline = builder.build().configure_for_environment(couchbase)
    sdc_executor.add_pipeline(pipeline)

    try:
        create_bucket(couchbase, bucket_name)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.stop_pipeline(pipeline)
    except ValidationError as e:
        # all configs are invalid
        assert e.issues['issueCount'] == 6
    finally:
        logger.info('Deleting %s Couchbase bucket ...', bucket_name)
        couchbase.bucket_manager.drop_bucket(bucket_name)


def test_write_empty_batch(sdc_builder, sdc_executor, couchbase):
    bucket_name = get_random_string(string.ascii_letters, 10)
    document_key = 'id'

    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                     json_content='ARRAY_OBJECTS',
                                                                     raw_data="[]",
                                                                     stop_after_first_batch=True)

    destination = builder.add_stage(name=STAGE_NAME)
    destination.set_attributes(authentication_mode='USER', bucket=bucket_name, document_key=document_key,
                               data_format='TEXT', text_field_path="/text", record_separator="\n")

    source >> destination

    pipeline = builder.build().configure_for_environment(couchbase)
    sdc_executor.add_pipeline(pipeline)

    try:
        create_bucket(couchbase, bucket_name)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        history = sdc_executor.get_pipeline_history(pipeline)
        num_records = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        logger.info(f"Wrote {num_records} records")
        assert num_records == 0, 'Number of records stored should equal the number of records that entered the pipeline'

    finally:
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting %s Couchbase bucket ...', bucket_name)
        couchbase.bucket_manager.drop_bucket(bucket_name)


@pytest.mark.parametrize('default_write_operation', ['DELETE', 'INSERT', 'REPLACE', 'UPSERT'])
def test_row_operations(sdc_builder, sdc_executor, couchbase, default_write_operation):
    bucket_name = get_random_string(string.ascii_letters, 10)
    document_key_field = 'mydocname'
    raw_dict = dict(f1='abc', f2='xyz', f3='lmn')
    raw_dict[document_key_field] = 'mydocid'
    raw_data = json.dumps(raw_dict)

    try:
        bucket = create_bucket(couchbase, bucket_name)

        # Build the pipeline
        builder = sdc_builder.get_pipeline_builder()

        source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                         raw_data=raw_data,
                                                                         stop_after_first_batch=True)

        destination = builder.add_stage(name=STAGE_NAME)
        destination.set_attributes(authentication_mode='USER', bucket=bucket_name,
                                   document_key="${record:value('/" + document_key_field + "')}",
                                   data_format='JSON',
                                   default_write_operation=default_write_operation)

        source >> destination

        pipeline = builder.build().configure_for_environment(couchbase)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        history = sdc_executor.get_pipeline_history(pipeline)
        num_records = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        if default_write_operation == 'INSERT' or default_write_operation == 'UPSERT':
            doc_value = get_value(bucket, raw_dict[document_key_field])
            assert doc_value == raw_dict
            assert num_records == 1
        else:
            assert num_records == 0
    finally:
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting %s Couchbase bucket ...', bucket_name)
        couchbase.bucket_manager.drop_bucket(bucket_name)


@pytest.mark.parametrize('default_write_operation', ['DELETE', 'INSERT', 'REPLACE', 'UPSERT'])
def test_row_operations(sdc_builder, sdc_executor, couchbase, default_write_operation):
    bucket_name = get_random_string(string.ascii_letters, 10)
    document_key_field = 'mydocname'
    raw_dict = dict(f1='abc', f2='xyz', f3='lmn')
    raw_dict[document_key_field] = 'mydocid'
    raw_data = json.dumps(raw_dict)

    try:
        bucket = create_bucket(couchbase, bucket_name)

        # Build the pipeline
        builder = sdc_builder.get_pipeline_builder()

        source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                         raw_data=raw_data,
                                                                         stop_after_first_batch=True)

        destination = builder.add_stage(name=STAGE_NAME)
        destination.set_attributes(authentication_mode='USER', bucket=bucket_name,
                                   document_key="${record:value('/" + document_key_field + "')}",
                                   data_format='JSON',
                                   default_write_operation=default_write_operation)

        source >> destination

        pipeline = builder.build().configure_for_environment(couchbase)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        history = sdc_executor.get_pipeline_history(pipeline)
        num_records = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        if default_write_operation == 'INSERT' or default_write_operation == 'UPSERT':
            doc_value = get_value(bucket, raw_dict[document_key_field])
            assert doc_value == raw_dict
            assert num_records == 1
        else:
            assert num_records == 0
    finally:
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting %s Couchbase bucket ...', bucket_name)
        couchbase.bucket_manager.drop_bucket(bucket_name)


@pytest.mark.parametrize('subdoc_op', ['DELETE', 'INSERT', 'REPLACE', 'UPSERT'])
def test_subdoc_operation(sdc_builder, sdc_executor, couchbase, subdoc_op):
    bucket_name = get_random_string(string.ascii_letters, 10)
    document_key_field = 'mydocname'
    raw_dict = dict(f1='abc', f2='xyz', f3='lmn')
    raw_dict[document_key_field] = 'mydocid'
    raw_data = json.dumps(raw_dict)

    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                     raw_data=raw_data,
                                                                     stop_after_first_batch=True)

    destination = builder.add_stage(name=STAGE_NAME)
    destination.set_attributes(authentication_mode='USER', bucket=bucket_name,
                               document_key="${record:value('/" + document_key_field + "')}",
                               data_format='JSON',
                               sub_document_path="myPath",
                               sub_document_operation=subdoc_op)

    source >> destination

    pipeline = builder.build().configure_for_environment(couchbase)
    sdc_executor.add_pipeline(pipeline)

    try:
        create_bucket(couchbase, bucket_name)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        history = sdc_executor.get_pipeline_history(pipeline)

        assert history.latest.metrics.counter('stage.Couchbase_01.outputRecords.counter').count == 1
        assert history.latest.metrics.counter('stage.Couchbase_01.errorRecords.counter').count == 0
    finally:
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting %s Couchbase bucket ...', bucket_name)
        couchbase.bucket_manager.drop_bucket(bucket_name)
