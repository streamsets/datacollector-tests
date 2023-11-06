# Copyright 2023 StreamSets Inc.

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


@pytest.mark.parametrize('num_threads', [1, 5, 10])
@pytest.mark.parametrize('batch_size', [10_000, 20_000])
def test_basic(sdc_builder, sdc_executor, couchbase, num_threads, batch_size):
    """
    Send simple JSON text into Couchbase destination from Dev Raw Data Source and assert Couchbase has received it.

    The pipeline looks like:
        dev_raw_data_source >> couchbase_destination
    """
    bucket_name = get_random_string(string.ascii_letters, 10)
    document_key_field = 'mydocname'

    create_bucket(couchbase, bucket_name)

    builder = sdc_builder.get_pipeline_builder()
    benchmark_stages = builder.add_benchmark_stages()
    benchmark_stages.origin.set_attributes(number_of_threads=num_threads,
                                           batch_size_in_recs=batch_size)
    couchbase_destination = builder.add_stage(name=STAGE_NAME)
    couchbase_destination.set_attributes(authentication_mode='USER',
                                         document_key="${record:value('/" + document_key_field + "')}",
                                         bucket=bucket_name)

    benchmark_stages.origin >> couchbase_destination

    pipeline = builder.build().configure_for_environment(couchbase)

    try:
        sdc_executor.benchmark_pipeline(pipeline, record_count=200_000)
    finally:
        logger.info('Deleting %s Couchbase bucket ...', bucket_name)
        couchbase.bucket_manager.drop_bucket(bucket_name)
