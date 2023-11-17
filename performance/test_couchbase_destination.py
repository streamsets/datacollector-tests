# Copyright 2023 StreamSets Inc.

import logging
import pytest

from streamsets.testframework.markers import couchbase, sdc_min_version

logger = logging.getLogger(__name__)

pytestmark = [couchbase, sdc_min_version('3.4.0')]

SUPPORTED_LIBS = ['streamsets-datacollector-couchbase_2-lib', 'streamsets-datacollector-couchbase_3-lib']
STAGE_NAME = 'com_streamsets_pipeline_stage_destination_couchbase_CouchbaseDTarget'


@pytest.fixture(autouse=True, scope='module')
def init(couchbase):
    for lib in couchbase.sdc_stage_libs:
        if lib in SUPPORTED_LIBS:
            couchbase.pre_create_buckets()
            return
    pytest.skip(f'Couchbase Destination test requires using libraries in {SUPPORTED_LIBS}')


@pytest.mark.parametrize('num_threads', [1, 5, 10])
@pytest.mark.parametrize('batch_size', [1_000, 10_000])
def test_basic(sdc_builder, sdc_executor, couchbase, num_threads, batch_size):
    """
    Send simple JSON text into Couchbase destination from Dev Raw Data Source and assert Couchbase has received it.

    The pipeline looks like:
        dev_raw_data_source >> couchbase_destination
    """
    document_key_field = 'mydocname'
    bucket_name = couchbase.get_bucket()

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
        couchbase.cleanup_buckets()
