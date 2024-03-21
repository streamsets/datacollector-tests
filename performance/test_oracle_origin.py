# Copyright 2023 StreamSets Inc.

"""
The tests in this module are for running high-volume pipelines, for the purpose of performance testing.
"""

import logging
import pytest

from streamsets.testframework.markers import database, sdc_min_version


logger = logging.getLogger(__name__)


@pytest.fixture(scope='module')
def sdc_builder_hook():
    def hook(data_collector):
        data_collector.SDC_JAVA_OPTS = '-Xmx8192m -Xms8192m'
    return hook


@database("oracle")
@sdc_min_version('5.6.0')
def test_oracle_consumer(sdc_executor, sdc_builder, database, origin_table):
    """Performance benchmark a simple Oracle Bulk Loader with some dummy records"""

    record_count = 700_000
    pipeline_builder = sdc_builder.get_pipeline_builder()
    oracle_consumer = pipeline_builder.add_stage('Oracle Bulkload')
    oracle_consumer.set_attributes(tables=[dict(schemaName='', tableName=origin_table.name)])

    benchmark_stages = pipeline_builder.add_benchmark_stages()
    oracle_consumer >> benchmark_stages.destination

    pipeline = pipeline_builder.build().configure_for_environment(database)
    origin_table.load_records(record_count)
    sdc_executor.benchmark_pipeline(pipeline, record_count=record_count, runs=1)
