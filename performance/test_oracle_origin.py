# Copyright 2023 StreamSets Inc.

"""
The tests in this module are for running high-volume pipelines, for the purpose of performance testing.
"""

import logging
import uuid
import pytest

from streamsets.testframework.markers import database, sdc_min_version


logger = logging.getLogger(__name__)


@pytest.fixture(scope='module')
def sdc_builder_hook():
    def hook(data_collector):
        data_collector.SDC_JAVA_OPTS = '-Xmx8192m -Xms8192m'
    return hook

@database('oracle')
@sdc_min_version('5.6.0')
def test_oracle_consumer(sdc_builder, sdc_executor, database, benchmark):
    """Performance benchmark a simple Oracle Bulk Loader to trash pipeline.
    The test uses an existing table in Oracle instance called VOLUME_TEST with 700000 records.
    The pipeline connects oracle bulk loader to trash and includes a finisher"""

    pipeline_builder = sdc_builder.get_pipeline_builder()
    oracle_consumer = pipeline_builder.add_stage('Oracle Bulkload')
    oracle_consumer.set_attributes(tables=[dict(schemaName='', tableName='VOLUME_TEST')])
    trash = pipeline_builder.add_stage('Trash')
    finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')

    oracle_consumer >> trash
    oracle_consumer >= finisher

    pipeline = pipeline_builder.build().configure_for_environment(database)

    def benchmark_pipeline(executor, pipeline):
        pipeline.id = str(uuid.uuid4())
        executor.add_pipeline(pipeline)
        logger.info('Starting the pipeline...')
        executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=3600)
        logger.info('Removing the pipeline...')
        executor.remove_pipeline(pipeline)

    benchmark.pedantic(benchmark_pipeline, args=(sdc_executor, pipeline), rounds=1)
