# Copyright 2021 StreamSets Inc.

import pytest
from streamsets.testframework.markers import database


@pytest.mark.parametrize('batch_size', [1_000, 10_000, 20_000])
@database
def test_defaults(sdc_builder, sdc_executor, database, origin_table, batch_size):
    """Benchmark JDBC Query Consumer with default settings"""

    number_of_records = 5_000_000

    pipeline_builder = sdc_builder.get_pipeline_builder()
    benchmark_stages = pipeline_builder.add_benchmark_stages()

    jdbc_query_consumer = pipeline_builder.add_stage('JDBC Query Consumer')
    jdbc_query_consumer.set_attributes(incremental_mode=False,
                                       max_batch_size_in_records=batch_size,
                                       sql_query=f'SELECT * FROM {origin_table.name}')
    jdbc_query_consumer >> benchmark_stages.destination

    pipeline = pipeline_builder.build().configure_for_environment(database)

    origin_table.load_records(number_of_records)
    sdc_executor.benchmark_pipeline(pipeline, record_count=number_of_records)
