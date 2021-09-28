# Copyright 2021 StreamSets Inc.

import pytest
from streamsets.testframework.markers import database, sdc_min_version


@database
def test_defaults(sdc_executor, sdc_builder, database, origin_table):
    """Benchmark JDBC Multitable Consumer origin with default settings"""
    number_of_records = 6_000_000

    pipeline_builder = sdc_builder.get_pipeline_builder()
    benchmark_stages = pipeline_builder.add_benchmark_stages()

    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(table_configs=[dict(tablePattern=origin_table.name)])

    jdbc_multitable_consumer >> benchmark_stages.destination

    pipeline = pipeline_builder.build().configure_for_environment(database)

    origin_table.load_records(number_of_records)
    sdc_executor.benchmark_pipeline(pipeline, record_count=number_of_records)


@sdc_min_version('2.7.0.0')
@pytest.mark.parametrize('number_of_threads', (1, 2, 4, 8))
@database
def test_multithreaded(sdc_executor, sdc_builder, database, origin_table, number_of_threads):
    """Benchmark JDBC Multitable Consumer origin with multithreading"""
    number_of_records = 6_000_000

    partition_size = str(int(number_of_records / number_of_threads))

    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(table_configs=[dict(tablePattern=origin_table.name,
                                                                partitioningMode='BEST_EFFORT',
                                                                maxNumActivePartitions=-1,
                                                                partitionSize=partition_size)],
                                            number_of_threads=number_of_threads,
                                            maximum_pool_size=number_of_threads)
    benchmark_stages = pipeline_builder.add_benchmark_stages()

    jdbc_multitable_consumer >> benchmark_stages.destination

    pipeline = pipeline_builder.build().configure_for_environment(database)

    origin_table.load_records(number_of_records)
    sdc_executor.benchmark_pipeline(pipeline, record_count=number_of_records)


@sdc_min_version('2.7.0.0')
@database
def test_partitioning_disabled(sdc_executor, sdc_builder, database, origin_table):
    """Benchmark JDBC Multitable Consumer origin with partitioning disabled"""

    number_of_records = 6_000_000

    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(table_configs=[dict(tablePattern=origin_table.name,
                                                                partitioningMode='DISABLED')])
    benchmark_stages = pipeline_builder.add_benchmark_stages()

    jdbc_multitable_consumer >> benchmark_stages.destination

    pipeline = pipeline_builder.build().configure_for_environment(database)

    origin_table.load_records(number_of_records)
    sdc_executor.benchmark_pipeline(pipeline, record_count=number_of_records)
