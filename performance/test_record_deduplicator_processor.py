# Copyright 2021 StreamSets Inc.

import pytest


@pytest.mark.parametrize('thread_count', [1, 4, 8])
def test_defaults(sdc_executor, sdc_builder, thread_count):
    """Benchmark Record Deduplicator processor with default settings"""

    pipeline_builder = sdc_builder.get_pipeline_builder()
    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')

    benchmark_stages = pipeline_builder.add_benchmark_stages()
    benchmark_stages.origin.set_attributes(number_of_threads=thread_count)
    benchmark_stages.origin >> record_deduplicator >> benchmark_stages.destination
    record_deduplicator >> benchmark_stages.destination

    pipeline = pipeline_builder.build()

    sdc_executor.benchmark_pipeline(pipeline, record_count=5_000_000, max_time=240, skip_metrics=['file_descriptors'])
