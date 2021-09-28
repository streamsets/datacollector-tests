# Copyright 2021 StreamSets Inc.

import pytest
from streamsets.testframework.markers import cluster
from streamsets.testframework.utils import get_random_string


@pytest.mark.parametrize('number_of_threads', [1, 2, 4, 8])
@cluster('kafka')
def test_multithreaded(sdc_builder, sdc_executor, cluster, origin_topic, number_of_threads):
    """Benchmark Kafka Multitopic Consumer origin with multithreading"""

    pipeline_builder = sdc_builder.get_pipeline_builder()
    benchmark_stages = pipeline_builder.add_benchmark_stages()

    kafka_multitopic_consumer = pipeline_builder.add_stage('Kafka Multitopic Consumer',
                                                           library=cluster.kafka.standalone_stage_lib)
    kafka_multitopic_consumer.set_attributes(data_format='AVRO',
                                             avro_schema_location='SOURCE',
                                             consumer_group=get_random_string(),
                                             auto_offset_reset='EARLIEST',
                                             number_of_threads=number_of_threads,
                                             topic_list=[origin_topic.name])

    kafka_multitopic_consumer >> benchmark_stages.destination

    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    # Kafka origin records should be > benchmark_pipeline parameters runs*record_count as the consumer group is not
    # changed between runs.
    origin_topic.load_records(10_000_000)
    sdc_executor.benchmark_pipeline(pipeline, runs=3, record_count=3_000_000)
