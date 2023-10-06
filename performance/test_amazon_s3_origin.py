# Copyright 2019 StreamSets Inc.
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

import pytest
from streamsets.testframework.markers import aws


@aws('s3')
@pytest.mark.parametrize('number_of_threads', (1, 2, 4, 8))
def test_multithreaded(sdc_builder, sdc_executor, aws, datasets, number_of_threads):
    """Benchmark Amazon S3 origin with multithreading"""

    DATA_FORMAT = 'AVRO'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')
    benchmark_stages = pipeline_builder.add_benchmark_stages()

    amazon_s3 = pipeline_builder.add_stage('Amazon S3', type='origin')
    amazon_s3.set_attributes(bucket=aws.s3_bucket_name,
                             data_format=DATA_FORMAT,
                             avro_schema_location='SOURCE',
                             common_prefix=f'datasets/{datasets.default.name}',
                             prefix_pattern=f'{DATA_FORMAT}/*',
                             number_of_threads=number_of_threads,
                             read_order='LEXICOGRAPHICAL')

    amazon_s3 >> benchmark_stages.destination

    pipeline = pipeline_builder.build().configure_for_environment(aws)

    sdc_executor.benchmark_pipeline(pipeline, record_count=10_000_000)
