# Copyright 2021 StreamSets Inc.
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

from streamsets.testframework.markers import elasticsearch


@elasticsearch
def test_defaults(sdc_builder, sdc_executor, elasticsearch, elasticsearch_origin_index):
    """Benchmark Elasticsearch origin with default settings"""

    pipeline_builder = sdc_builder.get_pipeline_builder()
    benchmark_stages = pipeline_builder.add_benchmark_stages()

    elasticsearch_origin = pipeline_builder.add_stage('Elasticsearch', type='origin')
    elasticsearch_origin.set_attributes(index=elasticsearch_origin_index, query="{'query': {'match_all': {}}}")

    elasticsearch_origin >> benchmark_stages.destination
    pipeline = pipeline_builder.build().configure_for_environment(elasticsearch)

    sdc_executor.benchmark_pipeline(pipeline, record_count=5_000_000)
