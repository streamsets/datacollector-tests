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

ELASTICSEARCH_VERSION_8 = 8


@elasticsearch
def test_defaults(sdc_builder, sdc_executor, elasticsearch, elasticsearch_destination_index):
    """Benchmark Elasticsearch destination with default settings"""

    pipeline_builder = sdc_builder.get_pipeline_builder()
    benchmark_stages = pipeline_builder.add_benchmark_stages()

    elasticsearch_destination = pipeline_builder.add_stage('Elasticsearch', type='destination')
    elasticsearch_destination.set_attributes(default_operation='INDEX', index=elasticsearch_destination_index)

    if elasticsearch.major_version < ELASTICSEARCH_VERSION_8:
        elasticsearch_destination.set_attributes(mapping='mapping')

    benchmark_stages.origin >> elasticsearch_destination
    pipeline = pipeline_builder.build().configure_for_environment(elasticsearch)

    sdc_executor.benchmark_pipeline(pipeline, record_count=5_000_000)
