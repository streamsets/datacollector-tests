# Copyright 2020 StreamSets Inc.
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

"""
The tests in this module are for running high-volume pipelines, for the purpose of performance testing.
"""

import logging
import string

import pytest
from streamsets.testframework.markers import elasticsearch
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


@pytest.fixture(scope='module')
def sdc_builder_hook():
    def hook(data_collector):
        data_collector.SDC_JAVA_OPTS = '-Xmx8192m -Xms8192m'
    return hook


@pytest.fixture(scope='module')
def test_index(elasticsearch, benchmark_args):
    index = benchmark_args.get('INDEX_NAME') or get_random_string(string.ascii_lowercase, 20)

    elasticsearch.client.create_index(index)

    yield index

    if not benchmark_args.get('KEEP_INDEX'):
        elasticsearch.client.delete_index(index)


@pytest.mark.parametrize('record_count', [500_000, 1_000_000])
@elasticsearch
def test_elasticsearch_destination(sdc_builder, sdc_executor, elasticsearch, record_count, test_index):
    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Dev Data Generator')
    origin.batch_size = 1000
    origin.delay_between_batches = 0
    origin.fields_to_generate = [{
      "type" : "LONG_SEQUENCE",
      "field" : "index"
    }]

    target = builder.add_stage('Elasticsearch', type='destination')
    target.set_attributes(default_operation='INDEX', index=test_index, mapping="mapping")

    origin >> target
    pipeline = builder.build().configure_for_environment(elasticsearch)

    print(sdc_executor.benchmark_pipeline(pipeline, record_count=record_count))
