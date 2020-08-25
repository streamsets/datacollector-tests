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
from collections import deque
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

    # There is currently a bug in the framework for pipelines that auto-stop (e.g. batc hpipelines)
    # So we add 200k records to the source so that it won't automatically stop but will wait on the
    # framework to stop it.
    upper = benchmark_args.get('RECORD_COUNT', 500_000) + 100_000
    def generator():
        for i in range(1, upper):
            if i % 10_000 == 0:
                logger.info(f'Inserting document with id/number {i}')

            yield {
                "_index": index,
                "_type": "data",
                "_source": {"number": i}
            }

    logger.info('Creating index %s', index)
    elasticsearch.client.create_index(index)
    logger.info('Populating index %s', index)
    deque(elasticsearch.client.parallel_bulk(generator(), thread_count=10))
    logger.info('Refreshing index %s', index)
    elasticsearch.client.refresh_index(index)

    yield index

    if not benchmark_args.get('KEEP_INDEX'):
        elasticsearch.client.delete_index(index)


@pytest.mark.parametrize('record_count', [100_000, 500_000])
@elasticsearch
def test_elasticsearch_origin(sdc_builder, sdc_executor, elasticsearch, record_count, test_index):
    """Performance benchmark a simple JDBC query consumer to trash pipeline."""

    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Elasticsearch', type='origin')
    origin.set_attributes(index=test_index, query="{'query': {'match_all': {}}}")
    trash = builder.add_stage('Trash')

    origin >> trash
    pipeline = builder.build().configure_for_environment(elasticsearch)

    print(sdc_executor.benchmark_pipeline(pipeline, record_count=record_count))
