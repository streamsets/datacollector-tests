# Copyright 2017 StreamSets Inc.
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

import ast
import json
import logging
import string

from streamsets.testframework.markers import redis
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# pylint: disable=pointless-statement, redefined-outer-name, too-many-locals


@redis
def test_redis_origin(sdc_builder, sdc_executor, redis):
    """Test for Redis origin stage. We do so by starting the pipeline first which ensures the required Redis channel
    is created and then we publish data to snapshot. The pipeline looks like:

        redis_consumer >> trash
    """
    raw_dict = dict(name='Jane Smith', zip_code=27023)
    raw_data = json.dumps(raw_dict)
    pattern_1 = get_random_string(string.ascii_letters, 10)
    pattern_2 = get_random_string(string.ascii_letters, 6)

    builder = sdc_builder.get_pipeline_builder()
    redis_consumer = builder.add_stage('Redis Consumer', type='origin')
    # have the Redis consumer read data out of channel based on glob patterns
    # set max_batch_size_in_records to 10 to let capture_snapshot's start_pipeline=False capture the 2nd batch
    redis_consumer.set_attributes(data_format='JSON', max_batch_size_in_records=10,
                                  pattern=[f'*{pattern_1}*', f'{pattern_2}?'])
    trash = builder.add_stage('Trash')

    redis_consumer >> trash
    pipeline = builder.build(title='Redis Consumer pipeline').configure_for_environment(redis)
    sdc_executor.add_pipeline(pipeline)

    try:
        # pipeline has to be started first to have the Redis channel to be created
        sdc_executor.start_pipeline(pipeline)

        # case when we have valid pattern for *
        snapshot_command = sdc_executor.capture_snapshot(pipeline, start_pipeline=False, wait=False)
        key_1 = f'extra{pattern_1}extra'
        for _ in range(20):  # 20 records will make 2 batches (each of 10)
            assert redis.client.publish(key_1, raw_data) == 1  # 1 indicates pipeline consumer received the raw_data
        snapshot = snapshot_command.wait_for_finished().snapshot
        assert redis_consumer.max_batch_size_in_records == len(snapshot[redis_consumer.instance_name].output)
        for record in snapshot[redis_consumer.instance_name].output:
            assert record.field['name'].value == raw_dict['name']
            assert record.field['zip_code'].value == raw_dict['zip_code']

        # case when we have valid pattern for ?
        snapshot_command = sdc_executor.capture_snapshot(pipeline, start_pipeline=False, wait=False)
        key_2 = f'{pattern_2}X'
        for _ in range(20):  # 20 records will make 2 batches (each of 10)
            assert redis.client.publish(key_2, raw_data) == 1  # 1 indicates pipeline consumer received the raw_data
        snapshot = snapshot_command.wait_for_finished().snapshot
        assert redis_consumer.max_batch_size_in_records == len(snapshot[redis_consumer.instance_name].output)
        for record in snapshot[redis_consumer.instance_name].output:
            assert record.field['name'].value == raw_dict['name']
            assert record.field['zip_code'].value == raw_dict['zip_code']

        # Case when we have an invalid pattern. No data is expected and hence no snapshot can be taken to compare.
        key_3 = f'{pattern_2}XX'
        for _ in range(20):
            assert redis.client.publish(key_3, raw_data) == 0  # 0 clients received the raw_data
    finally:
        sdc_executor.stop_pipeline(pipeline)


@redis
def test_redis_destination(sdc_builder, sdc_executor, redis):
    """Test for Redis destination stage. The pipeline looks like:

        dev_raw_data_source >> redis_destination
    """
    redis_key = get_random_string(string.ascii_letters, 10)
    raw_dict = dict(city=redis_key, coordinates=dict(latitude='37.7576948', longitude='-122.4726194'))
    raw_data = json.dumps(raw_dict)

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data)
    redis_destination = builder.add_stage('Redis', type='destination')
    redis_destination.set_attributes(mode='BATCH', fields=[{'keyExpr': '/city',
                                                            'valExpr': '/coordinates',
                                                            'dataType': 'HASH'}])

    dev_raw_data_source >> redis_destination
    pipeline = builder.build(title='Redis Destination pipeline').configure_for_environment(redis)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(1)
        sdc_executor.stop_pipeline(pipeline)

        # read data from Redis and assert to what pipeline ingested
        dict1 = raw_dict['coordinates']
        dict2 = {k.decode(): v.decode() for k, v in redis.client.hgetall(redis_key).items()}

        assert dict1 == dict2
    finally:
        # delete our key from Redis
        redis.client.delete(redis_key)


@redis
def test_redis_lookup_processor(sdc_builder, sdc_executor, redis):
    """Test for Redis Lookup Processor. We do so by putting data in Redis first and then have the Redis Lookup
    processor look for it. The pipeline looks like:

        dev_raw_data_source >> redis_lookup_processor >> trash
    """
    redis_key = get_random_string(string.ascii_letters, 10)
    redis_data = "('37.7576948', '-122.4726194')"
    raw_data = f'{{"city": "{redis_key}"}}'
    record_output_field = 'latitude_longitude'

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data)
    redis_lookup_processor = builder.add_stage('Redis Lookup Processor')
    redis_lookup_processor.set_attributes(enable_local_caching=True,
                                          eviction_policy_type='EXPIRE_AFTER_ACCESS', mode='BATCH',
                                          lookup_parameters=[{'dataType': 'LIST',
                                                              'keyExpr': "${record:value('/city')}",
                                                              'outputFieldPath': f'/{record_output_field}'}])
    trash = builder.add_stage('Trash')

    dev_raw_data_source >> redis_lookup_processor >> trash
    pipeline = builder.build(title='Redis Lookup Processor pipeline').configure_for_environment(redis)
    sdc_executor.add_pipeline(pipeline)

    try:
        # input data into Redis which should be looked by our processor
        redis.client.lpush(redis_key, redis_data)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        record = snapshot[redis_lookup_processor.instance_name].output[0].field
        expected_value = str(record[record_output_field][0])
        assert redis_data == expected_value
    finally:
        # delete our key from Redis
        redis.client.delete(redis_key)
