# Copyright 2017 StreamSets Inc.
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The tests in this module follow a pattern of creating pipelines with
:py:obj:`testframework.sdc_models.PipelineBuilder` in one version of SDC and then importing and running them in
another.
"""

import ast
import json
import logging
import string

from testframework.markers import redis
from testframework.utils import get_random_string

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
    redis_consumer.set_attributes(data_format='JSON', pattern=[f'*{pattern_1}*', f'{pattern_2}?'])
    trash = builder.add_stage('Trash')

    redis_consumer >> trash
    pipeline = builder.build(title='Redis Consumer pipeline').configure_for_environment(redis)
    sdc_executor.add_pipeline(pipeline)

    # pipeline has to be started first to have the Redis channel to be created
    sdc_executor.start_pipeline(pipeline).wait_for_status('RUNNING')

    # define a callback function when we stream publishing our raw_data to Redis channel
    def snapshot_origin(): # pylint: disable=missing-docstring
        command = sdc_executor.capture_snapshot(pipeline, start_pipeline=False).wait_for_finished()
        if command is not None:
            return command.snapshot

    # case when we have valid pattern for *
    key_1 = f'extra{pattern_1}extra'
    snapshot = redis.publish_message_streaming(snapshot_origin, key_1, raw_data)
    value = snapshot[redis_consumer.instance_name].output[0].value['value']
    assert value['name']['value'] == raw_dict['name'] and value['zip_code']['value'] == str(raw_dict['zip_code'])

    # case when we have valid pattern for ?
    key_2 = f'{pattern_2}X'
    snapshot = redis.publish_message_streaming(snapshot_origin, key_2, raw_data)
    value = snapshot[redis_consumer.instance_name].output[0].value['value']
    assert value['name']['value'] == raw_dict['name'] and value['zip_code']['value'] == str(raw_dict['zip_code'])

    # case when we have an invalid pattern and hence no data is expected
    key_3 = f'{pattern_2}XX'
    snapshot = redis.publish_message_streaming(snapshot_origin, key_3, raw_data)
    assert snapshot is None


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
    redis_data = ('37.7576948', '-122.4726194')
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
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
        sdc_executor.stop_pipeline(pipeline)

        record = snapshot[redis_lookup_processor.instance_name].output[0].value['value']
        expected_value = record[record_output_field]['value'][0]['value']
        assert redis_data == ast.literal_eval(expected_value)
    finally:
        # delete our key from Redis
        redis.client.delete(redis_key)
