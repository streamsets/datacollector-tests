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

import logging
import string

from streamsets.testframework.markers import aws, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@aws('kinesis')
@sdc_min_version('2.7.2.1')
def test_kinesis_write_to_error(sdc_builder, sdc_executor, aws):
    """Test error record handling to a Kinesis stream. We use a dev raw data source to generate record which are
    directly sent to error through an error destination. Then we use a Kinesis client to consume messages from
    the stream and verify that all the record errors generated by the pipeline reached the stream.

    Pipeline: dev_raw_data_source >> error_target

    """
    stream_name = f'{aws.kinesis_stream_prefix}_{get_random_string(string.ascii_letters, 10)}'
    raw_str = 'Hello World!'

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    err_stage = builder.add_error_stage('Write to Kinesis')
    err_stage.set_attributes(stream_name=stream_name)

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_str)

    error_target = builder.add_stage('To Error')

    dev_raw_data_source >> error_target
    pipeline = builder.build().configure_for_environment(aws)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create Kinesis stream.
        logger.debug('Creating %s Kinesis stream on AWS...', stream_name)
        aws.kinesis.create_stream(StreamName=stream_name, ShardCount=1)
        aws.wait_for_stream_status(stream_name=stream_name, status='ACTIVE')
        shard_id = aws.kinesis.describe_stream(StreamName=stream_name)['StreamDescription']['Shards'][0]['ShardId']

        # Run pipeline and get error metrics.
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(1)
        sdc_executor.stop_pipeline(pipeline)

        history = sdc_executor.get_pipeline_history(pipeline)
        msg_count = history.latest.metrics.counter('pipeline.batchErrorRecords.counter').count
        logger.debug('Number of records sent to error = %s.', msg_count)

        # Read data from Kinesis stream and compare with the records sent to error. We check that Kinesis
        # messages and error records match, comparing number of items and data (looking for ocurrences of
        # stage name and input string).
        response = aws.kinesis.get_shard_iterator(StreamName=stream_name,
                                                  ShardId=shard_id,
                                                  ShardIteratorType='TRIM_HORIZON')
        response = aws.kinesis.get_records(ShardIterator=response['ShardIterator'])

        assert len(response['Records']) == msg_count
        assert all([error_target.instance_name.encode() in rec['Data'] for rec in response['Records']])
        assert all([raw_str.encode() in rec['Data'] for rec in response['Records']])

    finally:
        _ensure_pipeline_is_stopped(sdc_executor, pipeline)
        logger.debug('Deleting Kinesis stream %s...', stream_name)
        aws.kinesis.delete_stream(StreamName=stream_name)


def _ensure_pipeline_is_stopped(sdc_executor, pipeline):
    if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
        sdc_executor.stop_pipeline(pipeline)
