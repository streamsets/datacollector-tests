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
import time
from datetime import datetime

from streamsets.testframework.markers import aws
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

# Sandbox prefix for S3 bucket
S3_SANDBOX_PREFIX = 'sandbox'

SERVICE_ENDPOINT_FORMAT = '{}.{}.amazonaws.com'


@aws('firehose', 's3')
def test_firehose_destination_to_s3(sdc_builder, sdc_executor, aws):
    """Test for Firehose target stage. This test assumes Firehose is destined to S3 bucket. We run a dev raw data source
    generator to Firehose destination which is pre-setup to put to S3 bucket. We then read S3 bucket using STF client
    to assert data between the client to what has been ingested into the pipeline. The pipeline looks like:

    Firehose Destination pipeline:
        dev_raw_data_source >> firehose_destination
    """
    s3_client = aws.s3
    firehose_client = aws.firehose

    # setup test static
    s3_bucket = aws.s3_bucket_name
    stream_name = aws.firehose_stream_name
    # json formatted string
    random_raw_str = f'{{"text":"{get_random_string(string.ascii_letters, 10)}"}}'
    record_count = 1  # random_raw_str record size
    s3_put_keys = []

    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=random_raw_str,
                                                                                  stop_after_first_batch=True)

    firehose_destination = builder.add_stage('Kinesis Firehose')
    firehose_destination.set_attributes(stream_name=stream_name, data_format='JSON')

    dev_raw_data_source >> firehose_destination

    firehose_dest_pipeline = builder.build(title='Amazon Firehose destination pipeline').configure_for_environment(aws)
    sdc_executor.add_pipeline(firehose_dest_pipeline)

    try:
        # start pipeline and assert
        sdc_executor.start_pipeline(firehose_dest_pipeline).wait_for_finished()

        # wait till data is available in S3. We do so by querying for buffer wait time and sleep till then
        resp = firehose_client.describe_delivery_stream(DeliveryStreamName=stream_name)
        dests = resp['DeliveryStreamDescription']['Destinations'][0]
        wait_secs = dests['ExtendedS3DestinationDescription']['BufferingHints']['IntervalInSeconds']
        time.sleep(wait_secs + 15)  # few seconds more to wait to make sure S3 gets the data

        # Firehose S3 object naming http://docs.aws.amazon.com/firehose/latest/dev/basic-deliver.html#s3-object-name
        # read data to assert
        list_s3_objs = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=datetime.utcnow().strftime("%Y/%m/%d"))
        for s3_content in list_s3_objs['Contents']:
            akey = s3_content['Key']
            aobj = s3_client.get_object(Bucket=s3_bucket, Key=akey)
            if aobj['Body'].read().decode().strip() == random_raw_str:
                s3_put_keys.append(akey)

        assert len(s3_put_keys) == record_count
    finally:
        _ensure_pipeline_is_stopped(sdc_executor, firehose_dest_pipeline)
        # delete S3 objects related to this test
        if len(s3_put_keys) > 0:
            delete_keys = {'Objects': [{'Key': k} for k in s3_put_keys]}
            s3_client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)


@aws('firehose', 's3')
def test_firehose_destination_to_s3_other_region(sdc_builder, sdc_executor, aws):
    """Test for Firehose target stage with other as region and service endpoint.
    This test assumes Firehose is destined to S3 bucket. We run a dev raw data source generator to Firehose destination
    which is pre-setup to put to S3 bucket.
    The region is set to other and the service endpoint for firehose is used.
    We then read S3 bucket using STF client to assert data between the client to what has been ingested into the pipeline.
    The pipeline looks like:

    Firehose Destination pipeline:
        dev_raw_data_source >> firehose_destination
    """
    endpoint = SERVICE_ENDPOINT_FORMAT.format('firehose', aws.region)

    s3_client = aws.s3
    firehose_client = aws.firehose

    # setup test static
    s3_bucket = aws.s3_bucket_name
    stream_name = aws.firehose_stream_name
    # json formatted string
    random_raw_str = f'{{"text":"{get_random_string(string.ascii_letters, 10)}"}}'
    record_count = 1  # random_raw_str record size
    s3_put_keys = []

    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=random_raw_str,
                                                                                  stop_after_first_batch=True)

    firehose_destination = builder.add_stage('Kinesis Firehose')
    firehose_destination.set_attributes(stream_name=stream_name, data_format='JSON')

    dev_raw_data_source >> firehose_destination

    firehose_dest_pipeline = builder.build().configure_for_environment(aws)
    firehose_destination.set_attributes(region='OTHER', endpoint=endpoint)
    sdc_executor.add_pipeline(firehose_dest_pipeline)

    try:
        # start pipeline and assert
        sdc_executor.start_pipeline(firehose_dest_pipeline).wait_for_finished()

        # wait till data is available in S3. We do so by querying for buffer wait time and sleep till then
        resp = firehose_client.describe_delivery_stream(DeliveryStreamName=stream_name)
        dests = resp['DeliveryStreamDescription']['Destinations'][0]
        wait_secs = dests['ExtendedS3DestinationDescription']['BufferingHints']['IntervalInSeconds']
        time.sleep(wait_secs + 15)  # few seconds more to wait to make sure S3 gets the data

        # Firehose S3 object naming http://docs.aws.amazon.com/firehose/latest/dev/basic-deliver.html#s3-object-name
        # read data to assert
        list_s3_objs = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=datetime.utcnow().strftime("%Y/%m/%d"))
        for s3_content in list_s3_objs['Contents']:
            akey = s3_content['Key']
            aobj = s3_client.get_object(Bucket=s3_bucket, Key=akey)
            if aobj['Body'].read().decode().strip() == random_raw_str:
                s3_put_keys.append(akey)

        assert len(s3_put_keys) == record_count
    finally:
        _ensure_pipeline_is_stopped(sdc_executor, firehose_dest_pipeline)
        # delete S3 objects related to this test
        if len(s3_put_keys) > 0:
            delete_keys = {'Objects': [{'Key': k} for k in s3_put_keys]}
            s3_client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)


def _ensure_pipeline_is_stopped(sdc_executor, pipeline):
    if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
        sdc_executor.stop_pipeline(pipeline)
