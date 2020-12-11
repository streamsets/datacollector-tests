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

# Sandbox prefix for S3 bucket
S3_SANDBOX_PREFIX = 'sandbox'


@aws('provisionemr')
@sdc_min_version('3.4.0')
def test_provision_emr_origin_to_s3(sdc_builder, sdc_executor, aws):
    """Test for EMR origin stage. We do so by putting data to a test S3 bucket using AWS S3 client and
    having a pipeline which reads that data using EMR origin stage and writes to S3 destination. Data is then asserted
    from destination S3 bucket to what is put by S3 client.

    An Amazon EMR is created in the pipeline. The pipeline looks like:

    EMR origin pipeline:
        emr_origin >> s3_destination
    """
    _test_emr_origin_to_s3(sdc_builder, sdc_executor, aws)


@aws('existingemr')
@sdc_min_version('3.4.0')
def test_existing_emr_origin_to_s3(sdc_builder, sdc_executor, aws):
    """Test for EMR origin stage. We do so by putting data to a test S3 bucket using AWS S3 client and
    having a pipeline which reads that data using EMR origin stage and writes to S3 destination.
    Data is then asserted from destination S3 bucket to what is put by S3 client.

    The Amazon EMR with the passed aws-emr-cluster-id is used in the pipeline. The pipeline looks like:

    EMR origin pipeline:
        emr_origin >> s3_destination
    """
    _test_emr_origin_to_s3(sdc_builder, sdc_executor, aws)


def _test_emr_origin_to_s3(sdc_builder, sdc_executor, aws):
    s3_bucket = aws.emr_s3_bucket_name
    s3_input_key = '{0}/{1}/input'.format(S3_SANDBOX_PREFIX, get_random_string(string.ascii_letters, 10))
    s3_output_key = '{0}/{1}/output'.format(S3_SANDBOX_PREFIX, get_random_string(string.ascii_letters, 10))

    s3_staging_bucket = aws.emr_s3_staging_bucket_name
    s3_staging_key = '{0}/{1}/sdc_staging'.format(S3_SANDBOX_PREFIX, get_random_string(string.ascii_letters, 10))
    s3_logging_key = '{0}/{1}/sdc_logging'.format(S3_SANDBOX_PREFIX, get_random_string(string.ascii_letters, 10))

    raw_str = 'Hello World!'
    s3_obj_count = 2  # keep it low, so as the number of MR jobs don't spin a lot and take a while lot of time

    logger.info('%s S3 bucket used with input key: %s output key: %s and object count: %s',
                s3_bucket, s3_input_key, s3_output_key, s3_obj_count)
    logger.info('%s S3 staging bucket used with EMR staging key: %s and EMR logging key: %s',
                s3_staging_bucket, s3_staging_key, s3_logging_key)

    # build pipeline
    builder = sdc_builder.get_pipeline_builder()

    emr_origin = builder.add_stage('Hadoop FS', type='origin')
    emr_origin.set_attributes(
        hadoop_fs_uri=f's3a://{s3_bucket}',
        input_paths=[f'/{s3_input_key}'],
        data_format='TEXT'
    )

    s3_destination = builder.add_stage('Amazon S3', type='destination')
    s3_destination.set_attributes(bucket=s3_bucket, data_format='TEXT', partition_prefix=s3_output_key)

    emr_origin >> s3_destination

    pipeline = builder.build(title='Amazon EMR to S3 pipeline').configure_for_environment(aws)
    sdc_executor.add_pipeline(pipeline)

    client = aws.s3
    try:
        logger.info('Creating input S3 data ...')
        [client.put_object(Bucket=s3_staging_bucket, Key='{0}/{1}'.format(s3_input_key, i), Body=raw_str)
         for i in range(s3_obj_count)]

        # lets not wait for pipeline start, as the transition from START to RUNNING takes more time
        sdc_executor.start_pipeline(pipeline, wait=False).wait_for_finished(timeout_sec=1800)

        # assert record count to S3 the size of the objects put
        list_s3_objs = client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_output_key)
        assert len(list_s3_objs['Contents']) == s3_obj_count

        # read data from S3 to assert it is what got ingested into the pipeline
        s3_contents = [client.get_object(Bucket=s3_bucket, Key=s3_content['Key'])['Body'].read().decode().strip()
                       for s3_content in list_s3_objs['Contents']]

        assert s3_contents == [raw_str] * s3_obj_count
    finally:
        logger.info('Deleting input S3 data from bucket %s with location %s ...', s3_bucket, s3_input_key)
        aws.delete_s3_data(s3_bucket, s3_input_key)

        logger.info('Deleting output S3 data from bucket %s with location %s ...', s3_bucket, s3_output_key)
        aws.delete_s3_data(s3_bucket, s3_output_key)

        logger.info('Deleting staging S3 data from bucket %s with location %s ...', s3_staging_bucket, s3_staging_key)
        aws.delete_s3_data(s3_staging_bucket, s3_staging_key)

        logger.info('Deleting logging S3 data from bucket %s with location %s ...', s3_staging_bucket, s3_logging_key)
        aws.delete_s3_data(s3_staging_bucket, s3_logging_key)
