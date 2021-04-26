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

import logging
import os

from streamsets.testframework.markers import aws, sdc_min_version
from streamsets.testframework.utils import get_random_string

import json

S3_SANDBOX_PREFIX = 'sandbox'

logger = logging.getLogger(__name__)


@aws('s3')
def test_startup(sdc_builder, sdc_executor, aws, benchmark):
    """Performance test for AWS S3 Origin.

    The test populates a random AWS S3 directory in the bucket specified by the aws instance with a total of
    1000 random files. Then the startup time of the pipeline is measured by waiting for the pipeline to reach
    the RUNNING state.

    Pipeline: s3_origin >> trash
    """
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/sdc'
    num_files = 1000
    data = dict(f1=get_random_string(), f2=get_random_string())

    try:
        # Build the pipeline
        builder = sdc_builder.get_pipeline_builder()
        builder.add_error_stage('Discard')

        s3_origin = builder.add_stage('Amazon S3', type='origin')
        s3_origin.set_attributes(bucket=aws.s3_bucket_name,
                                 data_format='JSON',
                                 prefix_pattern=f'{s3_key}/*',
                                 number_of_threads=5,
                                 read_order='LEXICOGRAPHICAL')

        trash = builder.add_stage('Trash')

        s3_origin >> trash

        s3_origin_pipeline = builder.build(title='Amazon S3 origin startup performance').configure_for_environment(aws)

        # Populate the directory
        for i in range(num_files):
            aws.s3.put_object(Bucket=aws.s3_bucket_name, Key=f'{s3_key}/{i}', Body=json.dumps(data))

        def benchmark_pipeline(executor, pipeline):
            executor.add_pipeline(pipeline)
            executor.start_pipeline(pipeline).wait_for_status('RUNNING', timeout_sec=1000)
            executor.stop_pipeline(pipeline).wait_for_stopped()
            executor.remove_pipeline(pipeline)

        benchmark.pedantic(benchmark_pipeline, args=(sdc_executor, s3_origin_pipeline), rounds=5)
    finally:
        # Clean up S3.
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in
                                   aws.s3.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)['Contents']]}
        aws.s3.delete_objects(Bucket=aws.s3_bucket_name, Delete=delete_keys)
