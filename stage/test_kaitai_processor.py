# Copyright 2023 StreamSets Inc.
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
import time

import pytest
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import aws, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

GIF_DIRECTORY = "/resources/resources/kaitai_processor/gif"
KSY_FILE_LOCATION = "/resources/resources/kaitai_processor/ksy/gif.ksy"
KAITAI_FILE_RELATIVE = "resources/kaitai_processor/ksy/gif.ksy"
GIF_DIRECTORY_RELATIVE = "resources/kaitai_processor/gif"

S3_SANDBOX_PREFIX = 'sandbox'

pytestmark = sdc_min_version('5.6.0')


def build_pipeline_with_inline_ksy(sdc_builder, number_of_threads):
    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')

    directory.set_attributes(data_format='BINARY',
                             file_name_pattern='*.gif',
                             file_name_pattern_mode='GLOB',
                             files_directory=GIF_DIRECTORY,
                             max_data_size_in_bytes=15000000,
                             number_of_threads=number_of_threads)

    kaitai_struct = pipeline_builder.add_stage('Kaitai Struct Parser', type='processor')
    kaitai_struct.set_attributes(kaitai_struct_source='INLINE',
                                 kaitai_struct_definition=get_kaitai_definition(KAITAI_FILE_RELATIVE))

    wiretap = pipeline_builder.add_wiretap()

    directory >> kaitai_struct >> wiretap.destination

    return pipeline_builder, wiretap


def get_kaitai_definition(file_path):
    with open(file_path, "r") as text_file:
        ksy_definition = text_file.read()
    return ksy_definition


def validate_output(sdc_builder, wiretap):
    output_records = wiretap.output_records
    assert len(output_records) == 9
    for record in output_records:
        assert 'hdr' in record.field
        assert 'logicalScreenDescriptor' in record.field
        assert 'globalColorTable' in record.field
        assert 'blocks' in record.field
        assert record.field['hdr']['version'] == "89a"
        if Version(sdc_builder.version) < Version('5.8.0'):
            assert record.field['hdr']['magic'] == "R0lG"
        else:
            assert record.field['hdr']['magic'] == b'GIF'

def skip_if_java8(sdc_builder):
    sdc_host_info = sdc_builder.api_client.get_health_report('HealthHostInformation').response.json()
    is_jdk8 = sdc_host_info['hostInformation']['javaVersion'] == 8 if 'hostInformation' in sdc_host_info else True
    if is_jdk8:
        pytest.skip("Skipping test for JDK 8")


@pytest.mark.parametrize('number_of_threads', [1, 5, 10])
def test_kaitai_processor_gif_dir_origin_inline(sdc_builder, sdc_executor, number_of_threads):
    skip_if_java8(sdc_builder)
    pipeline_builder, wiretap = build_pipeline_with_inline_ksy(sdc_builder, number_of_threads)
    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 9, 120)
    sdc_executor.stop_pipeline(pipeline)
    validate_output(sdc_builder, wiretap)


@pytest.mark.parametrize('number_of_threads', [1, 5, 10])
def test_kaitai_processor_gif_dir_origin_inline_multiple_pipelines(sdc_builder, sdc_executor, number_of_threads):
    skip_if_java8(sdc_builder)
    pipeline_builder1, wiretap1 = build_pipeline_with_inline_ksy(sdc_builder, number_of_threads)
    pipeline1 = pipeline_builder1.build()

    pipeline_builder2, wiretap2 = build_pipeline_with_inline_ksy(sdc_builder, number_of_threads)
    pipeline2 = pipeline_builder2.build()

    sdc_executor.add_pipeline(pipeline1)
    sdc_executor.add_pipeline(pipeline2)

    sdc_executor.start_pipeline(pipeline1)
    sdc_executor.start_pipeline(pipeline2)

    sdc_executor.wait_for_pipeline_metric(pipeline1, 'input_record_count', 9, 120)
    sdc_executor.wait_for_pipeline_metric(pipeline2, 'input_record_count', 9, 120)

    sdc_executor.stop_pipeline(pipeline1)
    sdc_executor.stop_pipeline(pipeline2)

    validate_output(sdc_builder, wiretap1)
    validate_output(sdc_builder, wiretap2)


@pytest.mark.parametrize('number_of_threads', [1, 5, 10])
def test_kaitai_processor_gif_dir_origin(sdc_builder, sdc_executor, number_of_threads):
    skip_if_java8(sdc_builder)
    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')

    directory.set_attributes(data_format='BINARY',
                             file_name_pattern='*.gif',
                             file_name_pattern_mode='GLOB',
                             files_directory=GIF_DIRECTORY,
                             max_data_size_in_bytes=15000000,
                             number_of_threads=number_of_threads)

    kaitai_struct = pipeline_builder.add_stage('Kaitai Struct Parser', type='processor')
    kaitai_struct.set_attributes(kaitai_struct_source='FILE_PATH',
                                 kaitai_struct_file_path=KSY_FILE_LOCATION)

    wiretap = pipeline_builder.add_wiretap()

    directory >> kaitai_struct >> wiretap.destination

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 9, 120)
    sdc_executor.stop_pipeline(pipeline)

    validate_output(sdc_builder, wiretap)


@aws('s3')
@pytest.mark.parametrize('number_of_threads', [1, 5, 10])
def test_kaitai_processor_gif_s3_origin(sdc_builder, sdc_executor, aws, number_of_threads):
    skip_if_java8(sdc_builder)
    try:
        s3_bucket = aws.s3_bucket_name
        s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/sdc/kaitai/'

        # Build pipeline.
        builder = sdc_builder.get_pipeline_builder()

        s3_origin = builder.add_stage('Amazon S3', type='origin')

        s3_origin.set_attributes(bucket=s3_bucket,
                                 data_format='BINARY',
                                 prefix_pattern=f'{s3_key}*',
                                 number_of_threads=number_of_threads,
                                 max_data_size_in_bytes=15000000,
                                 )

        kaitai_struct = builder.add_stage('Kaitai Struct Parser', type='processor')
        kaitai_struct.set_attributes(kaitai_struct_source='FILE_PATH',
                                     kaitai_struct_file_path=KSY_FILE_LOCATION)

        wiretap = builder.add_wiretap()

        s3_origin >> kaitai_struct >> wiretap.destination

        s3_origin_pipeline = builder.build().configure_for_environment(aws)
        s3_origin_pipeline.configuration['shouldRetry'] = False

        sdc_executor.add_pipeline(s3_origin_pipeline)

        client = aws.s3
        # Insert objects into S3.
        for file_path in os.listdir(GIF_DIRECTORY_RELATIVE):
            client.upload_file(GIF_DIRECTORY_RELATIVE + "/" + file_path, s3_bucket, s3_key + file_path)

        # AWS S3 Origin does not process files created less than 5 seconds ago by default, so we must wait a bit
        time.sleep(10)

        sdc_executor.start_pipeline(s3_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(s3_origin_pipeline, 'input_record_count', 9, 120)
        sdc_executor.stop_pipeline(s3_origin_pipeline)
        validate_output(sdc_builder, wiretap)

    finally:
        aws.delete_s3_data(s3_bucket, s3_key)
