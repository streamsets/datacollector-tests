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

import os
import string
import tempfile
import avro
import io
import tarfile
import base64
import datetime

import pytest
import pyarrow.parquet as pq

from avro.datafile import DataFileWriter
from streamsets.testframework.markers import aws, sdc_min_version
from streamsets.testframework.utils import get_random_string

# Sandbox prefix for S3 bucket
S3_SANDBOX_PREFIX = 'sandbox'


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.add_stage_lib('streamsets-datacollector-wholefile-transformer-lib')

    return hook


@sdc_min_version('3.5.0')
@aws('s3')
def test_parquet_to_s3(sdc_builder, sdc_executor, aws):
    """Test whole use case - writing records to local filesystem as avro file format and then uploading them to
      S3 converted to Parquet file.

      It's two pipeline solution that we recommend to use:

      raw_data_source >> schema_generator >> local_directory
      local_fs >> whole_file_transformer >> S3
    """
    # Test write directory
    name = get_random_string(string.ascii_letters, 10)
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    tmp_prefix = f'{S3_SANDBOX_PREFIX}/{get_random_string(string.ascii_letters, 10)}'

    # Build pipeline that will generate test record and it's schema
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.stop_after_first_batch = True
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = """{
      "a": 1,
      "b": 2
    }
    """

    # Generate schema for that record
    schema_generator = builder.add_stage('Schema Generator')
    schema_generator.schema_name = 'test_schema'

    # And store it in local file system
    local_fs = builder.add_stage('Local FS', type='destination')
    local_fs.directory_template = tmp_directory
    local_fs.data_format = 'AVRO'
    local_fs.configuration['configs.dataGeneratorFormatConfig.avroSchemaSource'] = 'HEADER'

    # Finish building the pipeline
    dev_raw_data_source >> schema_generator >> local_fs
    write_avro = builder.build('Write Avro File')

    # Second pipeline uploads the generated file to S3
    builder = sdc_builder.get_pipeline_builder()

    directory = builder.add_stage('Directory', type='origin')
    directory.data_format = 'WHOLE_FILE'
    directory.file_name_pattern = 'sdc*'
    directory.files_directory = tmp_directory

    whole_file = builder.add_stage('Whole File Transformer')
    whole_file.job_type = 'AVRO_PARQUET'

    s3 = builder.add_stage('Amazon S3', type='destination')
    s3.bucket = aws.s3_bucket_name
    s3.partition_prefix = tmp_prefix
    s3.data_format = 'WHOLE_FILE'
    s3.file_name_expression = name

    directory >> whole_file >> s3

    upload_to_s3 = builder.build('Upload to S3').configure_for_environment(aws)

    sdc_executor.add_pipeline(write_avro, upload_to_s3)

    # Start first pipeline (generate avro data)
    sdc_executor.start_pipeline(write_avro).wait_for_finished()
    history = sdc_executor.get_pipeline_history(write_avro)
    assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 1
    assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 1

    # Run the second pipeline (convert to parquet and upload it to S3)
    sdc_executor.start_pipeline(upload_to_s3).wait_for_pipeline_output_records_count(1)
    sdc_executor.stop_pipeline(upload_to_s3)
    history = sdc_executor.get_pipeline_history(write_avro)
    assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 1
    assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 1

    # Now let's validate that we successfully uploaded the parquet file to S3
    client = aws.s3
    try:
        # There should be the one file
        list_s3_objs = client.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=tmp_prefix)
        assert len(list_s3_objs['Contents']) == 1

        # That one file must be larger then 0 bytes (SDC-10732)
        assert list_s3_objs['Contents'][0]['Size'] > 0

        # Assert content of the parquet file
        # TODO: STF-731: Add support to read and parse Parquet files in the STF

    finally:
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in client.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=tmp_prefix)['Contents']]}
        client.delete_objects(Bucket=aws.s3_bucket_name, Delete=delete_keys)


@sdc_min_version('5.7.0')
def test_datetime_ms(sdc_builder, sdc_executor):
    """ This test ensures that when an avro file is transfored to Parquet, the date fields maintains the milliseconds 
    and not ignored as spotted in INT-2361 """

    if getattr(sdc_executor, 'container', None) == None:
        pytest.fail('As for now this tests only can be executed when running SDC along with STF using the '
                    '--sdc-version flag. The reason is that did not find a way to copy binary data into the '
                    'executor end (use an auxiliar pipeline seems like a bad practise) apart from using the docker '
                    '`put_archive` utility.')
    milliseconds = 54
    test_data = {"some_date": datetime.datetime.fromtimestamp((1600668873000 + milliseconds)/1000).astimezone()}

    """ Write Avro file to SDC end so that the pipeline can read it """
    avro_schema = """
    { 
        "type" : "record", 
        "name" : "SampleRecord", 
        "namespace" : "avropoc.example", 
        "fields" : [ {"name" : "some_date", "type":{"type":"long","logicalType":"timestamp-millis"}} ]
    }
    """

    bytes_writer = io.BytesIO()
    writer2 = DataFileWriter(bytes_writer, avro.io.DatumWriter(), avro.schema.parse(avro_schema))
    writer2.append(test_data)
    writer2.flush()

    tar_fileobj = io.BytesIO()   
    with tarfile.open(fileobj=tar_fileobj, mode="w|") as tar:
        tf = tarfile.TarInfo('test_datetime.avro')
        tf.size = len(bytes_writer.getvalue())
        bytes_writer.seek(0)
        tar.addfile(tf, bytes_writer)  
    tar_fileobj.seek(0) 

    sdc_executor.docker_client.put_archive(container=sdc_executor.container.id, 
                                           path='/tmp',
                                           data=tar_fileobj)

    """ And read avro file from a pipeline to convert it to parquet """
    builder = sdc_builder.get_pipeline_builder()

    directory = builder.add_stage('Directory', type='origin')
    directory.set_attributes(files_directory='/tmp', 
                             file_name_pattern='test_datetime.avro',
                             data_format='WHOLE_FILE')
    whole_file_transformer = builder.add_stage('Whole File Transformer')
    whole_file_transformer.job_type = 'AVRO_PARQUET'

    local_fs = builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(directory_template='/tmp',
                            data_format='WHOLE_FILE',
                            file_name_expression='test_datetime.parquet',
                            files_prefix='')


    directory >> whole_file_transformer >> local_fs
    pipeline = builder.build(title='Avro to Parquet datetime test')
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'output_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        with tempfile.NamedTemporaryFile() as tmp:
             base64_parquet = sdc_executor.execute_shell('cat /tmp/test_datetime.parquet | openssl base64 -A').stdout
             tmp.write(base64.b64decode(base64_parquet))
             tmp.flush()
             parquet = pq.read_table(tmp)

        assert int(str(parquet.to_pylist()[0]['some_date'].timestamp())[-3:]) == milliseconds
    finally:
        sdc_executor.remove_pipeline(pipeline)
