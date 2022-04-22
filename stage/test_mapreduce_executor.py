# Copyright 2018 StreamSets Inc.
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

import hdfs
import json
import logging
import io
import parquet

import pytest
from streamsets.testframework.markers import cluster
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

# taken from AvroParquetConstants when migrating from java IT to STF
TMP_PREFIX = '.avro_to_parquet_tmp_conversion_'


@cluster('cdh')
@pytest.mark.parametrize('compression_codec', ['UNCOMPRESSED', 'SNAPPY', 'GZIP'])
def test_mapreduce_executor(sdc_builder, sdc_executor, cluster, compression_codec):
    """Test MapReduce executor stage on different compression codec.
    After ingest the executor triggers MapReduce job which should convert the ingested HDFS Avro data to Parquet.
    The pipeline would look like:
        dev_raw_data_source >> hadoop_fs >= mapreduce
    """
    hdfs_directory = f'/tmp/out/{get_random_string()}'
    product_data = [dict(name='iphone', price=649.99),
                    dict(name='pixel', price=649.89)]
    raw_data = ''.join([json.dumps(product) for product in product_data])
    avro_schema = ('{ "type" : "record", "name" : "STF", "fields" : '
                   '[ { "name" : "name", "type" : "string" }, { "name" : "price", "type" : "double" } ] }')

    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_data,
                                                                                  stop_after_first_batch=True)
    hadoop_fs = builder.add_stage('Hadoop FS', type='destination')
    # max_records_in_file enables to close the file and generate the event
    hadoop_fs.set_attributes(avro_schema=avro_schema, avro_schema_location='INLINE', data_format='AVRO',
                             directory_template=hdfs_directory, files_prefix='sdc-${sdc:id()}', max_records_in_file=1)
    mapreduce = builder.add_stage('MapReduce', type='executor').set_attributes(job_type='AVRO_PARQUET',
                                                                               output_directory=hdfs_directory,
                                                                               compression_codec=compression_codec)

    wiretap_hadoop = builder.add_wiretap()
    wiretap_mapreduce = builder.add_wiretap()

    dev_raw_data_source >> hadoop_fs >= [mapreduce, wiretap_hadoop.destination]
    mapreduce >= wiretap_mapreduce.destination

    pipeline = builder.build(title='MapReduce executor pipeline').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # assert events (MapReduce) generated
        assert len(wiretap_hadoop.output_records) == len(product_data)

        # make sure MapReduce job is done and is successful
        for event in wiretap_mapreduce.output_records:
            job_id = event.field['job-id'].value
            assert cluster.yarn.wait_for_job_to_end(job_id) == 'SUCCEEDED'

        # assert parquet data is same as what is ingested
        for event in wiretap_hadoop.output_records:
            file_path = event.field['filepath'].value
            hdfs_parquet_file_path = f'{file_path}.parquet'
            hdfs_data = cluster.hdfs.get_data_from_parquet(hdfs_parquet_file_path)
            assert hdfs_data[0] in product_data
    finally:
        # remove HDFS files
        cluster.hdfs.client.delete(hdfs_directory, recursive=True)


@cluster('mapr')
@pytest.mark.parametrize('use_el_expression', [True, False])
def test_mapreduce_executor_avro_to_parquet(sdc_builder, sdc_executor, cluster, use_el_expression):
    """Test MapReduce executor stage when converting avro to parquet. Parquet version dependencies must be set
     accordingly to avro version dependencies, else parquet files are created with no content, and exceptions are
     thrown in hadoop application syslog (test made specifically to test mapr clusters, as CDH clusters used to have
     these dependencies correctly set up before).
     Similar to test above.
     Input file is always an EL by default. To test it hardcoded, we have
     test_mapreduce_executor_avro_to_parquet_fail_if_tmp_file_exists
     After ingest the executor triggers MapReduce job which should convert the ingested HDFS Avro data to Parquet.
     The pipeline would look like:
        dev_raw_data_source >> (expression_evaluator >>) mapr_fs >= mapreduce
    """
    # Generate some data.
    product_data = [dict(name='iphone', price=649.99),
                    dict(name='pixel', price=649.89)]
    raw_data = ''.join([json.dumps(product) for product in product_data])
    avro_schema = ('{ "type" : "record", "name" : "STF", "fields" : '
                   '[ { "name" : "name", "type" : "string" }, { "name" : "price", "type" : "double" } ] }')

    mapr_fs_output_path = f'/tmp/out/{get_random_string()}'
    mapr_fs_output_path_config = "${record:attribute('/output')}" if use_el_expression else mapr_fs_output_path

    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_data,
                                                                                  stop_after_first_batch=True)
    mapr_fs = builder.add_stage('MapR FS', type='destination')
    mapr_fs.set_attributes(avro_schema=avro_schema, avro_schema_location='INLINE', data_format='AVRO',
                           directory_template=mapr_fs_output_path, files_prefix='avro', max_records_in_file=1)
    mapreduce = builder.add_stage(
        'MapReduce', type='executor').set_attributes(job_type='AVRO_PARQUET',
                                                     output_directory=mapr_fs_output_path_config,
                                                     mapreduce_configuration_directory='mapr')

    wiretap_hadoop = builder.add_wiretap()
    wiretap_mapreduce = builder.add_wiretap()

    if use_el_expression:
        expression_evaluator = builder.add_stage('Expression Evaluator')
        expression_evaluator.set_attributes(
            header_attribute_expressions=[{'attributeToSet': '/output',
                                           'headerAttributeExpression': mapr_fs_output_path}])
        dev_raw_data_source >> mapr_fs >= [expression_evaluator, wiretap_hadoop.destination]
        expression_evaluator >> mapreduce
    else:
        dev_raw_data_source >> mapr_fs >= [mapreduce, wiretap_hadoop.destination]

    mapreduce >= wiretap_mapreduce.destination

    pipeline = builder.build(title='MapReduce executor pipeline').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # First, assert mapr_fs files have been created with correct content
        mapr_fs_files = cluster.mapr_fs.client.list(str(mapr_fs_output_path))
        # assert events (MapReduce) generated
        assert len(mapr_fs_files) == len(product_data)

        # make sure MapReduce job is done and is successful
        for event in wiretap_mapreduce.output_records:
            job_id = event.field['job-id'].value
            job_id = job_id.replace('job', 'application')
            assert cluster.yarn.wait_for_job_to_end(job_id) == 'FINISHED'

        # assert parquet data is same as what is ingested
        for event in wiretap_hadoop.output_records:
            file_path = event.field['filepath'].value
            maprfs_parquet_file_path = f'{file_path}.parquet'
            maprfs_output = io.BytesIO()
            with cluster.mapr_fs.client.read(maprfs_parquet_file_path) as reader:
                maprfs_output.write(reader.read())
            maprfs_data = [row for row in parquet.DictReader(maprfs_output)]
            assert maprfs_data[0] in product_data
    finally:
        cluster.mapr_fs.client.delete(mapr_fs_output_path, recursive=True)


@cluster('mapr')
def test_mapreduce_executor_avro_to_parquet_drop_input_file(sdc_builder, sdc_executor, cluster):
    """Very similar to other tests. We will just check the input file is still present after the job has finished.
     After ingest the executor triggers MapReduce job which should convert the ingested HDFS Avro data to Parquet.
     The pipeline would look like:
        dev_raw_data_source >> mapr_fs >= mapreduce
    """
    # Generate some data.
    product_data = [dict(name='iphone', price=649.99),
                    dict(name='pixel', price=649.89)]
    raw_data = ''.join([json.dumps(product) for product in product_data])
    avro_schema = ('{ "type" : "record", "name" : "STF", "fields" : '
                   '[ { "name" : "name", "type" : "string" }, { "name" : "price", "type" : "double" } ] }')

    mapr_fs_output_path = f'/tmp/out/{get_random_string()}'

    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_data,
                                                                                  stop_after_first_batch=True)
    mapr_fs = builder.add_stage('MapR FS', type='destination')
    mapr_fs.set_attributes(avro_schema=avro_schema, avro_schema_location='INLINE', data_format='AVRO',
                           directory_template=mapr_fs_output_path, files_prefix='avro', max_records_in_file=1)
    mapreduce = builder.add_stage(
        'MapReduce', type='executor').set_attributes(job_type='AVRO_PARQUET',
                                                     output_directory=mapr_fs_output_path,
                                                     mapreduce_configuration_directory='mapr',
                                                     keep_avro_input_file=True)

    wiretap_hadoop = builder.add_wiretap()
    wiretap_mapreduce = builder.add_wiretap()

    dev_raw_data_source >> mapr_fs >= [mapreduce, wiretap_hadoop.destination]
    mapreduce >= wiretap_mapreduce.destination

    pipeline = builder.build(title='MapReduce executor pipeline').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # First, assert mapr_fs files have been created with correct content
        mapr_fs_files = cluster.mapr_fs.client.list(str(mapr_fs_output_path))
        # assert events (MapReduce) generated
        assert len(mapr_fs_files) == len(product_data)

        # make sure MapReduce job is done and is successful
        for event in wiretap_mapreduce.output_records:
            job_id = event.field['job-id'].value
            job_id = job_id.replace('job', 'application')
            assert cluster.yarn.wait_for_job_to_end(job_id) == 'FINISHED'

        # assert input file is still there (we have twice the number of files)
        mapreduce_files = cluster.mapr_fs.client.list(str(mapr_fs_output_path))
        assert len(mapreduce_files) == 2 * len(product_data)
    finally:
        cluster.mapr_fs.client.delete(mapr_fs_output_path, recursive=True)


@cluster('mapr')
@pytest.mark.parametrize('overwrite_temporary_file', [True, False])
def test_mapreduce_executor_avro_to_parquet_tmp_file_exists(sdc_builder, sdc_executor, cluster,
                                                            overwrite_temporary_file):
    """Very similar to other tests. We will just check the pipeline works with and without the overwrite_temporary_file.
    If set to True, it will work even if we create a file with the same name. Else, it will do nothing and we will
    have no parquet file.
     After ingest the executor triggers MapReduce job which should convert the ingested HDFS Avro data to Parquet.
     The pipeline would look like:
        dev_raw_data_source >> mapr_fs >= mapreduce
    """
    # Generate some data.
    product_data = [dict(name='iphone', price=649.99)]
    raw_data = ''.join([json.dumps(product) for product in product_data])
    avro_schema = ('{ "type" : "record", "name" : "STF", "fields" : '
                   '[ { "name" : "name", "type" : "string" }, { "name" : "price", "type" : "double" } ] }')

    mapr_fs_output_path = f'/tmp/out/{get_random_string()}'

    builder_1 = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder_1.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                    raw_data=raw_data,
                                                                                    stop_after_first_batch=True)
    mapr_fs = builder_1.add_stage('MapR FS', type='destination')
    mapr_fs.set_attributes(avro_schema=avro_schema, avro_schema_location='INLINE', data_format='AVRO',
                           directory_template=mapr_fs_output_path, files_prefix='avro', max_records_in_file=1)

    wiretap_hadoop = builder_1.add_wiretap()

    dev_raw_data_source >> mapr_fs >= wiretap_hadoop.destination

    pipeline_1 = builder_1.build().configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline_1)

    try:
        sdc_executor.start_pipeline(pipeline_1).wait_for_finished()

        # First, assert mapr_fs files have been created with correct content
        mapr_fs_files = cluster.mapr_fs.client.list(str(mapr_fs_output_path))
        # assert events (MapReduce) generated
        assert len(mapr_fs_files) == len(product_data)
        input_file_path = ''
        for event in wiretap_hadoop.output_records:
            input_file_path = event.field['filepath'].value

        # We create the tmp file based on the input file and the tmp_prefix used by the stage
        tmp_input_file_array = input_file_path.split('/')
        tmp_input_file_array[4] = TMP_PREFIX + tmp_input_file_array[4]
        tmp_input_file = '/'.join(tmp_input_file_array)
        cluster.mapr_fs.client.write(tmp_input_file, 'FILE CONTENTS')

        builder_2 = sdc_builder.get_pipeline_builder()

        dev_raw_data_source = builder_2.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                        raw_data=raw_data,
                                                                                        stop_after_first_batch=True)
        mapreduce = builder_2.add_stage(
            'MapReduce', type='executor').set_attributes(job_type='AVRO_PARQUET',
                                                         output_directory=mapr_fs_output_path,
                                                         mapreduce_configuration_directory='mapr',
                                                         # input file is not an EL this way
                                                         input_avro_file=input_file_path,
                                                         overwrite_temporary_file=overwrite_temporary_file)

        wiretap_mapreduce = builder_2.add_wiretap()

        dev_raw_data_source >> mapreduce >= wiretap_mapreduce.destination

        pipeline_2 = builder_2.build().configure_for_environment(cluster)
        sdc_executor.add_pipeline(pipeline_2)

        sdc_executor.start_pipeline(pipeline_2).wait_for_finished()

        # make sure MapReduce job is done and is successful
        for event in wiretap_mapreduce.output_records:
            job_id = event.field['job-id'].value
            job_id = job_id.replace('job', 'application')
            assert cluster.yarn.wait_for_job_to_end(job_id) == 'FINISHED'

        # assert parquet data is same as what is ingested
        for event in wiretap_hadoop.output_records:
            file_path = event.field['filepath'].value
            maprfs_parquet_file_path = f'{file_path}.parquet'
            maprfs_output = io.BytesIO()
            # with overwrite_temporary_file set to False, there should be no parquet file
            with cluster.mapr_fs.client.read(maprfs_parquet_file_path) as reader:
                maprfs_output.write(reader.read())
            maprfs_data = [row for row in parquet.DictReader(maprfs_output)]
            assert maprfs_data[0] in product_data
    except hdfs.util.HdfsError as e:
        if not overwrite_temporary_file:
            # meaning there is no parquet file as pipeline did not do anything
            assert input_file_path in e.message
        else:
            raise e
    finally:
        cluster.mapr_fs.client.delete(mapr_fs_output_path, recursive=True)
