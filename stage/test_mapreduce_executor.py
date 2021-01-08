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

import json
import logging
import string

import pytest
from streamsets.testframework.markers import cluster
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


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
