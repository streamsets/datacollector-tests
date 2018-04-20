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

"""The tests in this module follow a pattern of creating pipelines with
:py:obj:`testframework.sdc_models.PipelineBuilder` in one version of SDC and then importing and running them in
another.
"""
import logging
import string
import time

import pytest
import sqlalchemy
from streamsets.testframework.markers import cluster, parcelpackaging, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

pytestmark = [parcelpackaging]

@cluster('cdh', 'hdp')
@sdc_min_version('3.2.0.0')
@pytest.mark.timeout(600)
def test_avro_orc_mapreduce_executor(sdc_builder, sdc_executor, cluster):
    """Tests the Avro to ORC MapReduce job type.

    Generates random records with various data types (using the Dev Data Generator) and writes them to HDFS files
    in Avro format.  The HDFS destination rolls the files every N records and kicks off the MapReduce executor to
    convert these Avro files to ORC.  The test then tries to examine the output directory and ensure the expected
    number of ORC files eventually appear.

    For now, we only check for the presence of the expected number of ORC files.  Eventulaly (when STF-439 is done)
    we will be able to validate the contents of these files upon completion.
    """
    builder = sdc_builder.get_pipeline_builder()
    hdfs_directory = '/tmp/out/{}'.format(get_random_string(string.ascii_letters, 10))
    dev_data_generator = builder.add_stage('Dev Data Generator')
    dev_data_generator.fields_to_generate = [
        {'field': 'field1', 'precision': 10, 'scale': 2, 'type': 'STRING'},
        {'field': 'field2', 'precision': 10, 'scale': 2, 'type': 'DATETIME'},
        {'field': 'field3', 'precision': 10, 'scale': 2, 'type': 'INTEGER'},
        {'field': 'field4', 'precision': 10, 'scale': 2, 'type': 'DECIMAL'},
        {'field': 'field5', 'precision': 10, 'scale': 2, 'type': 'DOUBLE'}
    ]
    avro_schema = ('{ "type" : "record", "name" : "Random_STF", "fields" : ['
                   '{ "name" : "field1", "type" : "string" },'
                   '{ "name" : "field2", "type" : {"type": "long", "logicalType": "timestamp"} },'
                   '{ "name" : "field3", "type" : "int" },'
                   '{ "name" : "field4", "type" : {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2} },'
                   '{ "name" : "field5", "type" : "double" }'
                   ' ] }')

    batch_size = 10000
    records_per_avro_file = batch_size * 10
    # we will run for long enough to generate 3 total files (2 full and 1 partial)
    total_records = 2.5 * records_per_avro_file
    dev_data_generator.set_attributes(delay_between_batches=0, batch_size=batch_size)

    hadoop_fs = builder.add_stage('Hadoop FS', type='destination')
    # max_records_in_file enables to close the file and generate the event
    hadoop_fs.set_attributes(avro_schema=avro_schema, avro_schema_location='INLINE', data_format='AVRO',
                             directory_template=hdfs_directory, files_prefix='sdc-${sdc:id()}', files_suffix='avro',
                             max_records_in_file=records_per_avro_file)
    mapreduce = builder.add_stage('MapReduce', type='executor')
    mapreduce.job_type = 'AVRO_ORC'
    mapreduce.output_directory = hdfs_directory

    dev_data_generator >> hadoop_fs >= mapreduce

    pipeline = builder.build(title='ORC MapReduce executor pipeline').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    try:
        snapshot = sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(total_records, timeout_sec=3600)
        sdc_executor.stop_pipeline(pipeline).wait_for_stopped()

        hdfs_files = []
        sleep_time = 10
        max_wait_iters = 50
        for wait_iter in range(max_wait_iters):
            hdfs_files = cluster.hdfs.client.list(hdfs_directory)
            logger.info('List of files in %s directory: %s', hdfs_directory, ",".join(hdfs_files))
            total_orc_files = sum(1 for hdfs_file in hdfs_files if hdfs_file.endswith('orc'))
            if total_orc_files == 3:
                break
            else:
                logger.info('Waiting for %d seconds (up to %d more times) for all files in %s to be converted to ORC',
                             sleep_time, max_wait_iters - wait_iter, hdfs_directory)
                time.sleep(sleep_time)
        else:
            pytest.fail('Reached %s iterations without reaching expected number of orc files (saw %s)',
                        max_wait_iters,
                        total_orc_files)

        #TODO: also check contents of ORC files once STF-439 is done

    finally:
        # remove HDFS files
        cluster.hdfs.client.delete(hdfs_directory, recursive=True)
