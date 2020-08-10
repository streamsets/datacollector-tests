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

"""A module to test Navigator support by StreamSets Data Collector."""

import time
import json
import logging
import os
import string

from streamsets.testframework.markers import navigator
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

TENNIS_CHAMPIONS_DATA = [
    {'name': 'Roger Federer', 'country': 'Switzerland'},
    {'name': 'Rafael Nadal',  'country': 'Spain'},
    {'name': 'Alexander Zverev', 'country': 'Germany'}
]


def _create_hadoop_fs_dest_pipeline(pipeline_builder, pipeline_title, hdfs_directory, hadoop_fs):
    """Helper function to create and return a pipeline with Hadoop FS destination
    The Deduplicator assures there is only one ingest to HDFS. The pipeline looks like:
        dev_raw_data_source >> record_deduplicator >> hadoop_fs
                                                   >> trash
    """
    raw_data = '\n'.join(json.dumps(champion) for champion in TENNIS_CHAMPIONS_DATA)
    logger.debug('Pipeline will write to HDFS directory %s ...', hdfs_directory)

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data)

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')

    # triggered the destination file to be closed after writing all data.
    hadoop_fs.set_attributes(max_records_in_file=len(TENNIS_CHAMPIONS_DATA))

    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> record_deduplicator >> hadoop_fs
    record_deduplicator >> trash
    return pipeline_builder.build(title=pipeline_title)


@navigator
def test_navigator_basic(sdc_builder, sdc_executor, cluster):
    """Test Navigator support in SDC using Hadoop FS destination with file prefix and suffix.
    The data is written to a HDFS file.
    Then lineage event details are fetched using CM Navigator REST API and later validated.
    """
    hdfs_directory = f'/tmp/out/{get_random_string(string.ascii_letters, 10)}'
    files_prefix, files_suffix = 'stages', 'txt'
    pipeline_builder = sdc_builder.get_pipeline_builder()

    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(data_format='JSON',
                             directory_template=hdfs_directory,
                             files_prefix=files_prefix,
                             files_suffix=files_suffix)

    pipeline = _create_hadoop_fs_dest_pipeline(pipeline_builder,
                                               'Hadoop FS Destination',
                                               hdfs_directory,
                                               hadoop_fs)
    sdc_executor.add_pipeline(pipeline.configure_for_environment(cluster))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(TENNIS_CHAMPIONS_DATA))

        # Fetch entities with a query using pipeline id.
        query_str = f'"properties":"pipelineId": "{pipeline.id}"'
        entities = cluster.navigator.get_entities(query=query_str)
        extractor_run_id = entities[0]['extractorRunId']

        # Fetch specific entities with the above extractorRunId and HDFS directory specified in pipeline.
        query_str = f'(extractorRunId:"{extractor_run_id}")AND("parentPath": "{hdfs_directory}")'
        entities = cluster.navigator.get_entities(query=query_str)
        entity = entities[0]

        # Check validity of data in lineage events entity fetched above.
        hdfs_fs_files = cluster.hdfs.client.list(hdfs_directory)
        assert len(hdfs_fs_files) == 1
        hdfs_fs_filename_expected = hdfs_fs_files[0]

        hdfs_fs_filepath_actual = entity['description']
        assert os.path.join(hdfs_directory, hdfs_fs_filename_expected) == hdfs_fs_filepath_actual
        assert hdfs_fs_filepath_actual.startswith(os.path.join(hdfs_directory, files_prefix))
        assert hdfs_fs_filepath_actual.endswith(files_suffix)

        assert 'admin' == entity['properties']['pipeline_user']
        assert 'sdc' == entity['packageName']
        assert 'sdc_dataset' == entity['metaClassName']

    finally:
        sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting Hadoop FS directory %s ...', hdfs_directory)
        cluster.hdfs.client.delete(hdfs_directory, recursive=True)


@navigator
def test_navigator_empty_params(sdc_builder, sdc_executor, cluster):
    """ Test that SDC can publish events to Cloudera Navigator when the pipeline has empty parameters.

        Pipeline looks like:

        dev_raw_data_source >> record_deduplicator >> hadoop_fs
        record_deduplicator >> trash
    """
    hdfs_directory = f'/tmp/out/{get_random_string(string.ascii_letters, 10)}'
    files_prefix, files_suffix = 'stages', 'txt'
    pipeline_builder = sdc_builder.get_pipeline_builder()

    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(data_format='JSON',
                             directory_template=hdfs_directory,
                             files_prefix=files_prefix,
                             files_suffix=files_suffix)

    pipeline = _create_hadoop_fs_dest_pipeline(pipeline_builder,
                                               'Cloudera Navigator Empty Parameters',
                                               hdfs_directory,
                                               hadoop_fs)
    pipeline.add_parameters(fields='f1,f2,f3', fromField='/f1', toField='/f1changed', emptyField1='', emptyField2='')
    sdc_executor.add_pipeline(pipeline.configure_for_environment(cluster))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(TENNIS_CHAMPIONS_DATA))

        # Fetch entities with a query using pipeline id.
        query_str = f'"properties":"pipelineId": "{pipeline.id}"'
        entities = cluster.navigator.get_entities(query=query_str)
        extractor_run_id = entities[0]['extractorRunId']

        # Fetch specific entities with the above extractorRunId and HDFS directory specified in pipeline.
        query_str = f'(extractorRunId:"{extractor_run_id}")AND("parentPath": "{hdfs_directory}")'
        entities = cluster.navigator.get_entities(query=query_str)

        if len(entities) == 0:
            time.sleep(10)
            entities = cluster.navigator.get_entities(query=query_str)

        entity = entities[0]

        # Check validity of data in lineage events entity fetched above.
        hdfs_fs_files = cluster.hdfs.client.list(hdfs_directory)
        assert len(hdfs_fs_files) == 1
        hdfs_fs_filename_expected = hdfs_fs_files[0]

        hdfs_fs_filepath_actual = entity['description']
        assert os.path.join(hdfs_directory, hdfs_fs_filename_expected) == hdfs_fs_filepath_actual
        assert hdfs_fs_filepath_actual.startswith(os.path.join(hdfs_directory, files_prefix))
        assert hdfs_fs_filepath_actual.endswith(files_suffix)

        assert 'admin' == entity['properties']['pipeline_user']
        assert 'sdc' == entity['packageName']
        assert 'sdc_dataset' == entity['metaClassName']

    finally:
        sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting Hadoop FS directory %s ...', hdfs_directory)
        cluster.hdfs.client.delete(hdfs_directory, recursive=True)


@navigator
def test_navigator_label_contains_invalid_characters(sdc_builder, sdc_executor, cluster):
    """ Test that SDC can publish events to Cloudera Navigator when the pipeline has a label with invalid characters
    like 'xavi/test' where the slash '/' would be an invalid character.

        Pipeline looks like:

        dev_raw_data_source >> record_deduplicator >> hadoop_fs
        record_deduplicator >> trash
    """
    hdfs_directory = f'/tmp/out/{get_random_string(string.ascii_letters, 10)}'
    files_prefix, files_suffix = 'stages', 'txt'
    pipeline_builder = sdc_builder.get_pipeline_builder()

    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(data_format='JSON',
                             directory_template=hdfs_directory,
                             files_prefix=files_prefix,
                             files_suffix=files_suffix)

    pipeline = _create_hadoop_fs_dest_pipeline(pipeline_builder,
                                               'Cloudera Navigator Invalid Characters in Label',
                                               hdfs_directory,
                                               hadoop_fs)
    pipeline.metadata['labels'] = ['xavi/test']
    sdc_executor.add_pipeline(pipeline.configure_for_environment(cluster))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(TENNIS_CHAMPIONS_DATA))

        # Fetch entities with a query using pipeline id.
        query_str = f'"properties":"pipelineId": "{pipeline.id}"'
        entities = cluster.navigator.get_entities(query=query_str)
        extractor_run_id = entities[0]['extractorRunId']

        # Fetch specific entities with the above extractorRunId and HDFS directory specified in pipeline.
        query_str = f'(extractorRunId:"{extractor_run_id}")AND("parentPath": "{hdfs_directory}")'
        entities = cluster.navigator.get_entities(query=query_str)
        entity = entities[0]

        # Check validity of data in lineage events entity fetched above.
        hdfs_fs_files = cluster.hdfs.client.list(hdfs_directory)
        assert len(hdfs_fs_files) == 1
        hdfs_fs_filename_expected = hdfs_fs_files[0]

        hdfs_fs_filepath_actual = entity['description']
        assert os.path.join(hdfs_directory, hdfs_fs_filename_expected) == hdfs_fs_filepath_actual
        assert hdfs_fs_filepath_actual.startswith(os.path.join(hdfs_directory, files_prefix))
        assert hdfs_fs_filepath_actual.endswith(files_suffix)

        assert 'admin' == entity['properties']['pipeline_user']
        assert 'sdc' == entity['packageName']
        assert 'sdc_dataset' == entity['metaClassName']

    finally:
        sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting Hadoop FS directory %s ...', hdfs_directory)
        cluster.hdfs.client.delete(hdfs_directory, recursive=True)
