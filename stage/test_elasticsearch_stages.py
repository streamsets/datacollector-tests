# Copyright 2017 StreamSets Inc.
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
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

from elasticsearch_dsl import DocType, Index, Search as ESSearch

from testframework.markers import elasticsearch
from testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@elasticsearch
def test_elasticsearch_origin(sdc_builder, sdc_executor, elasticsearch):
    """Test for Elasticsearch origin stage. We do so by putting data via Elastisearch client and reading via
    Elastisearch origin pipeline. To assert, we will snapshot the pipeline.
    The pipeline looks like:

    Elasticsearch origin pipeline:
        es_origin >> trash
    """
    # Test static
    es_index = get_random_string(string.ascii_letters, 10).lower() # Elasticsearch indexes must be lower case
    es_doc_id = get_random_string(string.ascii_letters, 10)
    raw_str = 'Hello World!'

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    es_origin = builder.add_stage('Elasticsearch', type='origin')
    es_origin.set_attributes(index=es_index, query="{'query': {'match_all': {}}}")

    trash = builder.add_stage('Trash')

    es_origin >> trash
    es_origin_pipeline = builder.build(title='ES origin pipeline').configure_for_environment(elasticsearch)
    sdc_executor.add_pipeline(es_origin_pipeline)

    try:
        # Put data to Elasticsearch
        elasticsearch.connect()
        doc_type = DocType(meta={'id':es_doc_id, 'index':es_index})
        doc_type.body = raw_str
        assert doc_type.save() # Assert to saving data in ES

        # Run pipeline and assert
        snapshot = sdc_executor.capture_snapshot(es_origin_pipeline, start_pipeline=True).wait_for_finished().snapshot
        # no need to stop pipeline - as ES origin shuts off once data is read from Elasticsearch
        snapshot_data = snapshot[es_origin.instance_name].output[0].value['value']
        # assert ES meta
        assert snapshot_data['_index']['value'] == es_index and snapshot_data['_id']['value'] == es_doc_id
        # assert ES data
        assert snapshot_data['_source']['value']['body']['value'] == raw_str
    finally:
        # Clean up test data in ES
        idx = Index(es_index)
        idx.delete()


@elasticsearch
def test_elasticsearch_pipeline_errors(sdc_builder, sdc_executor, elasticsearch):
    """Test for a pipeline's error records being pumped to Elasticsearch. We do so by making a Dev Raw Data source
    target to Error stage which would send records to the pipeline configured Elasticsearch error records handling.
    We then assert the error records what we find in Elasticsearch. The pipeline would look like:

    Elasticsearch error pipeline:
        dev_raw_data_source >> error_target
    """
    # Test static
    es_index = get_random_string(string.ascii_letters, 10).lower() # Elasticsearch indexes must be lower case
    es_mapping = get_random_string(string.ascii_letters, 10)
    es_doc_id = get_random_string(string.ascii_letters, 10)
    raw_str = 'Hello World!'

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    errstg = builder.add_error_stage('Write to Elasticsearch')
    errstg.set_attributes(document_id=es_doc_id, index=es_index, mapping=es_mapping)
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                  raw_data=raw_str)
    error_target = builder.add_stage('To Error')

    dev_raw_data_source >> error_target
    es_error_pipeline = builder.build(title='ES error pipeline').configure_for_environment(elasticsearch)
    sdc_executor.add_pipeline(es_error_pipeline)

    try:
        # Run pipeline and read from Elasticsearch to assert
        sdc_executor.start_pipeline(es_error_pipeline).wait_for_pipeline_batch_count(1)
        sdc_executor.stop_pipeline(es_error_pipeline).wait_for_stopped()

        # Since we are upsert on the same index, map, doc - there should only be one document (index 0)
        elasticsearch.connect()
        es_search = ESSearch(index=es_index)
        es_response = es_search.execute()
        es_meta = es_response[0].meta
        # assert meta ingest
        assert es_meta['index'] == es_index and es_meta['doc_type'] == es_mapping and es_meta['id'] == es_doc_id
        # assert data ingest
        assert raw_str == es_response[0].text
    finally:
        # Clean up test data in ES
        idx = Index(es_index)
        idx.delete()


@elasticsearch
def test_elasticsearch_target(sdc_builder, sdc_executor, elasticsearch):
    """Test for Elasticsearch target stage. We do so by ingesting data via Dev Raw Data source to
    Elasticsearch stage and then asserting what we ingest to what will be read from Elasticsearch.
    The pipeline looks like:

    Elasticsearch target pipeline:
        dev_raw_data_source >> es_target
    """
    # Test static
    es_index = get_random_string(string.ascii_letters, 10).lower() # Elasticsearch indexes must be lower case
    es_mapping = get_random_string(string.ascii_letters, 10)
    es_doc_id = get_random_string(string.ascii_letters, 10)
    raw_str = 'Hello World!'

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                  raw_data=raw_str)
    es_target = builder.add_stage('Elasticsearch', type='destination')
    es_target.set_attributes(default_operation='INDEX', document_id=es_doc_id, index=es_index, mapping=es_mapping)

    dev_raw_data_source >> es_target
    es_target_pipeline = builder.build(title='ES target pipeline').configure_for_environment(elasticsearch)

    sdc_executor.add_pipeline(es_target_pipeline)

    try:
        # Run pipeline and read from Elasticsearch to assert
        sdc_executor.start_pipeline(es_target_pipeline).wait_for_pipeline_batch_count(1)
        sdc_executor.stop_pipeline(es_target_pipeline).wait_for_stopped()

        # Since we are upsert on the same index, map, doc - there should only be one document (index 0)
        elasticsearch.connect()
        es_search = ESSearch(index=es_index)
        es_response = es_search.execute()
        es_meta = es_response[0].meta
        # assert meta ingest
        assert es_meta['index'] == es_index and es_meta['doc_type'] == es_mapping and es_meta['id'] == es_doc_id
        # assert data ingest
        assert raw_str == es_response[0].text
    finally:
        # Clean up test data in ES
        idx = Index(es_index)
        idx.delete()
