# Copyright 2024 StreamSets Inc.
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
import pytest
from collections import OrderedDict
from streamsets.sdk.exceptions import StartError

from streamsets.sdk.utils import Version
from streamsets.testframework.markers import sdc_min_version

logger = logging.getLogger(__name__)

# pylint: disable=pointless-statement, too-many-locals

def test_field_pivoter(sdc_builder, sdc_executor):
    """Test field pivoter processor. The pipeline would look like:

        dev_raw_data_source >> field_pivoter >> wiretap

    With the given config, data will pivot at field_to_pivot (/ballpoint/color_list). In this case by creating
    3 records (one for each of ['black', 'blue', 'red']) for one input record.
    """
    raw_dict = dict(ballpoint=dict(color_list=['black', 'blue', 'red'], unit_cost='.10'))
    raw_data = json.dumps(raw_dict)
    field_to_pivot = '/ballpoint/color_list'
    field_name_path = '/ballpoint/color_list_path'
    pivoted_item_path = '/ballpoint/color'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_pivoter = pipeline_builder.add_stage('Field Pivoter')
    field_pivoter.set_attributes(copy_all_fields=True,
                                 field_to_pivot=field_to_pivot,
                                 original_field_name_path=field_name_path,
                                 pivoted_items_path=pivoted_item_path,
                                 save_original_field_name=True)
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_pivoter >> wiretap.destination
    pipeline = pipeline_builder.build('test_field_pivoter pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # We should generate 3 records
        assert len(wiretap.output_records) == 3

        # Some fields are different in each record
        assert wiretap.output_records[0].get_field_data('/ballpoint/color') == 'black'
        assert wiretap.output_records[1].get_field_data('/ballpoint/color') == 'blue'
        assert wiretap.output_records[2].get_field_data('/ballpoint/color') == 'red'

        # While others should be exactly the same
        for i in range(3):
            assert wiretap.output_records[i].get_field_data('/ballpoint/unit_cost') == '.10'
            assert wiretap.output_records[i].get_field_data('/ballpoint/color_list_path') == '/ballpoint/color_list'
    finally:
        sdc_executor.remove_pipeline(pipeline)


def test_field_pivoter_list_map(sdc_builder, sdc_executor):
    """Test field pivoter processor for list map. The pipeline would look like:

        dev_raw_data_source >> field_pivoter >> wiretap
    """
    raw_data = """{
                     "list_field":[{"a": "aVal"},{ "b": "bVal"}]
                  }"""

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_pivoter = pipeline_builder.add_stage('Field Pivoter')
    field_pivoter.set_attributes(copy_all_fields=False,
                                 field_to_pivot='/list_field')
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_pivoter >> wiretap.destination
    pipeline = pipeline_builder.build('test_field_pivoter_list_map pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # We should generate 2 records
        assert len(wiretap.output_records) == 2
        assert wiretap.output_records[0].field == {"a": "aVal"}
        assert wiretap.output_records[1].field == {"b": "bVal"}
    finally:
        sdc_executor.remove_pipeline(pipeline)


@pytest.mark.parametrize('pivoted_items_path', ['','/op'])
def test_field_pivoter_copy_fields(sdc_builder, sdc_executor, pivoted_items_path):
    """Test field pivoter processor for copy fields. The pipeline would look like:

        dev_raw_data_source >> field_pivoter >> wiretap
    """
    raw_data = """{
                     "list_field":[{"a": "aVal"},{ "b": "bVal"}],
                     "copied":"rVal"
                  }"""

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_pivoter = pipeline_builder.add_stage('Field Pivoter')
    field_pivoter.set_attributes(copy_all_fields=True,
                                 field_to_pivot='/list_field',
                                 pivoted_items_path=pivoted_items_path)
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_pivoter >> wiretap.destination
    pipeline = pipeline_builder.build('test_field_pivoter_copy_fields pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # We should generate 2 records
        assert len(wiretap.output_records) == 2

        if pivoted_items_path == '/op':
            assert wiretap.output_records[0].field == {"copied":"rVal", "op":{"a": "aVal"}}
            assert wiretap.output_records[1].field == {"copied":"rVal", "op":{"b": "bVal"}}
        else:
            assert wiretap.output_records[0].field == {"copied":"rVal", "list_field":{"a": "aVal"}}
            assert wiretap.output_records[1].field == {"copied":"rVal", "list_field":{"b": "bVal"}}
    finally:
        sdc_executor.remove_pipeline(pipeline)


def test_field_pivoter_map_pivot(sdc_builder, sdc_executor):
    """Test field pivoter processor for map pivot. The pipeline would look like:

        dev_raw_data_source >> field_pivoter >> wiretap
    """
    raw_data = """{
                     "map_field":{"a": "aVal", "b": "bVal"}
                  }"""

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_pivoter = pipeline_builder.add_stage('Field Pivoter')
    field_pivoter.set_attributes(copy_all_fields=True,
                                 field_to_pivot='/map_field',
                                 save_original_field_name=True,
                                 original_field_name_path='/map_field_name')
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_pivoter >> wiretap.destination
    pipeline = pipeline_builder.build('test_field_pivoter_map_pivot pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # We should generate 2 records
        assert len(wiretap.output_records) == 2
        assert wiretap.output_records[0].field == {"map_field":"aVal", "map_field_name": "a"}
        assert wiretap.output_records[1].field == {"map_field":"bVal", "map_field_name": "b"}
    finally:
        sdc_executor.remove_pipeline(pipeline)


@pytest.mark.parametrize('copy_all_fields, field_to_pivot, pivoted_items_path, original_field_name_path',
[(False,'/map_field','','/map_field_name'),
 (True, '/a','/same','/same')
])
def test_field_pivoter_invalid_config(sdc_builder, sdc_executor, copy_all_fields,
                                      field_to_pivot,pivoted_items_path, original_field_name_path):
    """Test field pivoter processor for invalid config. The pipeline would look like:

        dev_raw_data_source >> field_pivoter >> wiretap
    """
    raw_data = "{'a':'b'}"

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_pivoter = pipeline_builder.add_stage('Field Pivoter')
    field_pivoter.set_attributes(copy_all_fields=copy_all_fields,
                                 field_to_pivot=field_to_pivot,
                                 save_original_field_name=True,
                                 pivoted_items_path=pivoted_items_path,
                                 original_field_name_path=original_field_name_path)
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_pivoter >> wiretap.destination
    pipeline = pipeline_builder.build('test_field_pivoter_invalid_config pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        with pytest.raises(StartError) as error:
          sdc_executor.start_pipeline(pipeline).wait_for_finished()
        if copy_all_fields == True:
            #LIST_PIVOT_03 -"Pivoted Items Path and Original Field Name Path can't contain the same value"
            assert error.value.message.startswith('LIST_PIVOT_03')
        else:
            #LIST_PIVOT_02 -"Cannot save original field name without copying all fields."
            assert error.value.message.startswith('LIST_PIVOT_02')
    finally:
        sdc_executor.remove_pipeline(pipeline)