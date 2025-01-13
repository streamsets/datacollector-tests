# Copyright 2025 StreamSets Inc.
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
import pytest
from collections import ChainMap

from streamsets.testframework.markers import sdc_min_version

logger = logging.getLogger(__name__)


# pylint: disable=pointless-statement, too-many-locals

@sdc_min_version('3.8.0')
def test_field_mapper_self_referencing_expression(sdc_builder, sdc_executor):
    """
    Field Mapper supports self-refercing expressions and we need to make sure that they work properly and don't
    run into StackOverflowError (which can happen with some optimizations like SDC-14645).
    """
    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('Dev Raw Data Source')
    source.data_format = 'JSON'
    source.raw_data = '{"key": "value", "old": "exists"}'
    source.stop_after_first_batch = True

    mapper = builder.add_stage('Field Mapper', type='processor')
    mapper.set_attributes(
        operate_on='FIELD_VALUES',
        conditional_expression='${f:name() == "key"}',
        mapping_expression='${record:value("/")}'
    )

    wiretap = builder.add_wiretap()

    source >> mapper >> wiretap.destination

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 1
    assert wiretap.output_records[0].get_field_data('/key/key') == 'value'
    assert wiretap.output_records[0].get_field_data('/old') == 'exists'
    assert wiretap.output_records[0].get_field_data('/key/old') == 'exists'


@sdc_min_version('3.8.0')
def test_field_mapper_min_max(sdc_builder, sdc_executor):
    """
    Test the Field Mapper processor, by path, with an aggregate expression and preserving original paths.

    This pipeline calculates the minimum and maximum value of integer fields whose name contains the word "value".

    The pipeline that will be constructed is:
    The pipeline that will be constructed is:

        dev_raw_data_source (JSON data) >> field_mapper (max value) >> field_mapper (min value)
        field_mapper (max value) >> wiretap
        field_mapper (min value) >> wiretap
    """
    raw_data = """{
      "sensor_id": "abc123",
      "sensor_readings": [
        {
          "reading_id": "def456",
          "value": 87
        },{
          "reading_id": "ghi789",
          "values": [-5, 17, 19]
        },{
          "reading_id": "jkl012",
          "values": [99, -107, 50]
        }
      ],
      "outputs": {}
    }"""

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)
    field_mapper_max = pipeline_builder.add_stage('Field Mapper', type='processor')
    field_mapper_max.set_attributes(
        operate_on='FIELD_PATHS',
        conditional_expression='${f:type() == \'INTEGER\' and str:startsWith(f:name(), \'value\')}',
        mapping_expression='/outputs/max',
        aggregation_expression='${max(fields)}',
        maintain_original_paths=True
    )
    field_mapper_min = pipeline_builder.add_stage('Field Mapper', type='processor')
    field_mapper_min.set_attributes(
        operate_on='FIELD_PATHS',
        conditional_expression='${f:type() == \'INTEGER\' and str:startsWith(f:name(), \'value\')}',
        mapping_expression='/outputs/min',
        aggregation_expression='${min(fields)}',
        maintain_original_paths=True
    )

    wiretap1 = pipeline_builder.add_wiretap()
    wiretap2 = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_mapper_max >> [field_mapper_min, wiretap1.destination]
    field_mapper_min >> wiretap2.destination

    pipeline = pipeline_builder.build('Field mapper - sensor reading value min and max pipeline')
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    sdc_executor.remove_pipeline(pipeline)

    max_processor_output = wiretap1.output_records
    assert max_processor_output[0].get_field_data('/outputs/max').type == 'LONG'
    assert max_processor_output[0].get_field_data('/outputs/max').value == 99
    # ensure original field was left in place
    assert max_processor_output[0].get_field_data('/sensor_readings[2]/values[0]').value == 99

    min_processor_output = wiretap2.output_records
    assert min_processor_output[0].get_field_data('/outputs/min').type == 'LONG'
    assert min_processor_output[0].get_field_data('/outputs/min').value == -107
    # ensure original field was left in place
    assert min_processor_output[0].get_field_data('/sensor_readings[2]/values[1]').value == -107


@sdc_min_version('3.8.0')
def test_field_mapper_gather_paths_with_predicate(sdc_builder, sdc_executor):
    """
    Test the Field Mapper processor, by path, with an aggregate expression (using previousPath and value to construct a
    new map), and preserving original paths.

    This pipeline attempts to find all occurences of String field values, or field names, containing "dave" and captures
    their original path in the record along with the original field value

    The pipeline that will be constructed is:

        dev_raw_data_source (JSON data) >> field_mapper (find Daves) >> wiretap
    """
    raw_data = """{
      "first": {
        "firstSub1": {
          "foo": 1,
          "bar": 2,
          "baz": ["John", "Mike", "Mary", "Dave Smith"]
        },
        "firstSub2": {
          "one": 14,
          "two": {
            "name": "Joe",
            "name2": "Dave Johnson"
          }
        }
      },
      "second": {
        "karen": 18,
        "secondSub": {
          "Dave": "Richardson"
        }
      },
      "outputs": {
        "daves": []
      }
    }"""

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)
    field_mapper = pipeline_builder.add_stage('Field Mapper', type='processor')
    field_mapper.set_attributes(
        operate_on='FIELD_PATHS',
        conditional_expression='${str:contains(str:toLower(f:name()), \'dave\') or' +
                               '(f:type() == \'STRING\' and str:contains(str:toLower(f:value()), \'dave\'))}',
        mapping_expression='/outputs/daves',
        aggregation_expression='${map(fields, fieldByPreviousPath())}',
        maintain_original_paths=True
    )
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_mapper >> wiretap.destination

    pipeline = pipeline_builder.build('Field mapper - The Daves I Know')
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    sdc_executor.remove_pipeline(pipeline)

    field_mapper_output = wiretap.output_records
    daves = field_mapper_output[0].get_field_data('/outputs/daves')
    assert len(daves) == 3
    assert daves[0]['/first/firstSub1/baz[3]'] == 'Dave Smith'
    assert daves[1]['/first/firstSub2/two/name2'] == 'Dave Johnson'
    assert daves[2]['/second/secondSub/Dave'] == 'Richardson'


@sdc_min_version('3.8.0')
def test_field_mapper_sanitize_names(sdc_builder, sdc_executor):
    """
    Test the Field Mapper processor, by name.  For purposes of this test, sanitizing means replacing all "z"s in field
    names with "2".

    The pipeline that will be constructed is:

        dev_raw_data_source (JSON data) >> field_mapper (sanitize names) >> wiretap
    """
    raw_data = """{
      "first": {
        "firstSub1": {
          "zug": 1,
          "barz": 2,
          "baz": ["blah", "blech", "blargh"]
        },
        "firstSub2": {
          "one": 14,
          "twoz": {
            "name": "Joe",
            "name2": "Dave Johnson"
          }
        }
      },
      "second": {
        "karen": 18,
        "secondSubz": {
          "Dave": "Richardson"
        }
      }
    }"""

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)
    field_mapper = pipeline_builder.add_stage('Field Mapper', type='processor')
    field_mapper.set_attributes(
        operate_on='FIELD_NAMES',
        mapping_expression='${str:replaceAll(f:name(), \'z\', \'2\')}',
        maintain_original_paths=False
    )
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_mapper >> wiretap.destination

    pipeline = pipeline_builder.build('Field mapper - The Daves I Know')
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    sdc_executor.remove_pipeline(pipeline)

    field_mapper_output = wiretap.output_records
    output_record = field_mapper_output[0];
    first_sub_1 = output_record.get_field_data('/first/firstSub1')
    assert first_sub_1.get('zug') == None
    assert first_sub_1.get('2ug') == 1
    assert first_sub_1.get('barz') == None
    assert first_sub_1.get('bar2') == 2
    assert first_sub_1.get('baz') == None
    assert first_sub_1.get('ba2')[0] == "blah"
    assert first_sub_1.get('ba2')[1] == "blech"
    assert first_sub_1.get('ba2')[2] == "blargh"

    first_sub_2 = output_record.get_field_data('/first/firstSub2')
    assert first_sub_2.get('twoz') == None
    assert first_sub_2.get('two2').get('name') == 'Joe'

    second = output_record.get_field_data('second')
    assert second.get('secondSubz') == None
    assert second.get('secondSub2').get('Dave') == 'Richardson'


def test_field_mapper_names_ignore(sdc_builder, sdc_executor):
    """
    Test the Field Mapper processor, by name. Set a condition that matches the root field and list elements and
    check that they are ignored since they have no names.

    (SDC-16864)

    The pipeline that will be constructed is:

        dev_raw_data_source (JSON data) >> field_mapper >> wiretap
    """
    raw_data = '{ "first": ["blah", "blech", "blargh"] }'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)
    field_mapper = pipeline_builder.add_stage('Field Mapper', type='processor')
    field_mapper.set_attributes(
        operate_on='FIELD_NAMES',
        conditional_expression="${str:matches(f:name(),'req_.*') == false}",
        mapping_expression='data_${f:name()}'
    )
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_mapper >> wiretap.destination

    pipeline = pipeline_builder.build()
    try:
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        field_mapper_output = wiretap.output_records
        output_field = field_mapper_output[0].field;
        assert 'first' not in output_field
        assert output_field['data_first'][0] == "blah"
        assert output_field['data_first'][1] == "blech"
        assert output_field['data_first'][2] == "blargh"
    finally:
        sdc_executor.remove_pipeline(pipeline)


@sdc_min_version('3.8.0')
def test_field_mapper_operate_on_values(sdc_builder, sdc_executor):
    """
    Test the Field Mapper processor, by value.  Rounds double fields up to the nearest integer (ceiling).

    The pipeline that will be constructed is:

        dev_raw_data_source (JSON data) >> field_mapper (ceiling) >> wiretap
    """
    raw_data = """{
      "someData": {
        "value1": 19.2,
        "value2": -16.5,
        "value3": 1987.44,
        "subData": {
          "value4": 0.45
        }
      },
      "moreData": {
        "value5": 19884.5,
        "value6": -0.25
      }
    }"""

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)
    field_mapper = pipeline_builder.add_stage('Field Mapper', type='processor')
    field_mapper.set_attributes(
        operate_on='FIELD_VALUES',
        conditional_expression='${f:type() == \'DOUBLE\'}',
        mapping_expression='${math:ceil(f:value())}',
        maintain_original_paths=False
    )
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_mapper >> wiretap.destination

    pipeline = pipeline_builder.build('Field mapper - The Daves I Know')
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    sdc_executor.remove_pipeline(pipeline)

    output_record = wiretap.output_records[0]
    assert output_record.get_field_data('/someData/value1') == 20
    assert output_record.get_field_data('/someData/value2') == -16
    assert output_record.get_field_data('/someData/value3') == 1988
    assert output_record.get_field_data('/someData/subData/value4') == 1
    assert output_record.get_field_data('/moreData/value5') == 19885
    assert output_record.get_field_data('/moreData/value6') == 0


@sdc_min_version('5.8.0')
@pytest.mark.parametrize('function, aggregation_expression', [
    ('previousPath', '${asFields(map(fields, previousPath()))}'),
    ('fieldByPreviousPath', '${map(fields, fieldByPreviousPath())}')
])
def test_field_mapper_aggregate_expressions_field_mapping(
        sdc_builder,
        sdc_executor,
        function,
        aggregation_expression
):
    """
    Tests the Field Mapper processor aggregate expressions for field mapping when having fields with the same values and
    when dealing with complex structures such as lists and maps.

    The pipeline used by the test is:
        Dev Raw Data Source >> Field Mapper >> Wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    basic_data_1 = 'Hello World! :D'
    basic_data_2 = 1
    array_1 = [1, 2]
    array_2 = [5]
    map_1 = {"a": "1", "b": "2"}
    raw_map_1 = '{"a": "1", "b": "2"}'
    map_2 = {}

    string_field = 'basic_data'
    array_field = 'data_in_a_list'
    map_field = 'data_in_a_map'
    mapping_field = 'Data'

    raw_data = f"""{{
      "{string_field}": {{
        "a": "{basic_data_1}",
        "b": "{basic_data_1}",
        "c": {basic_data_2},
        "d": "{basic_data_1}"
      }},
      "{array_field}": {{
        "a": {array_1},
        "b": {array_1},
        "c": {array_2}
      }},
      "{map_field}": {{
        "a": {raw_map_1},
        "b": {raw_map_1},
        "c": {map_2}
      }}
    }}"""

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='JSON',
        raw_data=raw_data,
        stop_after_first_batch=True
    )

    field_mapper = pipeline_builder.add_stage('Field Mapper', type='processor')
    field_mapper.set_attributes(
        operate_on='FIELD_PATHS',
        conditional_expression='',
        mapping_expression=f'/{mapping_field}',
        aggregation_expression=aggregation_expression,
        maintain_original_paths=False
    )

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_mapper >> wiretap.destination

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        output_records = wiretap.output_records
        assert len(output_records) == 1
        assert len(wiretap.error_records) == 0

        record = output_records[0]

        assert mapping_field in record.field
        fields = record.field[mapping_field]

        if function == 'fieldByPreviousPath':
            # fields is a list of maps where each map contains the field path and field value, so we create a map
            # containing all the values from the maps to check the results with more ease
            fields_map = dict(ChainMap(*fields))

            assert fields_map[f'/{string_field}/a'] == basic_data_1
            assert fields_map[f'/{string_field}/b'] == basic_data_1
            assert fields_map[f'/{string_field}/c'] == basic_data_2
            assert fields_map[f'/{string_field}/d'] == basic_data_1

            assert fields_map[f'/{array_field}/a[0]'] == array_1[0]
            assert fields_map[f'/{array_field}/a[1]'] == array_1[1]
            assert fields_map[f'/{array_field}/b[0]'] == array_1[0]
            assert fields_map[f'/{array_field}/b[1]'] == array_1[1]
            assert fields_map[f'/{array_field}/c[0]'] == array_2[0]

            assert fields_map[f'/{map_field}/a/a'] == map_1["a"]
            assert fields_map[f'/{map_field}/a/b'] == map_1["b"]
            assert fields_map[f'/{map_field}/b/a'] == map_1["a"]
            assert fields_map[f'/{map_field}/b/b'] == map_1["b"]
            assert f'/{map_field}/c' not in fields_map, \
                f"The field {mapping_field}/{map_field}/c is an empty map, so no entries should have appeared"
        else:
            assert fields == [
                f'/{string_field}/a',
                f'/{string_field}/b',
                f'/{string_field}/c',
                f'/{string_field}/d',
                f'/{array_field}/a[0]',
                f'/{array_field}/a[1]',
                f'/{array_field}/b[0]',
                f'/{array_field}/b[1]',
                f'/{array_field}/c[0]',
                f'/{map_field}/a/a',
                f'/{map_field}/a/b',
                f'/{map_field}/b/a',
                f'/{map_field}/b/b'
            ]

    finally:
        if pipeline and (sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING'):
            sdc_executor.stop_pipeline(pipeline)
