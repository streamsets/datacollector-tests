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

import json
import logging
import pytest

from streamsets.testframework.markers import sdc_min_version

logger = logging.getLogger(__name__)

FIELD_RENAMER_03 = 'FIELD_RENAMER_03'
FIELD_RENAMER_03_ERR_MSG = 'Same fields matched by multiple expressions'
FIELD_RENAMER_04 = 'FIELD_RENAMER_04'
FIELD_RENAMER_04_ERR_MSG = 'Cannot set value in field'

def test_field_renamer(sdc_builder, sdc_executor):
    """Test field renamer processor. The pipeline would look like:

        dev_raw_data_source >> field_renamer >> wiretap

    With the given config below, based on regex, the incoming fields OPS_name1 and OPS_name2 will be renamed to
    name1 and name2 respectively.
    """
    strip_word = 'OPS_'
    raw_dict = dict(OPS_name1='abc1', OPS_name2='abc2')
    raw_data = json.dumps(raw_dict)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)
    field_renamer = pipeline_builder.add_stage('Field Renamer')
    field_renamer.fields_to_rename = [{'fromFieldExpression': f'(.*){strip_word}(.*)',
                                       'toFieldExpression': '$1$2'}]
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_renamer >> wiretap.destination
    pipeline = pipeline_builder.build('test_field_renamer pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        new_value = wiretap.output_records[0].field
        for key in raw_dict:
            assert key not in new_value and key.strip(strip_word) in new_value
    finally:
        sdc_executor.remove_pipeline(pipeline)


@sdc_min_version('3.7.0')
def test_field_renamer_uppercasing(sdc_builder, sdc_executor):
    """Test uppercasing of all fields - a common action done with the renamer."""
    raw_dict = dict(first_key='IPO', second_key='StreamSets')
    raw_data = json.dumps(raw_dict)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)
    field_renamer = pipeline_builder.add_stage('Field Renamer')
    field_renamer.fields_to_rename = [{'fromFieldExpression':'/(.*)',
                                       'toFieldExpression': '/${str:toUpper("$1")}'}]
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_renamer >> wiretap.destination
    pipeline = pipeline_builder.build('test_field_renamer_uppercasing pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        record = wiretap.output_records[0]
        assert record.field['FIRST_KEY'] == "IPO"
        assert record.field['SECOND_KEY'] == "StreamSets"
    finally:
        sdc_executor.remove_pipeline(pipeline)


@sdc_min_version('5.7.0')
def test_field_renamer_lowercasing_nested_data(sdc_builder, sdc_executor):
    """Test lower casing of all fields in nested data."""

    raw_data = """{
                 "Name": "Leo",
                 "age": 20,
                 "ADDRESS":{"Home":"XYZ", "Office":"ABC"},
                 "Phone": ["11223344","222114455"],
                 "Hobbies":[{"Reading":{"Books":["B1","B2"]}, "Music":["Jazz","Pop"]},"Travelling"]
               }"""

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_renamer = pipeline_builder.add_stage('Field Renamer')
    field_renamer.set_attributes(fields_to_rename=[{'fromFieldExpression':'/(.*)',
                                                   'toFieldExpression': '/${str:toLower("$1")}'}],
                                 target_field_already_exists='REPLACE')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_renamer >> wiretap.destination
    pipeline = pipeline_builder.build('test_field_renamer_lowercasing_nested_data pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        output_record = wiretap.output_records[0].field

        #verify all the fields have been converted to lowercase
        assert "Name" not in output_record, "Field 'Name' should not exist"
        assert "ADDRESS" not in output_record, "Field 'ADDRESS' should not exist"
        assert "Phone" not in output_record, "Field 'Phone' should not exist"
        assert "Hobbies" not in output_record, "Field 'Hobbies' should not exist"
        assert output_record['name'] == "Leo"
        assert output_record['age'] == 20
        assert output_record['address'] == {'home':'XYZ', 'office':'ABC'}
        assert output_record['phone'] == ['11223344','222114455']
        assert output_record['hobbies'] == [{'reading':{'books':['B1','B2']}, 'music':['Jazz','Pop']},'Travelling']
    finally:
        sdc_executor.remove_pipeline(pipeline)


@pytest.mark.parametrize('toFieldExpression,source_field_does_not_exist,target_field_already_exists', [
    ('/alsoNonExisting','CONTINUE','TO_ERROR'),
    ('/existing','CONTINUE','TO_ERROR'),
    ('/existing','CONTINUE','REPLACE'),
    ('/existing','TO_ERROR','TO_ERROR'),
])
def test_field_renamer_non_existing_source_field(sdc_builder, sdc_executor, toFieldExpression,
                                                 source_field_does_not_exist, target_field_already_exists):
    """
      Test for non-existing source fields-
      1. If neither the source or target fields exist, then field renaming is a noop, and should succeed.
      2. If source field does not exist, and precondition is set to continue, this should succeed
      3. If precondition failure is set to error, a missing source field should cause an error
    """

    raw_data = """{
                 "name":"Leo"
               }"""

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_renamer = pipeline_builder.add_stage('Field Renamer')
    field_renamer.set_attributes(fields_to_rename=[{'fromFieldExpression':'/nonExisting',
                                                   'toFieldExpression':toFieldExpression}],
                                 source_field_does_not_exist=source_field_does_not_exist,
                                 target_field_already_exists=target_field_already_exists,
                                 multiple_source_field_matches='TO_ERROR')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_renamer >> wiretap.destination
    pipeline = pipeline_builder.build('test_field_renamer_non_existing_source_field pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        output_records = wiretap.output_records
        error_records = wiretap.error_records

        #assert the output
        if source_field_does_not_exist == 'TO_ERROR' and target_field_already_exists == 'TO_ERROR':
            assert len(output_records) == 0
            assert len(error_records) == 1
        else:
            assert len(error_records) == 0
            assert len(output_records) == 1
            assert output_records[0].field['name'] == "Leo"
    finally:
        sdc_executor.remove_pipeline(pipeline)


@pytest.mark.parametrize('fromFieldExpression,toFieldExpression,target_field_already_exists', [
    ('/existing','/overwrite','REPLACE'),
    ('/existing','/existing','REPLACE'),
    ('/existing','/overwrite','TO_ERROR'),
    ('/existing','/overwrite','APPEND_NUMBERS'),
])
def test_field_renamer_target_field_exist(sdc_builder, sdc_executor, fromFieldExpression, toFieldExpression,
                                           target_field_already_exists):
    """
      Test for target field exist-
      1. Standard overwrite condition. Source and target fields exist or are the same.
      2. If target_field_already_exists is set to TO_ERROR, overwriting should result in an error
    """

    raw_data = """{
                 "existing":"foo",
                 "overwrite":"bar"
               }"""

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_renamer = pipeline_builder.add_stage('Field Renamer')
    field_renamer.set_attributes(fields_to_rename=[{'fromFieldExpression':fromFieldExpression,
                                                   'toFieldExpression':toFieldExpression}],
                                 source_field_does_not_exist='CONTINUE',
                                 target_field_already_exists=target_field_already_exists,
                                 multiple_source_field_matches='TO_ERROR')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_renamer >> wiretap.destination
    pipeline = pipeline_builder.build('test_field_renamer_target_field_exist pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        output_records = wiretap.output_records
        error_records = wiretap.error_records

        #assert the output
        if target_field_already_exists == 'TO_ERROR':
            assert len(output_records) == 0
            assert len(error_records) == 1
        elif target_field_already_exists == 'REPLACE' and toFieldExpression == '/overwrite':
            assert len(output_records) == 1
            output_record = output_records[0].field
            assert output_record['overwrite'] == "foo"
            assert 'existing' not in output_record, "Field 'existing' should not exist"
        elif target_field_already_exists == 'REPLACE' and toFieldExpression == '/existing':
            assert len(output_records) == 1
            output_record = output_records[0].field
            assert output_record['existing'] == "foo"
            assert output_record['overwrite'] == "bar"
        elif target_field_already_exists == 'APPEND_NUMBERS':
            assert len(output_records) == 1
            output_record = output_records[0].field
            assert 'existing' not in output_record, "Field 'existing' should not exist"
            assert output_record['overwrite1'] == "foo"
            assert output_record['overwrite'] == "bar"
    finally:
        sdc_executor.remove_pipeline(pipeline)


@pytest.mark.parametrize('multiple_source_field_matches', ['TO_ERROR', 'CONTINUE'])
def test_field_renamer_multiple_regex_matching_same_field(sdc_builder, sdc_executor, multiple_source_field_matches):
    """
      It should throw error if multiple regex match the same field.
    """

    raw_data = """{
                 "sqlField":"foo"
               }"""

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_renamer = pipeline_builder.add_stage('Field Renamer')
    field_renamer.set_attributes(fields_to_rename=[{'fromFieldExpression':'/sql(.*)',
                                                   'toFieldExpression':'/sqlRename$1'},
                                                   {'fromFieldExpression':'/s(.*)',
                                                    'toFieldExpression':'/sRename$1'}],
                                 source_field_does_not_exist='CONTINUE',
                                 target_field_already_exists='TO_ERROR',
                                 multiple_source_field_matches=multiple_source_field_matches)

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_renamer >> wiretap.destination
    pipeline = pipeline_builder.build('test_field_renamer_multiple_regex_matching_same_field pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        output_records = wiretap.output_records
        error_records = wiretap.error_records

        #assert the output
        if multiple_source_field_matches == 'TO_ERROR':
            assert len(output_records) == 0
            assert len(error_records) == 1
            error_record = error_records[0]
            assert error_record.field['sqlField'] == 'foo'
            assert error_record.header['errorCode'] == FIELD_RENAMER_03
            assert FIELD_RENAMER_03_ERR_MSG in error_record.header['errorMessage']
        elif multiple_source_field_matches == 'CONTINUE':
            assert len(output_records) == 1
            output_record = output_records[0].field
            assert output_record['sqlField'] == 'foo'
    finally:
        sdc_executor.remove_pipeline(pipeline)


def test_field_renamer_map_field(sdc_builder, sdc_executor):
    """
      Test for map field
    """

    raw_data = """{
                 "first":{"value":"value"}
               }"""

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_renamer = pipeline_builder.add_stage('Field Renamer')
    field_renamer.set_attributes(fields_to_rename=[{'fromFieldExpression':'/first',
                                                   'toFieldExpression':'/second'}],
                                 source_field_does_not_exist='CONTINUE',
                                 target_field_already_exists='TO_ERROR',
                                 multiple_source_field_matches='TO_ERROR')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_renamer >> wiretap.destination
    pipeline = pipeline_builder.build('test_field_renamer_map_field pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        output_records = wiretap.output_records

        #assert the output
        assert len(output_records) == 1
        output_record = output_records[0].field
        assert output_record['second'] == {'value':'value'}
        assert 'first' not in output_record, "Field 'first' should not exist"
    finally:
        sdc_executor.remove_pipeline(pipeline)


def test_field_renamer_different_matches_regex(sdc_builder, sdc_executor):
    """
      Test for different matches of regular expressions
    """

    raw_data = """
                  {"#abcd":"hashabcd"}
                  {"ab#cd":"abhashcd"}
                  {"abcd#":"abcdhash"}
                """

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_renamer = pipeline_builder.add_stage('Field Renamer')
    field_renamer.set_attributes(fields_to_rename=[{'fromFieldExpression':"/'(.*)(#)(.*)'",
                                                   'toFieldExpression':"/$1hash$3"}],
                                 source_field_does_not_exist='CONTINUE',
                                 target_field_already_exists='TO_ERROR',
                                 multiple_source_field_matches='TO_ERROR')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_renamer >> wiretap.destination
    pipeline = pipeline_builder.build('test_field_renamer_different_matches_regex pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        output_records = wiretap.output_records

        #assert the output
        assert len(output_records) == 3
        assert output_records[0].field['hashabcd'] == 'hashabcd'
        assert output_records[1].field['abhashcd'] == 'abhashcd'
        assert output_records[2].field['abcdhash'] == 'abcdhash'
    finally:
        sdc_executor.remove_pipeline(pipeline)


def test_field_renamer_regex_in_non_complex_type(sdc_builder, sdc_executor):
    """
      Test for regular expression in non complex type
    """

    raw_data = """
                  {"sql#1":"foo"}
                """

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_renamer = pipeline_builder.add_stage('Field Renamer')
    field_renamer.set_attributes(fields_to_rename=[{'fromFieldExpression':"/'sql(#)(.*)'",
                                                   'toFieldExpression':"/sql$2"}],
                                 source_field_does_not_exist='CONTINUE',
                                 target_field_already_exists='TO_ERROR',
                                 multiple_source_field_matches='TO_ERROR')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_renamer >> wiretap.destination
    pipeline = pipeline_builder.build('test_field_renamer_regex_in_non_complex_type pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        output_records = wiretap.output_records

        #assert the output
        assert len(output_records) == 1
        output_record = output_records[0].field
        assert output_record['sql1'] == 'foo'
        assert 'sql#1' not in output_record, "Field 'sql#1' should not exist"
    finally:
        sdc_executor.remove_pipeline(pipeline)


def test_field_renamer_regex_in_complex_map_type(sdc_builder, sdc_executor):
    """
      Test for regular expression in complex map type
    """

    raw_data = """
                  {
                    "map1":{"SQL#1":"foo1"},
                    "map2":{"SQL#2":"foo2"}
                  }
                """

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_renamer = pipeline_builder.add_stage('Field Renamer')
    field_renamer.set_attributes(fields_to_rename=[{'fromFieldExpression':"/(*)/'SQL(#)(.*)'",
                                                   'toFieldExpression':"/$1/SQL$3"}],
                                 source_field_does_not_exist='CONTINUE',
                                 target_field_already_exists='TO_ERROR',
                                 multiple_source_field_matches='TO_ERROR')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_renamer >> wiretap.destination
    pipeline = pipeline_builder.build('test_field_renamer_regex_in_complex_map_type pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        output_records = wiretap.output_records

        #assert the output
        assert len(output_records) == 1
        output_record = output_records[0].field
        assert output_record['map1'] == {"SQL1":"foo1"}
        assert output_record['map2'] == {"SQL2":"foo2"}
        assert 'SQL#1' not in output_record, "Field 'SQL#1' should not exist"
        assert 'SQL#2' not in output_record, "Field 'SQL#2' should not exist"
    finally:
        sdc_executor.remove_pipeline(pipeline)


def test_field_renamer_regex_in_complex_list_type(sdc_builder, sdc_executor):
    """
      Test for regular expression in complex list type
    """

    raw_data = """
                  {
                    "list":[{"SQL#1":"foo1"},{"SQL#2":"foo2"}]
                  }
                """

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_renamer = pipeline_builder.add_stage('Field Renamer')
    field_renamer.set_attributes(fields_to_rename=[{'fromFieldExpression':"/(*)[(*)]/'SQL(#)(.*)'",
                                                   'toFieldExpression':"/$1[$2]/SQL$4"}],
                                 source_field_does_not_exist='CONTINUE',
                                 target_field_already_exists='TO_ERROR',
                                 multiple_source_field_matches='TO_ERROR')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_renamer >> wiretap.destination
    pipeline = pipeline_builder.build('test_field_renamer_regex_in_complex_list_type pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        output_records = wiretap.output_records

        #assert the output
        assert len(output_records) == 1
        output_record = output_records[0].field
        assert output_record['list'] == [{"SQL1":"foo1"},{"SQL2":"foo2"}]
    finally:
        sdc_executor.remove_pipeline(pipeline)


def test_field_renamer_category_regex_in_non_complex_type(sdc_builder, sdc_executor):
    """
      Test for category regular expression in non complex type
    """

    raw_data = """
                  {
                    "a#b":"foo1",
                    "a_b":"foo2",
                    "a&b":"foo3",
                    "a|b":"foo4",
                    "a@b":"foo5"
                  }
                """

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_renamer = pipeline_builder.add_stage('Field Renamer')
    field_renamer.set_attributes(fields_to_rename=[{'fromFieldExpression':"/'(.*)[#&@|](.*)'",
                                                   'toFieldExpression':"/$1_$2"}],
                                 source_field_does_not_exist='CONTINUE',
                                 target_field_already_exists='APPEND_NUMBERS',
                                 multiple_source_field_matches='TO_ERROR')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_renamer >> wiretap.destination
    pipeline = pipeline_builder.build('test_field_renamer_category_regex_in_non_complex_type pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        output_records = wiretap.output_records

        #assert the output
        assert len(output_records) == 1
        output_record = output_records[0].field
        assert output_record['a_b'] == "foo2"
        assert output_record['a_b1'] == "foo1"
        assert output_record['a_b2'] == "foo3"
        assert output_record['a_b3'] == "foo4"
        assert output_record['a_b4'] == "foo5"
    finally:
        sdc_executor.remove_pipeline(pipeline)


def test_field_renamer_simple_and_complex_type(sdc_builder, sdc_executor):
    """
      Test for field renaming in simple and complex type
    """

    raw_data = """
                  {
                    "existing":"foo",
                    "listOfMaps":[{"existing":"foo"}],
                    "mapOfMaps":{"innerMap":{"existing":"foo"}}
                  }
                """

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_renamer = pipeline_builder.add_stage('Field Renamer')
    field_renamer.set_attributes(fields_to_rename=[{'fromFieldExpression':"/existing",
                                                   'toFieldExpression':"/nonExisting"},
                                                   {'fromFieldExpression':"/listOfMaps[0]/existing",
                                                    'toFieldExpression':"/listOfMaps[0]/nonExisting"},
                                                   {'fromFieldExpression':"/mapOfMaps/(*)/existing",
                                                    'toFieldExpression':"/mapOfMaps/$1/nonExisting"}],
                                 source_field_does_not_exist='TO_ERROR',
                                 target_field_already_exists='REPLACE',
                                 multiple_source_field_matches='TO_ERROR')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_renamer >> wiretap.destination
    pipeline = pipeline_builder.build('test_field_renamer_simple_and_complex_type pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        output_records = wiretap.output_records

        #assert the output
        assert len(output_records) == 1
        output_record = output_records[0].field
        assert output_record['nonExisting'] == 'foo'
        assert output_record['listOfMaps'] == [{"nonExisting":"foo"}]
        assert output_record['mapOfMaps'] == {"innerMap":{"nonExisting":"foo"}}
        assert 'existing' not in output_record, "Field 'existing' should not exist"
    finally:
        sdc_executor.remove_pipeline(pipeline)


@sdc_min_version('5.7.0')
@pytest.mark.parametrize('fromFieldExpression,toFieldExpression', [
    (["/listOfInts[0]","/listOfInts[1]","/listOfInts[2]"],["/nonExisting0","/nonExisting1","/nonExisting2"]),
    (["/listOfInts[(0)]","/listOfInts[(1)]","/listOfInts[(2)]"],["/nonExisting$1","/nonExisting$1","/nonExisting$1"]),
])
def test_field_renamer_multiple_list_elements_with_const_idx_expr_or_grp(sdc_builder, sdc_executor,
                                                                          fromFieldExpression, toFieldExpression):
    """
      Test for field renaming in multiple list elements
    """

    raw_data = """
                  {
                    "listOfInts":[1, 2, 3]
                  }
                """

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_renamer = pipeline_builder.add_stage('Field Renamer')
    field_renamer.set_attributes(fields_to_rename=[{'fromFieldExpression':fromFieldExpression[0],
                                                   'toFieldExpression':toFieldExpression[0]},
                                                   {'fromFieldExpression':fromFieldExpression[1],
                                                    'toFieldExpression':toFieldExpression[1]},
                                                   {'fromFieldExpression':fromFieldExpression[2],
                                                    'toFieldExpression':toFieldExpression[2]}],
                                 source_field_does_not_exist='TO_ERROR',
                                 target_field_already_exists='REPLACE',
                                 multiple_source_field_matches='TO_ERROR')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_renamer >> wiretap.destination
    pipeline = pipeline_builder.build('test_field_renamer_multiple_list_elements_with_const_idx_expr_or_grp pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        output_records = wiretap.output_records

        #assert the output
        assert len(output_records) == 1
        output_record = output_records[0].field
        assert output_record['listOfInts'] == []
        assert output_record['nonExisting0'] == 1
        assert output_record['nonExisting1'] == 2
        assert output_record['nonExisting2'] == 3
    finally:
        sdc_executor.remove_pipeline(pipeline)


def test_field_renamer_multiple_list_elements_with_regex_grp_expr(sdc_builder, sdc_executor):
    """
      Test for multiple list elements with group regular expression
    """

    raw_data = """
                  {
                    "listOfInts":[1, 2, 3]
                  }
                """

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_renamer = pipeline_builder.add_stage('Field Renamer')
    field_renamer.set_attributes(fields_to_rename=[{'fromFieldExpression':"/listOfInts[(*)]",
                                                   'toFieldExpression':"/nonExisting$1"}],
                                 source_field_does_not_exist='TO_ERROR',
                                 target_field_already_exists='REPLACE',
                                 multiple_source_field_matches='TO_ERROR')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_renamer >> wiretap.destination
    pipeline = pipeline_builder.build('test_field_renamer_multiple_list_elements_with_regex_grp_expr pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        output_records = wiretap.output_records

        #assert the output
        assert len(output_records) == 1
        output_record = output_records[0].field
        assert output_record['listOfInts'] == []
        assert output_record['nonExisting0'] == 1
        assert output_record['nonExisting1'] == 2
        assert output_record['nonExisting2'] == 3
    finally:
        sdc_executor.remove_pipeline(pipeline)


def test_field_renamer_multiple_map_elements_expr(sdc_builder, sdc_executor):
    """
      Test for field renaming in multiple map elements using expression
    """

    raw_data = """
                  {
                    "oe1":{"ie1":"ie1","ie2":"ie2","ie3":"ie3"},
                    "oe2":{"ie1":"ie1","ie2":"ie2","ie3":"ie3"},
                    "oe3":{"ie1":"ie1","ie2":"ie2","ie3":"ie3"}
                  }
                """

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_renamer = pipeline_builder.add_stage('Field Renamer')
    field_renamer.set_attributes(fields_to_rename=[{'fromFieldExpression':"/oe1/(*)",
                                                   'toFieldExpression':"/oe1/irename_$1"},
                                                   {'fromFieldExpression':"/oe2/(*)",
                                                    'toFieldExpression':"/oe2/irename_$1"},
                                                   {'fromFieldExpression':"/oe3/(*)",
                                                    'toFieldExpression':"/oe3/irename_$1"}],
                                 source_field_does_not_exist='TO_ERROR',
                                 target_field_already_exists='REPLACE',
                                 multiple_source_field_matches='TO_ERROR')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_renamer >> wiretap.destination
    pipeline = pipeline_builder.build('test_field_renamer_multiple_map_elements_expr pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        output_records = wiretap.output_records

        #assert the output
        assert len(output_records) == 1
        output_record = output_records[0].field
        assert output_record['oe1'] == {"irename_ie1":"ie1","irename_ie2":"ie2","irename_ie3":"ie3"}
        assert output_record['oe2'] == {"irename_ie1":"ie1","irename_ie2":"ie2","irename_ie3":"ie3"}
        assert output_record['oe3'] == {"irename_ie1":"ie1","irename_ie2":"ie2","irename_ie3":"ie3"}
    finally:
        sdc_executor.remove_pipeline(pipeline)


def test_field_renamer_unreachable_fields(sdc_builder, sdc_executor):
    """
      Test for unreachable fields
    """

    raw_data = """
                  {
                    "a":123
                  }
                """

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_renamer = pipeline_builder.add_stage('Field Renamer')
    field_renamer.set_attributes(fields_to_rename=[{'fromFieldExpression':"/a",
                                                   'toFieldExpression':"/b/c/d"}],
                                 source_field_does_not_exist='TO_ERROR',
                                 target_field_already_exists='REPLACE',
                                 multiple_source_field_matches='TO_ERROR')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_renamer >> wiretap.destination
    pipeline = pipeline_builder.build('test_field_renamer_unreachable_fields pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        output_records = wiretap.output_records
        error_records = wiretap.error_records

        #assert the output
        assert len(output_records) == 0
        assert len(error_records) == 1
        error_record = error_records[0]
        assert error_record.field['a'] == 123
        assert error_record.header['errorCode'] == FIELD_RENAMER_04
        assert FIELD_RENAMER_04_ERR_MSG in error_record.header['errorMessage']
    finally:
        sdc_executor.remove_pipeline(pipeline)


def test_field_renamer_source_with_quoted_substring(sdc_builder, sdc_executor):
    """
      Test for source with quoted substring
    """

    raw_data = """
                  {
                    "attr|OrderNum":"foo"
                  }
                """

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_renamer = pipeline_builder.add_stage('Field Renamer')
    field_renamer.set_attributes(fields_to_rename=[{'fromFieldExpression':"/'attr|OrderNum'",
                                                   'toFieldExpression':"/theOrderNum"}],
                                 source_field_does_not_exist='CONTINUE',
                                 target_field_already_exists='REPLACE',
                                 multiple_source_field_matches='TO_ERROR')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_renamer >> wiretap.destination
    pipeline = pipeline_builder.build('test_field_renamer_source_with_quoted_substring pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        output_records = wiretap.output_records
        output_record = output_records[0].field

        #assert the output
        assert len(output_records) == 1
        assert output_record['theOrderNum'] == 'foo'
        assert "'attr|OrderNum'" not in output_record, "Field ''attr|OrderNum'' should not exist"
        assert "attr|OrderNum" not in output_record, "Field 'attr|OrderNum' should not exist"
    finally:
        sdc_executor.remove_pipeline(pipeline)


def test_field_renamer_name_with_slash(sdc_builder, sdc_executor):
    """
      Test for name with slash
    """

    raw_data = """
                  {
                    "a/b":"foo"
                  }
                """

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_renamer = pipeline_builder.add_stage('Field Renamer')
    field_renamer.set_attributes(fields_to_rename=[{'fromFieldExpression':"/(.*)",
                                                   'toFieldExpression':"/moved_$1"}],
                                 source_field_does_not_exist='CONTINUE',
                                 target_field_already_exists='REPLACE',
                                 multiple_source_field_matches='TO_ERROR')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_renamer >> wiretap.destination
    pipeline = pipeline_builder.build('test_field_renamer_name_with_slash pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        output_records = wiretap.output_records
        output_record = output_records[0].field

        #assert the output
        assert len(output_records) == 1
        assert output_record['moved_a/b'] == 'foo'
        assert "a/b" not in output_record, "Field 'a/b' should not exist"
    finally:
        sdc_executor.remove_pipeline(pipeline)