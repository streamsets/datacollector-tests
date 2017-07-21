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

import hashlib
import json
import logging
import re
from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# pylint: disable=pointless-statement, too-many-locals


def test_field_flattener(sdc_builder, sdc_executor):
    """Test field flattener processor. The pipeline would look like:

        dev_raw_data_source >> field_flattener >> trash

    With given raw_data below, /contact/address will move to newcontact/address and its elements will be flatten as
    home.state and home.zipcode
    """
    name_seperator = '.'
    raw_data = """
        {
          "contact": {
             "name": "Jane Smith",
             "id": "557",
             "address": {
               "home": {
                 "state": "NC",
                 "zipcode": "27023"
                  }
              }
          },
           "newcontact": {
             "address": {}
          }
        }
    """

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data)
    field_flattener = pipeline_builder.add_stage('Field Flattener')
    field_flattener.set_attributes(fields=['/contact/address'], flatten_in_place=False,
                                   flatten_target_field='/newcontact/address', flatten_type='SPECIFIC_FIELDS',
                                   name_seperator=name_seperator, remove_flatten_field=True)
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> field_flattener >> trash
    pipeline = pipeline_builder.build('Field Flattener pipeline')
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
    sdc_executor.stop_pipeline(pipeline)

    new_value = snapshot[field_flattener.instance_name].output[0].value['value']
    # assert remove_flatten_field
    assert 'address' not in new_value['contact']['value']
    # assert flatten_target_field with name_seperator
    assert f'home{name_seperator}state' in new_value['newcontact']['value']['address']['value']
    assert f'home{name_seperator}zipcode' in new_value['newcontact']['value']['address']['value']


def test_field_hasher(sdc_builder, sdc_executor):
    """Test field hasher. The pipeline would look like:

        dev_raw_data_source >> field_hasher >> trash

    With the given config below, we will have md5passcode, sha1passcode, sha2passcode and myrecord md5 holding for
    entire record.
    """
    raw_id = '557'
    raw_passcode = 'mysecretcode'
    raw_dict = dict(contact=dict(name='Jane Smith', id=raw_id, passcode=raw_passcode))
    raw_data = json.dumps(raw_dict)
    field_hasher_in_place_configs = [
        {
            'sourceFieldsToHash': ['/contact/id'],
            'hashType': 'MD5'
        }
    ]
    field_hasher_target_configs = [
        {
            'sourceFieldsToHash': ['/contact/passcode'],
            'hashType': 'MD5',
            'targetField': '/md5passcode',
            'headerAttribute': 'md5passcode'
        }, {
            'sourceFieldsToHash': ['/contact/passcode'],
            'hashType': 'SHA1',
            'targetField': '/sha1passcode',
            'headerAttribute': 'sha1passcode'
        }, {
            'sourceFieldsToHash': ['/contact/passcode'],
            'hashType': 'SHA2',
            'targetField': '/sha2passcode',
            'headerAttribute': 'sha2passcode'
        }
    ]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data)
    field_hasher = pipeline_builder.add_stage('Field Hasher')
    field_hasher.set_attributes(field_hasher_in_place_configs=field_hasher_in_place_configs,
                                field_hasher_target_configs=field_hasher_target_configs,
                                record_hasher_hash_entire_record=True, record_hasher_hash_type='MD5',
                                record_hasher_header_attribute='myrecord', record_hasher_include_record_header=False,
                                record_hasher_target_field='/myrecord')
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> field_hasher >> trash
    pipeline = pipeline_builder.build('Field Hasher pipeline')
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
    sdc_executor.stop_pipeline(pipeline)

    new_header = snapshot[field_hasher.instance_name].output[0].header
    new_value = snapshot[field_hasher.instance_name].output[0].value['value']
    # assert new header fields are created same as generated value fields
    assert new_header['sha1passcode'] == new_value['sha1passcode']['value']
    assert new_header['myrecord'] == new_value['myrecord']['value']
    assert new_header['md5passcode'] == new_value['md5passcode']['value']
    assert new_header['sha2passcode'] == new_value['sha2passcode']['value']
    # assert in place record field being hashed as expected
    id_hash = hashlib.md5()
    id_hash.update(raw_id.encode())
    assert new_value['contact']['value']['id']['value'] == id_hash.hexdigest()
    # assert new record field has an expected hash of MD5
    passcode_md5hash = hashlib.md5()
    passcode_md5hash.update(raw_passcode.encode())
    assert new_value['md5passcode']['value'] == passcode_md5hash.hexdigest()
    # assert new record field has an expected hash of SHA1
    passcode_sha1hash = hashlib.sha1()
    passcode_sha1hash.update(raw_passcode.encode())
    assert new_value['sha1passcode']['value'] == passcode_sha1hash.hexdigest()
    # assert new record field has an expected hash of SHA2
    passcode_sha2hash = hashlib.sha256()
    passcode_sha2hash.update(raw_passcode.encode())
    assert new_value['sha2passcode']['value'] == passcode_sha2hash.hexdigest()


def test_field_masker(sdc_builder, sdc_executor):
    """Test field masker processor. The pipeline would look like:

        dev_raw_data_source >> field_masker >> trash

    With the given config below, `donKey` will be masked as `xxxxxxxxxx` (for fixed) and `xxxxxx` (for variable),
    `617-567-8888` will be masked as `617-xxx-xxxx`, `94086-6161` will be masked as `940xx`, `30529 - 123-45-6789`
    will be masked as `30529xxx123xxxxxxxx` (regex to mask all except groups 1 and 2).
    """
    raw_dict = dict(fixed_passwd='donKey', variable_passwd='donKey', custom_ph='617-567-8888',
                    custom_zip='94086-6161', social='30529 - 123-45-6789')
    raw_data = json.dumps(raw_dict)
    mask_configs = [
        {
            'fields': ['/fixed_passwd'],
            'maskType': 'FIXED_LENGTH',
            'regex': '(.*)',
            'groupsToShow': '1'
        }, {
            'fields': ['/variable_passwd'],
            'maskType': 'VARIABLE_LENGTH',
            'regex': '(.*)',
            'groupsToShow': '1'
        }, {
            'fields': ['/custom_ph'],
            'maskType': 'CUSTOM',
            'regex': '(.*)',
            'groupsToShow': '1',
            'mask': '###-xxx-xxxx'
        }, {
            'fields': ['/custom_zip'],
            'maskType': 'CUSTOM',
            'regex': '(.*)',
            'groupsToShow': '1',
            'mask': '###xx'
        }, {
            'fields': ['/social'],
            'maskType': 'REGEX',
            'regex': '([0-9]{5}) - ([0-9]{3})-([0-9]{2})-([0-9]{4})',
            'groupsToShow': '1,2'
        }
    ]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data)
    field_masker = pipeline_builder.add_stage('Field Masker')
    field_masker.set_attributes(mask_configs=mask_configs)
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> field_masker >> trash
    pipeline = pipeline_builder.build('Field Masker pipeline')
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
    sdc_executor.stop_pipeline(pipeline)

    new_value = snapshot[field_masker.instance_name].output[0].value['value']
    # assert fixed length mask always have the same masked characters
    fixed_value = new_value['fixed_passwd']['value']
    assert fixed_value == len(fixed_value) * fixed_value[0]
    # assert variable length mask always have the same masked characters with original length of the value
    variable_value = new_value['variable_passwd']['value']
    assert variable_value == len(raw_dict['variable_passwd']) * variable_value[0]
    # assert custom mask works
    assert new_value['custom_ph']['value'] == '{}-xxx-xxxx'.format(raw_dict['custom_ph'][0:3])
    # assert length of custom mask is the mask pattern length. Mask here is '###xx' and hence length of 5
    custom_zip = new_value['custom_zip']['value']
    assert len(custom_zip) == 5 and custom_zip == '{}xx'.format(raw_dict['custom_zip'][0:3])
    # assert regular expression mask
    match = re.search('([0-9]{5}) - ([0-9]{3})-([0-9]{2})-([0-9]{4})', raw_dict['social'])
    assert new_value['social']['value'] == '{}xxx{}xxxxxxxx'.format(match.group(1), match.group(2))


def test_field_merger(sdc_builder, sdc_executor):
    """Test field merger processor. The pipeline would look like:

        dev_raw_data_source >> field_merger >> trash

    With the given config below, /contact will move to existing path /dupecontact and /identity will move to new path
    /uniqueid.
    """
    raw_data = """
        {
          "contact": {
            "city": "San Juan"
          },
          "dupecontact": {},
          "identity": {
            "name": "Jane Smith",
            "id": "557"
          }
        }
    """
    raw_dict = json.loads(raw_data)
    merge_mapping_configs = [
        {
            'fromField': '/identity',
            'toField': '/uniqueid'
        },
        {
            'fromField': '/contact',
            'toField': '/dupecontact'
        }
    ]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data)
    field_merger = pipeline_builder.add_stage('Field Merger')
    field_merger.set_attributes(merge_mapping=merge_mapping_configs, overwrite_existing=True)
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> field_merger >> trash
    pipeline = pipeline_builder.build('Field Merger pipeline')
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
    sdc_executor.stop_pipeline(pipeline)

    new_value = snapshot[field_merger.instance_name].output[0].value['value']
    # assert overwrite existing works
    assert len(new_value['dupecontact']['value']) > 0
    # assert merge works by comparing `identity` to new `uniqueid` dict
    dict1 = raw_dict['identity']
    dict2 = new_value['uniqueid']['value']
    assert len(dict1) == len(dict2)
    assert all(source_value == dict2[source_key]['value'] for source_key, source_value in dict1.items())


def test_field_order(sdc_builder, sdc_executor):
    """Test field order processor. The pipeline would look like:

        dev_raw_data_source >> field_order >> trash

    With given config, 3 ordered_fields will be created by dropping all the rest. /address/home/country is a new
    path being created with extra_field_value (eg 'USA').
    """
    raw_data = """
        {
          "name": "Jane Smith",
          "id": "557",
          "address": {
            "home": {
              "state": "NC",
              "zipcode": "27023"
                }
            }
        }
    """
    extra_field = '/address/home/country'
    extra_field_value = 'USA'
    ordered_fields = ['/address/home/zipcode', '/address/home/state', extra_field]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data)
    field_order = pipeline_builder.add_stage('Field Order')
    field_order.set_attributes(extra_field_action='DISCARD', fields_to_order=ordered_fields,
                               missing_field_action='USE_DEFAULT', missing_field_data_type='STRING',
                               missing_field_default_value=extra_field_value, output_type='LIST_MAP')
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> field_order >> trash
    pipeline = pipeline_builder.build('Field Order pipeline')
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
    sdc_executor.stop_pipeline(pipeline)

    new_value = snapshot[field_order.instance_name].output[0].value['value']
    # assert we got ordered fields and we don't have discarded fields
    assert ordered_fields == [i['sqpath'].replace("'", '').replace('.', '/') for i in new_value]
    # assert we got extra field value as expected
    extra_fields = [i['value'] for i in new_value if i['sqpath'].replace("'", '').replace('.', '/') == extra_field]
    assert extra_fields[0] == extra_field_value


def test_field_pivoter(sdc_builder, sdc_executor):
    """Test field pivoter processor. The pipeline would look like:

        dev_raw_data_source >> field_pivoter >> trash

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
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data)
    field_pivoter = pipeline_builder.add_stage('Field Pivoter')
    field_pivoter.set_attributes(copy_all_fields=True, field_to_pivot=field_to_pivot,
                                 original_field_name_path=field_name_path, pivoted_items_path=pivoted_item_path,
                                 save_original_field_name=True)
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> field_pivoter >> trash
    pipeline = pipeline_builder.build('Field Pivoter pipeline')
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
    sdc_executor.stop_pipeline(pipeline)

    new_value = snapshot[field_pivoter.instance_name].output[0].value['value']
    # assert our record got pivoted into expected length
    assert len(raw_dict['ballpoint']['color_list']) == len(snapshot[field_pivoter.instance_name].output)
    # assert pivoted field name is stored in the expected path
    assert new_value['ballpoint']['value']['color_list_path']['value'] == field_to_pivot
    # assert pivoted item path
    assert new_value['ballpoint']['value']['color']['sqpath'] == pivoted_item_path


def test_field_remover(sdc_builder, sdc_executor):
    """Test field remover processor for three different actions. The pipeline would look like:

        dev_raw_data_source >> field_remover1 >> field_remover2 >> field_remover3 >> trash

    With given 3 different field remover configs, the field_remover1 will remove /name and /id paths. field_remover2
    will remove any input fields which are null - in this case /checknull1 and /checknull2. field_remover3 will remove
    all fields except the mention ones - in this case except /address/home/zipcode and /address/home/country
    """
    raw_data = """
        {
            "name": "Jane Smith",
            "id": "557",
            "checknull1": null,
            "checknull2": null,
            "address": {
                "home": {
                    "state": "NC",
                    "zipcode": "27023"
                }
            }
        }
    """

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data)
    field_remover1 = pipeline_builder.add_stage('Field Remover')
    field_remover1.set_attributes(fields=['/id', '/name'], field_operation='REMOVE')
    field_remover2 = pipeline_builder.add_stage('Field Remover')
    field_remover2.set_attributes(fields=['/checknull1', '/checknull2'], field_operation='REMOVE_NULL')
    field_remover3 = pipeline_builder.add_stage('Field Remover')
    field_remover3.set_attributes(fields=['/address/home/zipcode', '/address/home/country'], field_operation='KEEP')
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> field_remover1 >> field_remover2 >> field_remover3 >> trash
    pipeline = pipeline_builder.build('Field Remover pipeline')
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
    sdc_executor.stop_pipeline(pipeline)

    remover1_value = snapshot[field_remover1.instance_name].output[0].value['value']
    remover2_value = snapshot[field_remover2.instance_name].output[0].value['value']
    remover3_value = snapshot[field_remover3.instance_name].output[0].value['value']

    # assert remove listed fields action
    assert 'name' not in remover1_value and 'id' not in remover1_value
    # assert remove listed fields if their values are null action
    assert 'checknull1' not in remover2_value and 'checknull2' not in remover2_value
    # assert keep only the listed fields action
    assert len(remover3_value) == 1 and 'state' not in remover3_value['address']['value']['home']['value']


def test_field_renamer(sdc_builder, sdc_executor):
    """Test field renamer processor. The pipeline would look like:

        dev_raw_data_source >> field_renamer >> trash

    With the given config below, based on regex, the incoming fields OPS_name1 and OPS_name2 will be renamed to
    name1 and name2 respectively.
    """
    strip_word = 'OPS_'
    raw_dict = dict(OPS_name1='abc1', OPS_name2='abc2')
    raw_data = json.dumps(raw_dict)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data)
    field_renamer = pipeline_builder.add_stage('Field Renamer')
    field_renamer.set_attributes(fields_to_rename=
                                 [{'fromFieldExpression': f'(.*){strip_word}(.*)', 'toFieldExpression': '$1$2'}])
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> field_renamer >> trash
    pipeline = pipeline_builder.build('Field Renamer pipeline')
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
    sdc_executor.stop_pipeline(pipeline)

    new_value = snapshot[field_renamer.instance_name].output[0].value['value']
    for key in raw_dict:
        assert key not in new_value and key.strip(strip_word) in new_value


def test_field_splitter(sdc_builder, sdc_executor):
    """Test field splitter processor. The pipeline would look like:

        dev_raw_data_source >> field_splitter >> trash

    With given config to process 2 records, the first record's /error/text value will split into /error/code and
    /error/message based on separator (,). The second record's /error/text value will split similarly but since it has
    too many split, the extra splits will go into new field called /error/etcMessages.
    """
    raw_data = """
        [ { "error": {
                "text": "GM-302,information that you might need"
            }
          },
          { "error": {
                "text": "ME-3042,message about error,additional information from server,network error,driver error"
            }
          }
        ]
    """
    raw_list = json.loads(raw_data)
    separator = ','
    split_fields = ['/error/code', '/error/message']
    source_sub_field = 'text'
    etc_sub_field = 'etcMessages'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', data_format_config='ARRAY_OBJECTS', raw_data=raw_data)
    field_splitter = pipeline_builder.add_stage('Field Splitter')
    field_splitter.set_attributes(field_for_remaining_splits=f'/error/{etc_sub_field}',
                                  field_to_split=f'/error/{source_sub_field}', new_split_fields=split_fields,
                                  not_enough_splits='CONTINUE', original_field='REMOVE',
                                  separator=separator, too_many_splits='TO_LIST')
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> field_splitter >> trash
    pipeline = pipeline_builder.build('Field Splitter pipeline')
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
    sdc_executor.stop_pipeline(pipeline)

    record_1 = snapshot[field_splitter.instance_name].output[0].value['value']['error']['value']
    record_2 = snapshot[field_splitter.instance_name].output[1].value['value']['error']['value']
    # assert we got expected number of splits
    assert len(raw_list[0]['error']['text'].split(separator)) == len(record_1)
    # assert record data
    raw_record_data = raw_list[0]['error']['text'].split(separator)
    for value in record_1.values():
        assert value['value'] in raw_record_data
    # assert field_for_remaining_splits
    raw_record_data = raw_list[1]['error']['text'].split(separator)
    # etc_sub_field will only have a subset of splits and hence need to take out (subtract) the remaining
    assert len(record_2[etc_sub_field]['value']) == len(raw_record_data) - len(split_fields)
    for data in record_2[etc_sub_field]['value']:
        assert data['value'] in raw_record_data
    # assert original_field being removed
    assert source_sub_field not in record_1 and source_sub_field not in record_2


def test_field_type_converter(sdc_builder, sdc_executor):
    """Test field type converter processor. We will use two stages to test field by field type conversion and
    data type conversion. The pipeline would look like:

        dev_raw_data_source >> field_type_converter_fields >> field_type_converter_types >> trash

    With given 2 stages for converter, field_type_converter_fields will convert field for field conversion. For e.g.,
    record1's /amInteger will convert from INTEGER to BYTE. field_type_converter_types will convert any field type to
    any field type if it matches the type criteria. For e.g., /amDateTime field type DATETIME will convert to LONG.
    """
    utc_datetime_str = '1978-01-05 19:38:01'
    utc_datetime = datetime.strptime(utc_datetime_str, '%Y-%m-%d %H:%M:%S')
    utc_datetime_in_int = int(utc_datetime.strftime('%s')) * 1000 # multiply by 1000 to account for milliseconds
    raw_str_value = 'hello again!'
    # note, date time here is in UTC. Each map is an SDC record to process.
    raw_col = [{'amInteger': 123}, {'amDouble': 12345.6789115}, {'amString': 'hello'}, {'amBool': True},
               {'amDateTime': utc_datetime_str}, {'amString2': raw_str_value}]
    raw_data = json.dumps(raw_col)
    field_type_converter_configs = [
        {
            'fields': ['/amInteger'],
            'targetType': 'BYTE',
            'dataLocale': 'en,US'
        }, {
            'fields': ['/amDouble'],
            'targetType': 'INTEGER',
            'dataLocale': 'en,US'
        }, {
            'fields': ['/amString'],
            'targetType': 'CHAR'
        }, {
            'fields': ['/amBool'],
            'targetType': 'BOOLEAN'
        }, {
            'fields': ['/amDateTime'],
            'targetType': 'DATETIME',
            'dateFormat': 'YYYY_MM_DD_HH_MM_SS'
        }, {
            'fields': ['/amString2'],
            'targetType': 'BYTE_ARRAY'
        }
    ]
    whole_type_converter_configs = [
        {
            'sourceType': 'BYTE',
            'targetType': 'DECIMAL',
            'dataLocale': 'en,US',
            'scale': -1,
            'decimalScaleRoundingStrategy': 'ROUND_UNNECESSARY'
        }, {
            'sourceType': 'INTEGER',
            'targetType': 'SHORT',
            'dataLocale': 'en,US'
        }, {
            'sourceType': 'CHAR',
            'targetType': 'STRING',
            'treatInputFieldAsDate': False
        }, {
            'sourceType': 'BOOLEAN',
            'targetType': 'STRING',
            'treatInputFieldAsDate': False,
            'encoding': 'UTF-8'
        }, {
            'sourceType': 'DATETIME',
            'targetType': 'LONG',
            'treatInputFieldAsDate': True,
            'dateFormat': 'YYYY_MM_DD_HH_MM_SS',
            'encoding': 'UTF-8',
            'dataLocale' : 'en,US'
        }, {
            'sourceType': 'BYTE_ARRAY',
            'targetType': 'STRING',
            'treatInputFieldAsDate': False,
            'encoding': 'UTF-8'
        }
    ]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', data_format_config='ARRAY_OBJECTS', raw_data=raw_data)
    field_type_converter_fields = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter_fields.set_attributes(conversion_method='BY_FIELD',
                                               field_type_converter_configs=field_type_converter_configs)
    field_type_converter_types = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter_types.set_attributes(conversion_method='BY_TYPE',
                                              whole_type_converter_configs=whole_type_converter_configs)
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> field_type_converter_fields >> field_type_converter_types >> trash
    pipeline = pipeline_builder.build('Field Type Converter pipeline')
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
    sdc_executor.stop_pipeline(pipeline)

    # assert field by field type conversion
    field_output = snapshot[field_type_converter_fields.instance_name].output
    assert field_output[0].value['value']['amInteger']['type'] == 'BYTE'
    assert field_output[1].value['value']['amDouble']['type'] == 'INTEGER'
    assert field_output[2].value['value']['amString']['type'] == 'CHAR'
    assert field_output[3].value['value']['amBool']['type'] == 'BOOLEAN'
    assert field_output[4].value['value']['amDateTime']['type'] == 'DATETIME'
    assert field_output[5].value['value']['amString2']['type'] == 'BYTE_ARRAY'
    # assert data type conversion
    type_output = snapshot[field_type_converter_types.instance_name].output
    assert type_output[0].value['value']['amInteger']['type'] == 'DECIMAL'
    assert type_output[1].value['value']['amDouble']['type'] == 'SHORT'
    assert type_output[2].value['value']['amString']['type'] == 'STRING'
    assert type_output[3].value['value']['amBool']['type'] == 'STRING'
    assert type_output[4].value['value']['amDateTime']['type'] == 'LONG'
    assert type_output[5].value['value']['amString2']['type'] == 'STRING'
    # assert values which can be compared
    assert utc_datetime_in_int == int(type_output[4].value['value']['amDateTime']['value'])
    assert raw_str_value == type_output[5].value['value']['amString2']['value']


def test_field_zip(sdc_builder, sdc_executor):
    """Test field zip processor. The pipeline would look like:

        dev_raw_data_source >> field_zip >> trash

    With given config, /basics and /additional will zip to /all path. For e.g., /all will have be a list of
    2 zipped maps of {id: 23, inventory:80}, {color: 20005, cost:5} similarly for /itemID and /cost.
    """
    # Note: This test will fail till SDC-6657 is fixed.

    raw_data = """
        [
          {
            "order": 23523482,
            "itemID": [2, 113, 954, 6502],
            "cost": [89.95, 8.95, 6.95]
          },
          {
            "basics": {
                "id": 23, "color": 20005, "info": null
            },
            "additional": {
                "inventory": 80, "cost": 5
            }
          }
        ]
    """
    raw_list = json.loads(raw_data)
    dest_var = 'all'
    fields_to_zip_configs = [
        {
            'zippedFieldPath': '/purchase',
            'firstField': '/itemID',
            'secondField': '/cost'
        }, {
            'zippedFieldPath': f'/{dest_var}',
            'firstField': '/basics',
            'secondField': '/additional'
        }
    ]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', data_format_config='ARRAY_OBJECTS', raw_data=raw_data)
    field_zip = pipeline_builder.add_stage('Field Zip')
    field_zip.set_attributes(field_does_not_exist='CONTINUE', fields_to_zip=fields_to_zip_configs,
                             zip_values_only=False)
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> field_zip >> trash
    pipeline = pipeline_builder.build('Field Zip pipeline')
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
    sdc_executor.stop_pipeline(pipeline)

    record_output = snapshot[field_zip.instance_name].output[0].value['value']
    raw_items = raw_list[1]
    raw_item_1 = raw_items['basics']
    raw_item_2 = raw_items['additional']
    output_item_1 = record_output[dest_var]['value'][0]['value']
    output_item_2 = record_output[dest_var]['value'][1]['value']

    # assert zip merge size
    assert len(list(zip(raw_item_1, raw_item_2))) == len(record_output[dest_var]['value'])
    # assert zip merge data
    raw_merge = list(zip(raw_item_1.values(), raw_item_2.values()))
    output_merge = [tuple((int(a['value']) for a in output_item_1.values())),
                    tuple((int(a['value']) for a in output_item_2.values()))]
    assert raw_merge == output_merge


def test_value_replacer(sdc_builder, sdc_executor):
    """Test Value Replacer processor replacing values in fields. The pipeline would look like:

        dev_raw_data_source >> value_replacer >> trash
    """
    expected_password_value = 'mysecretcode'
    expected_state_value = 'NC'
    raw_data = """
        {
          "contact": {
             "fname": "Jane",
             "lname": "Smith",
             "id": 557,
             "address": {
               "home": {
                 "state": "North Carolina",
                 "zipcode": "27023"
                }
              },
              "password": null,
              "state": null
          }
        }
    """

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data)
    value_replacer = pipeline_builder.add_stage('Value Replacer', type='processor')
    value_replacer.set_attributes(conditional_replace_values=[{
        'fieldNames': ['/contact/address/home/state', '/contact/state'],
        'operator': 'ALL',
        'comparisonValue': 'North Carolina',
        'replacementValue': expected_state_value
    }], fields_to_null=[{
        'fields': ['/contact/password'],
        'newValue': expected_password_value
    }], replace_null_values=[{
        'fieldsToNull': ['/contact/*name'],
        'condition': "${record:value('/contact/id') > 0}"
    }])
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> value_replacer >> trash
    pipeline = pipeline_builder.build('Value Replacer pipeline')
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
    sdc_executor.stop_pipeline(pipeline)

    new_value = snapshot[value_replacer.instance_name].output[0].value['value']['contact']['value']
    # assert fields to null
    assert None == new_value['fname']['value'] == new_value['lname']['value']
    # assert replace null values
    assert expected_password_value == new_value['password']['value']
    # assert conditionally replace values
    assert expected_state_value == new_value['state']['value']
    assert expected_state_value == new_value['address']['value']['home']['value']['state']['value']
