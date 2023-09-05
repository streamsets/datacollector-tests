# Copyright 2017 StreamSets Inc.
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

import hashlib
import json
import logging
import pytest
import time
from datetime import datetime
from decimal import Decimal
from collections import ChainMap, OrderedDict

from streamsets.sdk.utils import Version
from streamsets.testframework.markers import sdc_min_version

logger = logging.getLogger(__name__)

# pylint: disable=pointless-statement, too-many-locals


def test_field_flattener(sdc_builder, sdc_executor):
    """Test field flattener processor. The pipeline would look like:

        dev_raw_data_source >> field_flattener >> wiretap

    With given raw_data below, /contact/address will move to newcontact/address and its elements will be flatten as
    home.state and home.zipcode
    """
    name_separator = '.'
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
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)
    field_flattener = pipeline_builder.add_stage('Field Flattener')
    field_flattener.set_attributes(fields=['/contact/address'], flatten_in_place=False,
                                   target_field='/newcontact/address', flatten='SPECIFIC_FIELDS',
                                   name_separator=name_separator, remove_flattened_field=True)
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_flattener >> wiretap.destination
    pipeline = pipeline_builder.build('Field Flattener pipeline')
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    new_value = wiretap.output_records[0].field
    assert 'address' not in new_value['contact']
    assert f'home{name_separator}state' in new_value['newcontact']['address']
    assert f'home{name_separator}zipcode' in new_value['newcontact']['address']


@sdc_min_version('5.3.0')
def test_field_flattener_listmap(sdc_builder, sdc_executor):
    """Test field flattener processor. The pipeline would look like:

        dev_raw_data_source >> field_flattener >> wiretap
    """
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
          }
        }
    """

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)
    field_flattener = pipeline_builder.add_stage('Field Flattener')
    field_flattener.set_attributes(name_separator='.', remove_flattened_field=True, output_type='LIST_MAP')
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_flattener >> wiretap.destination
    pipeline = pipeline_builder.build('Field Flattener pipeline')
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    output_value = wiretap.output_records[0].field

    assert isinstance(output_value, OrderedDict)

    assert output_value['"contact.name"'] == 'Jane Smith'
    assert output_value['"contact.id"'] == '557'
    assert output_value['"contact.address.home.state"'] == 'NC'
    assert output_value['"contact.address.home.zipcode"'] == '27023'


def test_field_flattener_all(sdc_builder, sdc_executor):
    """Test field flattener processor. The pipeline would look like:

        dev_raw_data_source >> field_flattener >> wiretap

    With given raw_data below, all elements will be flattened and use '_._' as the separator
    """
    name_separator = '_._'
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
          }
        }
    """

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)
    field_flattener = pipeline_builder.add_stage('Field Flattener')
    field_flattener.set_attributes(flatten='ENTIRE_RECORD', name_separator=name_separator)

    if Version(sdc_builder.version) >= Version('5.3.0'):
        field_flattener.set_attributes(output_type='MAP')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_flattener >> wiretap.destination
    pipeline = pipeline_builder.build('Field Flattener (all) pipeline')
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    new_value = wiretap.output_records[0].field
    assert isinstance(new_value, dict)
    # assert everything has been flattened and there's no extra fields
    assert new_value[f'contact{name_separator}name'] == 'Jane Smith'
    assert new_value[f'contact{name_separator}id'] == '557'
    assert new_value[f'contact{name_separator}address{name_separator}home{name_separator}state'] == 'NC'
    assert new_value[f'contact{name_separator}address{name_separator}home{name_separator}zipcode'] == '27023'
    assert len(new_value) == 4


def test_field_hasher(sdc_builder, sdc_executor):
    """Test field hasher. The pipeline would look like:

        dev_raw_data_source >> field_hasher >> wiretap

    With the given config below, we will have md5passcode, sha1passcode, sha2passcode and myrecord md5 holding for
    entire record.  In versions >= 3.7.0, we'll have sha256passcode instead of sha2passcode, as well as sha512passcode.
    """
    raw_id = '557'
    raw_passcode = 'mysecretcode'
    raw_dict = dict(contact=dict(name='Jane Smith', id=raw_id, passcode=raw_passcode))
    raw_data = json.dumps(raw_dict)
    hash_in_place = [
        {
            'sourceFieldsToHash': ['/contact/id'],
            'hashType': 'MD5'
        }
    ]
    hash_to_target = [
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
        }
    ]
    # In 3.7.0, SHA2 was renamed to SHA256 (it will be automatically upgraded) and SHA512 was added
    if Version(sdc_builder.version) < Version('3.7.0'):
        hash_to_target.append(
            {
                'sourceFieldsToHash': ['/contact/passcode'],
                'hashType': 'SHA2',
                'targetField': '/sha2passcode',
                'headerAttribute': 'sha2passcode'
            }
        )
    if Version(sdc_executor.version) >= Version('3.7.0'):
        hash_to_target.append(
            {
                'sourceFieldsToHash': ['/contact/passcode'],
                'hashType': 'SHA256',
                'targetField': '/sha256passcode',
                'headerAttribute': 'sha256passcode'
            }
        )
        hash_to_target.append(
            {
                'sourceFieldsToHash': ['/contact/passcode'],
                'hashType': 'SHA512',
                'targetField': '/sha512passcode',
                'headerAttribute': 'sha512passcode'
            }
        )

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)
    field_hasher = pipeline_builder.add_stage('Field Hasher')
    field_hasher.set_attributes(hash_in_place=hash_in_place,
                                hash_to_target=hash_to_target,
                                hash_entire_record=True, hash_type='MD5',
                                header_attribute='myrecord', include_record_header=False,
                                target_field='/myrecord')
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_hasher >> wiretap.destination
    pipeline = pipeline_builder.build('Field Hasher pipeline')
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    new_header = wiretap.output_records[0].header
    new_value = wiretap.output_records[0].field
    # assert new header fields are created same as generated value fields
    assert new_header['values']['sha1passcode'] == new_value['sha1passcode'].value
    assert new_header['values']['myrecord'] == new_value['myrecord'].value
    assert new_header['values']['md5passcode'] == new_value['md5passcode'].value
    if Version(sdc_builder.version) < Version('3.7.0'):
        assert new_header['values']['sha2passcode'] == new_value['sha2passcode'].value
    if Version(sdc_executor.version) >= Version('3.7.0'):
        assert new_header['values']['sha256passcode'] == new_value['sha256passcode'].value
        assert new_header['values']['sha512passcode'] == new_value['sha512passcode'].value
    # assert in place record field being hashed as expected
    id_hash = hashlib.md5()
    id_hash.update(raw_id.encode())
    assert new_value['contact']['id'].value == id_hash.hexdigest()
    # assert new record field has an expected hash of MD5
    passcode_md5hash = hashlib.md5()
    passcode_md5hash.update(raw_passcode.encode())
    assert new_value['md5passcode'].value == passcode_md5hash.hexdigest()
    # assert new record field has an expected hash of SHA1
    passcode_sha1hash = hashlib.sha1()
    passcode_sha1hash.update(raw_passcode.encode())
    assert new_value['sha1passcode'].value == passcode_sha1hash.hexdigest()
    # assert new record field has an expected hash of SHA2
    passcode_sha2hash = hashlib.sha256()
    passcode_sha2hash.update(raw_passcode.encode())
    if Version(sdc_builder.version) < Version('3.7.0'):
        assert new_value['sha2passcode'].value == passcode_sha2hash.hexdigest()
    if Version(sdc_executor.version) >= Version('3.7.0'):
        # assert new record field has an expected hash of SHA256
        assert new_value['sha256passcode'].value == passcode_sha2hash.hexdigest()
        # assert new record field has an expected hash of SHA512
        passcode_sha512hash = hashlib.sha512()
        passcode_sha512hash.update(raw_passcode.encode())
        assert new_value['sha512passcode'].value == passcode_sha512hash.hexdigest()


def test_field_merger(sdc_builder, sdc_executor):
    """Test field merger processor. The pipeline would look like:

        dev_raw_data_source >> field_merger >> wiretap

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
    fields_to_merge = [
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
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)
    field_merger = pipeline_builder.add_stage('Field Merger')
    field_merger.set_attributes(fields_to_merge=fields_to_merge, overwrite_fields=True)
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_merger >> wiretap.destination
    pipeline = pipeline_builder.build('Field Merger pipeline')
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    new_value = wiretap.output_records[0].field
    # assert overwrite existing works
    assert len(new_value['dupecontact']) > 0
    # assert merge works by comparing `identity` to new `uniqueid` dict
    dict1 = raw_dict['identity']
    dict2 = new_value['uniqueid']
    assert len(dict1) == len(dict2)
    assert all(source_value == dict2[source_key].value for source_key, source_value in dict1.items())


def test_field_order(sdc_builder, sdc_executor):
    """Test field order processor. The pipeline would look like:

        dev_raw_data_source >> field_order >> wiretap

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
    default_value = 'USA'
    fields_to_order = ['/address/home/zipcode', '/address/home/state', extra_field]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)
    field_order = pipeline_builder.add_stage('Field Order')
    field_order.set_attributes(extra_fields='DISCARD', fields_to_order=fields_to_order,
                               missing_fields='USE_DEFAULT', default_type='STRING',
                               default_value=default_value, output_type='LIST_MAP')
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_order >> wiretap.destination
    pipeline = pipeline_builder.build('Field Order pipeline')
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    new_value = wiretap.output_records[0].field
    # assert we got ordered fields and we don't have discarded fields
    assert fields_to_order == ['/{}'.format(i.replace('"', '').replace('.', '/')) for i in new_value]
    # assert we got extra field value as expected
    extra_fields = [i for i in new_value if '/{}'.format(i.replace('"', '').replace('.', '/')) == extra_field]
    assert new_value[extra_fields[0]] == default_value


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
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)
    field_pivoter = pipeline_builder.add_stage('Field Pivoter')
    field_pivoter.set_attributes(copy_all_fields=True, field_to_pivot=field_to_pivot,
                                 original_field_name_path=field_name_path, pivoted_items_path=pivoted_item_path,
                                 save_original_field_name=True)
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_pivoter >> wiretap.destination
    pipeline = pipeline_builder.build('Field Pivoter pipeline')
    sdc_executor.add_pipeline(pipeline)
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


def test_field_remover(sdc_builder, sdc_executor):
    """Test field remover processor for three different actions. The pipeline would look like:

        dev_raw_data_source >> field_remover1 >> field_remover2 >> field_remover3
        field_remover1 >> wiretap1
        field_remover2 >> wiretap2
        field_remover3 >> wiretap3

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
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)
    field_remover1 = pipeline_builder.add_stage('Field Remover')
    field_remover1.set_attributes(fields=['/id', '/name'], action='REMOVE')
    field_remover2 = pipeline_builder.add_stage('Field Remover')
    field_remover2.set_attributes(fields=['/checknull1', '/checknull2'], action='REMOVE_NULL')
    field_remover3 = pipeline_builder.add_stage('Field Remover')
    field_remover3.set_attributes(fields=['/address/home/zipcode', '/address/home/country'], action='KEEP')
    wiretap1 = pipeline_builder.add_wiretap()
    wiretap2 = pipeline_builder.add_wiretap()
    wiretap3 = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_remover1 >> [field_remover2, wiretap1.destination]
    field_remover2 >> [field_remover3, wiretap2.destination]
    field_remover3 >> wiretap3.destination

    pipeline = pipeline_builder.build('Field Remover pipeline')
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    remover1_value = wiretap1.output_records[0].field
    remover2_value = wiretap2.output_records[0].field
    remover3_value = wiretap3.output_records[0].field

    # assert remove listed fields action
    assert 'name' not in remover1_value and 'id' not in remover1_value
    # assert remove listed fields if their values are null action
    assert 'checknull1' not in remover2_value and 'checknull2' not in remover2_value
    # assert keep only the listed fields action
    assert len(remover3_value) == 1 and 'state' not in remover3_value['address']['home']


@sdc_min_version('3.1.0.0')
def test_field_replacer(sdc_builder, sdc_executor):
    """Test field replacer processor. The pipeline would look like:

        dev_raw_data_source >> field_replacer >> wiretap

    With the given replacement rules, we do following:
        change value of ssn field to be 'XXX-XX-XXXX',
        set ranking field to null,
        set statistics field to null if it contains 'NA' or 'not_available'
    """
    winners = [dict(ssn='111-11-1111', year='2010', ranking='3', statistics='NA'),
               dict(ssn='111-22-1111', year='2011', ranking='2', statistics='2-3-3'),
               dict(ssn='111-33-1111', year='2012', ranking='1', statistics='not_available')]
    raw_data = ''.join([json.dumps(winner) for winner in winners])

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)
    field_replacer = pipeline_builder.add_stage('Field Replacer')
    field_replacer.replacement_rules = [{'setToNull': False, 'fields': '/ssn', 'replacement': 'XXX-XX-XXXX'},
                                        {'setToNull': True, 'fields': '/ranking'},
                                        {'setToNull': True,
                                         'fields': '/*[${f:value() == "NA" || f:value() == "not_available"}]'}]
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_replacer >> wiretap.destination
    pipeline = pipeline_builder.build('Field Replacer pipeline')
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    for rec in winners:
        rec['ssn'] = 'XXX-XX-XXXX'
        rec['ranking'] = None
        if rec['statistics'] == 'NA' or rec['statistics'] == 'not_available':
            rec['statistics'] = None
    actual_data = [rec.field for rec in wiretap.output_records]
    assert actual_data == winners


@sdc_min_version('5.3.0')
def test_field_replacer_non_existent_fields(sdc_builder, sdc_executor):
    """Test field replacer processor for non-existent fields. The pipeline would look like:

        dev_raw_data_source >> [field_replacer1, field_replacer2]
        field_replacer1 >> wiretap1
        field_replacer2 >> wiretap2

    With the given replacement rules, we do following:
    Field Replacer 1 -
        change value of 'name' field to be 'unknown',
        skip processing 'level' (a non-existent field),
        set value of 'laps' to null,
        skip processing 'cars' (a non-existent field)

    Field Replacer 2 -
        change value of 'name' field to be 'unknown',
        add field 'level' (a non-existent field) with value 'high',
        set value of 'laps' to null,
        add field 'cars' (a non-existent field) and set it to null
    """
    records = [dict(id=1, name='Harvey', laps=10),
               dict(id=2, name='Mike', laps=8)]
    raw_data = ''.join([json.dumps(record) for record in records])

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)

    field_replacer_1 = pipeline_builder.add_stage('Field Replacer')
    field_replacer_2 = pipeline_builder.add_stage('Field Replacer')

    # Case 1 - Include without processing : Skip non-existent fields
    field_replacer_1.set_attributes(field_does_not_exist='CONTINUE')
    field_replacer_1.replacement_rules = [{'setToNull': False, 'fields': '/name', 'replacement': 'unknown'},
                                          {'setToNull': False, 'fields': '/level', 'replacement': 'high'},
                                          {'setToNull': True, 'fields': '/laps'},
                                          {'setToNull': True, 'fields': '/cars'}]
    wiretap_1 = pipeline_builder.add_wiretap()

    # Case 2 - Add if not exist : Add non-existent fields
    field_replacer_2.set_attributes(field_does_not_exist='ADD_FIELD')
    field_replacer_2.replacement_rules = [{'setToNull': False, 'fields': '/name', 'replacement': 'unknown'},
                                          {'setToNull': False, 'fields': '/level', 'replacement': 'high'},
                                          {'setToNull': True, 'fields': '/laps'},
                                          {'setToNull': True, 'fields': '/cars'}]
    wiretap_2 = pipeline_builder.add_wiretap()

    dev_raw_data_source >> [field_replacer_1, field_replacer_2]
    field_replacer_1 >> wiretap_1.destination
    field_replacer_2 >> wiretap_2.destination

    pipeline = pipeline_builder.build('Field Replacer Pipeline')
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    records_1 = records
    records_2 = records

    for rec in records_1:
        rec['name'] = 'unknown'
        rec['laps'] = None
    actual_data_1 = [rec.field for rec in wiretap_1.output_records]
    # assert records_1 data
    assert actual_data_1 == records_1

    for rec in records_2:
        rec['name'] = 'unknown'
        rec['level'] = 'high'
        rec['laps'] = None
        rec['cars'] = None
    actual_data_2 = [rec.field for rec in wiretap_2.output_records]
    # assert records_2 data
    assert actual_data_2 == records_2


@sdc_min_version('3.1.0.0')
def test_field_replacer_self_referencing_expression(sdc_builder, sdc_executor):
    """Field Replacer supports self-referencing expressions and we need to make sure that they work properly and don't
    run into StackOverflowError (which can happen with some optimizations like SDC-14645).
    """
    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('Dev Raw Data Source')
    source.data_format='JSON'
    source.raw_data='{"key": "value"}'
    source.stop_after_first_batch = True

    replacer = builder.add_stage('Field Replacer')
    replacer.replacement_rules = [{'setToNull': False, 'fields': '/new', 'replacement': '${record:value("/")}'}]

    wiretap = builder.add_wiretap()

    source >> replacer >> wiretap.destination
    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 1
    assert wiretap.output_records[0].get_field_data('/new/key') == 'value'


def test_field_type_converter_by_field_type(sdc_builder, sdc_executor):
    """Test field type converter processor. We will use one stage to test field by field type conversion.
    The pipeline would look like:

        dev_raw_data_source >> field_type_converter_fields >> wiretap
    """
    utc_datetime_str = '1978-01-05 19:38:01'
    utc_datetime = datetime.strptime(utc_datetime_str, '%Y-%m-%d %H:%M:%S')
    utc_datetime_in_int = int(utc_datetime.strftime('%s')) * 1000  #multiply by 1000 to account for milliseconds
    raw_str_value = 'hello again!'
    # note, date time here is in UTC. Each map is an SDC record to process.
    raw_col = [{'amInteger': 123}, {'amDouble': 12345.6789115}, {'amString': 'hello'}, {'amBool': True},
               {'amDateTime': utc_datetime_str}, {'amString2': raw_str_value}, {'amZonedDateTime': None}]

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
        }, {
            'fields': ['/amZonedDateTime'],
            'targetType': 'ZONED_DATETIME'
        }
    ]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_type_converter_fields = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter_fields.set_attributes(conversion_method='BY_FIELD',
                                               field_type_converter_configs=field_type_converter_configs)
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_type_converter_fields >> wiretap.destination
    pipeline = pipeline_builder.build('Field Type Converter pipeline')
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    # assert field by field type conversion
    field_output = wiretap.output_records
    assert field_output[0].field['amInteger'].type == 'BYTE'
    assert field_output[1].field['amDouble'].type == 'INTEGER'
    assert field_output[2].field['amString'].type == 'CHAR'
    assert field_output[3].field['amBool'].type == 'BOOLEAN'
    assert field_output[4].field['amDateTime'].type == 'DATETIME'
    assert field_output[5].field['amString2'].type == 'BYTE_ARRAY'
    assert field_output[6].field['amZonedDateTime'].type == 'ZONED_DATETIME'
    # assert value which can be compared
    assert utc_datetime_in_int == int(field_output[4].field['amDateTime'].value.timestamp() * 1000)
    assert 'b\'' + raw_str_value + '\'' == str(field_output[5].field['amString2'].value)


def test_field_type_converter_by_data_type(sdc_builder, sdc_executor):
    """Test field type converter processor. We will use two stages to test field by field type conversion and
    data type conversion. The pipeline would look like:

        dev_raw_data_source >> field_type_converter_fields >> field_type_converter_types >> wiretap

    With given 2 stages for converter, field_type_converter_fields will convert field for field conversion. For e.g.,
    record1's /amInteger will convert from INTEGER to BYTE. field_type_converter_types will convert any field type to
    any field type if it matches the type criteria. For e.g., /amDateTime field type DATETIME will convert to LONG.
    """
    utc_datetime_str = '1978-01-05 19:38:01'
    utc_datetime = datetime.strptime(utc_datetime_str, '%Y-%m-%d %H:%M:%S')
    # add an hour and multiply by 1000 to account for milliseconds
    utc_datetime_in_int = int(utc_datetime.strftime('%s')) * 1000
    raw_str_value = 'hello again!'
    # note, date time here is in UTC. Each map is an SDC record to process.
    raw_col = [{'amInteger': 123}, {'amDouble': 12345.6789115}, {'amString': 'hello'}, {'amBool': True},
               {'amDateTime': utc_datetime_str}, {'amString2': raw_str_value}, {'amZonedDateTime': None}]
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
        }, {
            'fields': ['/amZonedDateTime'],
            'targetType': 'ZONED_DATETIME'
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
            'dataLocale': 'en,US'
        }, {
            'sourceType': 'BYTE_ARRAY',
            'targetType': 'STRING',
            'treatInputFieldAsDate': False,
            'encoding': 'UTF-8'
        }, {
            'sourceType': 'ZONED_DATETIME',
            'targetType': 'STRING',
            'encoding': 'UTF-8'
        }
    ]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_type_converter_fields = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter_fields.set_attributes(conversion_method='BY_FIELD',
                                               field_type_converter_configs=field_type_converter_configs)
    field_type_converter_types = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter_types.set_attributes(conversion_method='BY_TYPE',
                                              whole_type_converter_configs=whole_type_converter_configs)
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_type_converter_fields >> field_type_converter_types >> wiretap.destination
    pipeline = pipeline_builder.build('Field Type Converter pipeline')
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    # assert data type conversion
    type_output = wiretap.output_records

    assert type_output[0].field['amInteger'].type == 'DECIMAL'
    assert type_output[1].field['amDouble'].type == 'SHORT'
    assert type_output[2].field['amString'].type == 'STRING'
    assert type_output[3].field['amBool'].type == 'STRING'
    assert type_output[4].field['amDateTime'].type == 'LONG'
    assert type_output[5].field['amString2'].type == 'STRING'
    assert type_output[6].field['amZonedDateTime'].type == 'STRING'
    # assert values which can be compared
    assert utc_datetime_in_int == int(type_output[4].field['amDateTime'].value)
    assert raw_str_value == type_output[5].field['amString2'].value


def test_field_type_converter_long_decimals(sdc_builder, sdc_executor):
    """
    This is a test for SDC-10949.

    This test creates a raw data -> converter -> wiretap pipeline.  The raw data will contain a decimal (in STRING form)
    with a high precision.  The converter will convert this to DECIMAL type, and we assert that all digits were
    preserved in the process.
    """
    decimal_str_val = '11235813213455.55342113853211';
    raw_data = json.dumps([{'largeDecimal': decimal_str_val}])
    field_type_converter_configs = [
        {
            'fields': ['/largeDecimal'],
            'targetType': 'DECIMAL',
            'dataLocale': 'en,US',
            'scale': -1,
            'decimalScaleRoundingStrategy': 'ROUND_UNNECESSARY'
        }
    ]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data)

    # 1st pipeline where check field type before use Field Type Converter
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> wiretap.destination
    pipeline_test = pipeline_builder.build('Field without Converter')
    sdc_executor.add_pipeline(pipeline_test)
    sdc_executor.start_pipeline(pipeline_test)
    sdc_executor.wait_for_pipeline_metric(pipeline_test, 'data_batch_count', 1)
    sdc_executor.stop_pipeline(pipeline_test)

    # assert field coming out of origin is STRING (sanity check)
    raw_output = wiretap.output_records
    assert raw_output[0].field['largeDecimal'].type == 'STRING'

    # 2nd pipeline with Field Type Converter
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_type_converter_fields = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter_fields.set_attributes(conversion_method='BY_FIELD',
                                               field_type_converter_configs=field_type_converter_configs)
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_type_converter_fields >> wiretap.destination
    pipeline = pipeline_builder.build('Field Type Converter large decimal pipeline')
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    # assertions on field coming out of field type converter
    field_output = wiretap.output_records
    # assert the type
    assert field_output[0].field['largeDecimal'].type == 'DECIMAL'
    # and value
    assert field_output[0].field['largeDecimal'].value == Decimal(decimal_str_val)


# SDC-11561: File Type Converter doesn't work properly with null in MAP and LIST types
@sdc_min_version('3.9.0') # For the JavaScript processor use
def test_field_type_converter_null_map(sdc_builder, sdc_executor):
    """Make sure that the processor doesn't fail (does a no-op) on a map that is null."""
    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Dev Raw Data Source')
    origin.data_format = 'TEXT'
    origin.raw_data = 'Does not matter'
    origin.stop_after_first_batch = True

    javascript = builder.add_stage('JavaScript Evaluator')
    javascript.record_type = 'SDC_RECORDS'
    javascript.init_script = ''
    javascript.destroy_script = ''
    javascript.script =  """
          var Field = Java.type('com.streamsets.pipeline.api.Field');
          for (var i = 0; i < records.length; i++) {
            records[i].sdcRecord.set(Field.create(Field.Type.MAP, null));
            output.write(records[i]);
          }
        """

    converter = builder.add_stage('Field Type Converter')
    converter.conversion_method = 'BY_FIELD'
    converter.field_type_converter_configs = [{
        'fields': ['/somethingSomewhere'],
        'targetType': 'DECIMAL',
        'dataLocale': 'en,US',
        'scale': -1,
        'decimalScaleRoundingStrategy': 'ROUND_UNNECESSARY'
    }]

    trash = builder.add_stage('Trash')

    origin >> javascript >> converter >> trash

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    # No exceptions, so the pipeline should be in finished state
    status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
    assert status == 'FINISHED'

    history = sdc_executor.get_pipeline_history(pipeline)
    assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 1
    assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 1


# SDC-8583 fixed this in 3.16.0
@sdc_min_version('3.16.0')
def test_field_type_converter_trim(sdc_builder, sdc_executor):
    """Make sure that we properly trim if doing conversion from String to some of the other types."""
    raw_data = json.dumps([{
        'short': ' 123 ',
        'long': ' 123 ',
        'integer': ' 123 ',
        'float': ' 123.5 ',
        'double': ' 123.5 ',
        'decimal': ' 123.5 ',
        'zonedDatetime': ' 2011-12-03T10:15:30+01:00[Europe/Paris] ',
        'datetime': ' 1978-01-05 19:38:01 ',
        'time': '1978-01-05 19:38:01 ',
        'date': ' 1978-01-05 19:38:01 ',
        'boolean': ' true ',
    }])

    field_type_converter_configs = [
        {
            'fields': ['/short'],
            'targetType': 'SHORT',
        }, {
            'fields': ['/long'],
            'targetType': 'LONG',
        }, {
            'fields': ['/integer'],
            'targetType': 'INTEGER'
        }, {
            'fields': ['/float'],
            'targetType': 'FLOAT'
        }, {
            'fields': ['/double'],
            'targetType': 'DOUBLE'
        }, {
            'fields': ['/decimal'],
            'targetType': 'DECIMAL',
            'scale': -1,
            'decimalScaleRoundingStrategy': 'ROUND_UNNECESSARY'
        }, {
            'fields': ['/zonedDatetime'],
            'targetType': 'ZONED_DATETIME'
        }, {
            'fields': ['/time'],
            'targetType': 'TIME',
            'dateFormat': 'YYYY_MM_DD_HH_MM_SS'
        }, {
            'fields': ['/datetime'],
            'targetType': 'DATETIME',
            'dateFormat': 'YYYY_MM_DD_HH_MM_SS'
        }, {
            'fields': ['/date'],
            'targetType': 'DATE',
            'dateFormat': 'YYYY_MM_DD_HH_MM_SS'
        }, {
            'fields': ['/boolean'],
            'targetType': 'BOOLEAN'
        }
    ]

    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('Dev Raw Data Source')
    source.data_format = 'JSON'
    source.json_content = 'ARRAY_OBJECTS'
    source.raw_data = raw_data
    source.stop_after_first_batch = True

    converter = builder.add_stage('Field Type Converter')
    converter.conversion_method = 'BY_FIELD'
    converter.field_type_converter_configs = field_type_converter_configs

    wiretap = builder.add_wiretap()

    source >> converter >> wiretap.destination
    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    # Verify metadata (types)
    output = wiretap.output_records
    assert output[0].field['short'].type == 'SHORT'
    assert output[0].field['short'].value == 123

    assert output[0].field['long'].type == 'LONG'
    assert output[0].field['long'].value == 123

    assert output[0].field['integer'].type == 'INTEGER'
    assert output[0].field['integer'].value == 123

    assert output[0].field['float'].type == 'FLOAT'
    assert output[0].field['float'].value == 123.5

    assert output[0].field['double'].type == 'DOUBLE'
    assert output[0].field['double'].value == 123.5

    assert output[0].field['decimal'].type == 'DECIMAL'
    assert output[0].field['decimal'].value == 123.5

    assert output[0].field['zonedDatetime'].type == 'ZONED_DATETIME'
    assert output[0].field['zonedDatetime'].value == '2011-12-03T10:15:30+01:00[Europe/Paris]'

    assert output[0].field['datetime'].type == 'DATETIME'
    assert output[0].field['datetime'].value == datetime(1978, 1, 5, 19, 38, 1)

    assert output[0].field['time'].type == 'TIME'
    assert output[0].field['time'].value == datetime(1978, 1, 5, 19, 38, 1)

    assert output[0].field['date'].type == 'DATE'
    assert output[0].field['date'].value == datetime(1978, 1, 5, 19, 38, 1)

    assert output[0].field['boolean'].type == 'BOOLEAN'
    assert output[0].field['boolean'].value == True

@pytest.mark.parametrize('input,target_type', [
    ('r', 'SHORT'),
    ('r', 'INTEGER'),
    ('r', 'LONG'),
    ('r', 'FLOAT'),
    ('r', 'DOUBLE'),
    ('r', 'DECIMAL'),
    ('r', 'DATE'),
    ('r', 'TIME'),
    ('99:99:99', 'TIME'),
    ('r', 'DATETIME'),
    ('r', 'ZONED_DATETIME'),
])
# SDC-16121: Field Type Converter propagates ZONED_DATETIME parsing error as StageError
def test_field_type_converter_parse_errors(sdc_builder, sdc_executor, input, target_type):
    """Ensure that various data-related errors will end up in error stream and won't stop the pipeline"""
    raw_data = json.dumps([{'value': input}])
    converter_configs = [
        {
            'fields': ['/value'],
            'targetType': target_type,
            'dataLocale': 'en,US',
            'scale': -1,
            'decimalScaleRoundingStrategy': 'ROUND_UNNECESSARY',
            "dateFormat": "YYYY_MM_DD",
            "zonedDateTimeFormat": "ISO_ZONED_DATE_TIME",
            "encoding": "UTF-8"
        }
    ]

    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('Dev Raw Data Source')
    source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data,
                          stop_after_first_batch=True)

    converter = builder.add_stage('Field Type Converter')
    converter.set_attributes(conversion_method='BY_FIELD', field_type_converter_configs=converter_configs)

    wiretap = builder.add_wiretap()

    source >> converter >> wiretap.destination

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 0
    assert len(wiretap.error_records) == 1
    assert wiretap.error_records[0].header['errorCode'] == 'CONVERTER_00'


@pytest.mark.parametrize('target_type', [
     'CHAR',
     'BYTE',
     'SHORT',
     'INTEGER',
     'LONG',
     'FLOAT',
     'DOUBLE',
     'DECIMAL',
     'DATE',
     'TIME',
     'DATETIME',
     'ZONED_DATETIME',
])
# SDC-16770: field type converter is not ignoring empty string when converting them to double
def test_field_type_converter_source_empty_error_by_type(sdc_builder, sdc_executor, target_type):
    """Ensure that an error record is given when trying to convert from emtpy string"""
    raw_data = json.dumps([{'empty': '', 'list': ['']}, ''])
    converter_configs = [
        {
            'sourceType': 'STRING',
            'targetType': target_type,
            'dataLocale': 'en,US',
            'scale': -1,
            'decimalScaleRoundingStrategy': 'ROUND_UNNECESSARY',
            "dateFormat": "YYYY_MM_DD_HH_MM_SS",
            "encoding": "UTF-8"
        }
    ]

    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('Dev Raw Data Source')
    source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data,
                          stop_after_first_batch=True)

    converter = builder.add_stage('Field Type Converter')
    converter.set_attributes(conversion_method='BY_TYPE', whole_type_converter_configs=converter_configs)

    wiretap = builder.add_wiretap()

    source >> converter >> wiretap.destination

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 0
    assert len(wiretap.error_records) == 2
    for record in wiretap.error_records:
        assert record.header['errorCode'] == 'CONVERTER_00'


@sdc_min_version('4.0.0')
@pytest.mark.parametrize('target_type', [
     'CHAR',
     'BYTE',
     'SHORT',
     'INTEGER',
     'LONG',
     'FLOAT',
     'DOUBLE',
     'DECIMAL',
     'DATE',
     'TIME',
     'DATETIME',
     'ZONED_DATETIME',
])
# SDC-16770: field type converter is not ignoring empty string when converting them to double
def test_field_type_converter_source_empty_null_by_type(sdc_builder, sdc_executor, target_type):
    """Ensure that converting from emtpy string with the NULL config results in null"""
    raw_data = json.dumps([{'empty': '', 'list': ['']}, ''])
    converter_configs = [
        {
            'sourceType': 'STRING',
            'targetType': target_type,
            'dataLocale': 'en,US',
            'scale': -1,
            'decimalScaleRoundingStrategy': 'ROUND_UNNECESSARY',
            "dateFormat": "YYYY_MM_DD_HH_MM_SS",
            "encoding": "UTF-8",
            'inputFieldEmpty': 'NULL'
        }
    ]

    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('Dev Raw Data Source')
    source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data,
                          stop_after_first_batch=True)

    converter = builder.add_stage('Field Type Converter')
    converter.set_attributes(conversion_method='BY_TYPE', whole_type_converter_configs=converter_configs)

    wiretap = builder.add_wiretap()

    source >> converter >> wiretap.destination

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 2
    assert len(wiretap.error_records) == 0

    output = wiretap.output_records

    assert output[0].field['empty'].type == target_type
    assert output[0].field['empty'].value is None

    assert len(output[0].field['list']) == 1
    assert output[0].field['list'][0].type == target_type
    assert output[0].field['list'][0].value is None

    assert output[1].field.type == target_type
    assert output[1].field.value is None


@sdc_min_version('4.0.0')
@pytest.mark.parametrize('target_type', [
     'CHAR',
     'BYTE',
     'SHORT',
     'INTEGER',
     'LONG',
     'FLOAT',
     'DOUBLE',
     'DECIMAL',
     'DATE',
     'TIME',
     'DATETIME',
     'ZONED_DATETIME',
])
# SDC-16770: field type converter is not ignoring empty string when converting them to double
def test_field_type_converter_source_empty_delete_by_type(sdc_builder, sdc_executor, target_type):
    """Ensure that emtpy string fields are deleted with the DELETE config"""
    raw_data = json.dumps([{'empty': '', 'list': ['']}, ''])
    converter_configs = [
        {
            'sourceType': 'STRING',
            'targetType': target_type,
            'dataLocale': 'en,US',
            'scale': -1,
            'decimalScaleRoundingStrategy': 'ROUND_UNNECESSARY',
            "dateFormat": "YYYY_MM_DD_HH_MM_SS",
            "encoding": "UTF-8",
            'inputFieldEmpty': 'DELETE'
        }
    ]

    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('Dev Raw Data Source')
    source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data,
                          stop_after_first_batch=True)

    converter = builder.add_stage('Field Type Converter')
    converter.set_attributes(conversion_method='BY_TYPE', whole_type_converter_configs=converter_configs)

    wiretap = builder.add_wiretap()

    source >> converter >> wiretap.destination

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 1
    assert len(wiretap.error_records) == 0

    output = wiretap.output_records

    assert 'empty' not in output[0].field

    assert len(output[0].field['list']) == 0


@sdc_min_version('4.0.0')
@pytest.mark.parametrize('target_type', [
     'CHAR',
     'BYTE',
     'SHORT',
     'INTEGER',
     'LONG',
     'FLOAT',
     'DOUBLE',
     'DECIMAL',
     'DATE',
     'TIME',
     'DATETIME',
     'ZONED_DATETIME',
])
# SDC-16770: field type converter is not ignoring empty string when converting them to double
def test_field_type_converter_source_empty_ignore_by_type(sdc_builder, sdc_executor, target_type):
    """Ensure that empty strings are not converted with the IGNORE config"""
    raw_data = json.dumps([{'empty': '', 'list': ['']}, ''])
    converter_configs = [
        {
            'sourceType': 'STRING',
            'targetType': target_type,
            'dataLocale': 'en,US',
            'scale': -1,
            'decimalScaleRoundingStrategy': 'ROUND_UNNECESSARY',
            "dateFormat": "YYYY_MM_DD_HH_MM_SS",
            "encoding": "UTF-8",
            'inputFieldEmpty': 'IGNORE'
        }
    ]

    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('Dev Raw Data Source')
    source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data,
                          stop_after_first_batch=True)

    converter = builder.add_stage('Field Type Converter')
    converter.set_attributes(conversion_method='BY_TYPE', whole_type_converter_configs=converter_configs)

    wiretap = builder.add_wiretap()

    source >> converter >> wiretap.destination

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 2
    assert len(wiretap.error_records) == 0

    output = wiretap.output_records

    assert output[0].field['empty'].type == 'STRING'
    assert output[0].field['empty'].value == ''

    assert len(output[0].field['list']) == 1
    assert output[0].field['list'][0].type == 'STRING'
    assert output[0].field['list'][0].value == ''

    assert output[1].field.type == 'STRING'
    assert output[1].field.value == ''


@pytest.mark.parametrize('target_type', [
     'CHAR',
     'BYTE',
     'SHORT',
     'INTEGER',
     'LONG',
     'FLOAT',
     'DOUBLE',
     'DECIMAL',
     'DATE',
     'TIME',
     'DATETIME',
     'ZONED_DATETIME',
])
# SDC-16770: field type converter is not ignoring empty string when converting them to double
def test_field_type_converter_source_empty_error_by_field(sdc_builder, sdc_executor, target_type):
    """Ensure that an error record is given when trying to convert from emtpy string"""
    raw_data = json.dumps([{'value': ''}])
    converter_configs = [
        {
            'fields': ['/value'],
            'targetType': target_type,
            'dataLocale': 'en,US',
            'scale': -1,
            'decimalScaleRoundingStrategy': 'ROUND_UNNECESSARY',
            "dateFormat": "YYYY_MM_DD_HH_MM_SS",
            "encoding": "UTF-8",
        }
    ]

    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('Dev Raw Data Source')
    source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data,
                          stop_after_first_batch=True)

    converter = builder.add_stage('Field Type Converter')
    converter.set_attributes(conversion_method='BY_FIELD', field_type_converter_configs=converter_configs)

    wiretap = builder.add_wiretap()

    source >> converter >> wiretap.destination

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 0
    assert len(wiretap.error_records) == 1
    for record in wiretap.error_records:
        assert record.header['errorCode'] == 'CONVERTER_00'


@sdc_min_version('4.0.0')
@pytest.mark.parametrize('target_type', [
     'CHAR',
     'BYTE',
     'SHORT',
     'INTEGER',
     'LONG',
     'FLOAT',
     'DOUBLE',
     'DECIMAL',
     'DATE',
     'TIME',
     'DATETIME',
     'ZONED_DATETIME',
])
# SDC-16770: field type converter is not ignoring empty string when converting them to double
def test_field_type_converter_source_empty_null_by_field(sdc_builder, sdc_executor, target_type):
    """Ensure that converting from emtpy string with the NULL config results in null"""
    raw_data = json.dumps([{'value': ''}])
    converter_configs = [
        {
            'fields': ['/value'],
            'targetType': target_type,
            'dataLocale': 'en,US',
            'scale': -1,
            'decimalScaleRoundingStrategy': 'ROUND_UNNECESSARY',
            "dateFormat": "YYYY_MM_DD_HH_MM_SS",
            "encoding": "UTF-8",
            'inputFieldEmpty': 'NULL'
        }
    ]

    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('Dev Raw Data Source')
    source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data,
                          stop_after_first_batch=True)

    converter = builder.add_stage('Field Type Converter')
    converter.set_attributes(conversion_method='BY_FIELD', field_type_converter_configs=converter_configs)

    wiretap = builder.add_wiretap()

    source >> converter >> wiretap.destination

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 1
    assert len(wiretap.error_records) == 0

    output = wiretap.output_records

    assert output[0].field['value'].type == target_type
    assert output[0].field['value'].value is None


@sdc_min_version('4.0.0')
@pytest.mark.parametrize('target_type', [
     'CHAR',
     'BYTE',
     'SHORT',
     'INTEGER',
     'LONG',
     'FLOAT',
     'DOUBLE',
     'DECIMAL',
     'DATE',
     'TIME',
     'DATETIME',
     'ZONED_DATETIME',
])
# SDC-16770: field type converter is not ignoring empty string when converting them to double
def test_field_type_converter_source_empty_delete_by_field(sdc_builder, sdc_executor, target_type):
    """Ensure that emtpy string fields are deleted with the DELETE config"""
    raw_data = json.dumps([{'value': ''}])
    converter_configs = [
        {
            'fields': ['/value'],
            'targetType': target_type,
            'dataLocale': 'en,US',
            'scale': -1,
            'decimalScaleRoundingStrategy': 'ROUND_UNNECESSARY',
            "dateFormat": "YYYY_MM_DD_HH_MM_SS",
            "encoding": "UTF-8",
            'inputFieldEmpty': 'DELETE'
        }
    ]

    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('Dev Raw Data Source')
    source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data,
                          stop_after_first_batch=True)

    converter = builder.add_stage('Field Type Converter')
    converter.set_attributes(conversion_method='BY_FIELD', field_type_converter_configs=converter_configs)

    wiretap = builder.add_wiretap()

    source >> converter >> wiretap.destination

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 1
    assert len(wiretap.error_records) == 0

    output = wiretap.output_records

    assert 'value' not in output[0].field


@sdc_min_version('4.0.0')
@pytest.mark.parametrize('target_type', [
     'CHAR',
     'BYTE',
     'SHORT',
     'INTEGER',
     'LONG',
     'FLOAT',
     'DOUBLE',
     'DECIMAL',
     'DATE',
     'TIME',
     'DATETIME',
     'ZONED_DATETIME',
])
# SDC-16770: field type converter is not ignoring empty string when converting them to double
def test_field_type_converter_source_empty_ignore_by_field(sdc_builder, sdc_executor, target_type):
    """Ensure that empty strings are not converted with the IGNORE config"""
    raw_data = json.dumps([{'value': ''}])
    converter_configs = [
        {
            'fields': ['/value'],
            'targetType': target_type,
            'dataLocale': 'en,US',
            'scale': -1,
            'decimalScaleRoundingStrategy': 'ROUND_UNNECESSARY',
            "dateFormat": "YYYY_MM_DD_HH_MM_SS",
            "encoding": "UTF-8",
            'inputFieldEmpty': 'IGNORE'
        }
    ]

    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('Dev Raw Data Source')
    source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data,
                          stop_after_first_batch=True)

    converter = builder.add_stage('Field Type Converter')
    converter.set_attributes(conversion_method='BY_FIELD', field_type_converter_configs=converter_configs)

    wiretap = builder.add_wiretap()

    source >> converter >> wiretap.destination

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 1
    assert len(wiretap.error_records) == 0

    output = wiretap.output_records

    assert output[0].field['value'].type == 'STRING'
    assert output[0].field['value'].value == ''


@sdc_min_version('5.1.0')
def test_field_type_converter_unix_timestamp(sdc_builder, sdc_executor):
    """
    Test the Field Type Converter can convert Unix Timestamp values to dates.

    This test creates a Raw Data -> Field Type Converter -> Wiretap pipeline. The raw data will contain a Unix Timestamp
    stored in seconds and milliseconds, written as a number and as a String, each of which the Converter shall transform
    into a Date. The test asserts that these dates correspond to the initial Unix Timestamp.
    """
    timestamp_in_milliseconds = int(time.time() * 1000)
    timestamp_in_seconds = int(timestamp_in_milliseconds / 1000)

    raw_data = json.dumps([
        {
            'timestamp_in_milliseconds': timestamp_in_milliseconds,
            'string_timestamp_in_milliseconds': str(timestamp_in_milliseconds),
            'timestamp_in_seconds': timestamp_in_seconds,
            'string_timestamp_in_seconds': str(timestamp_in_seconds)
        }
    ])
    field_type_converter_configs = [
        {
            'fields': [
                '/timestamp_in_milliseconds',
                '/string_timestamp_in_milliseconds'
            ],
            'targetType': 'DATETIME',
            'dateFormat': 'UNIX_TIMESTAMP_IN_MILLISECONDS'
        },
        {
            'fields': [
                '/timestamp_in_seconds',
                '/string_timestamp_in_seconds'
            ],
            'targetType': 'DATETIME',
            'dateFormat': 'UNIX_TIMESTAMP_IN_SECONDS'
        }
    ]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='JSON',
        json_content='ARRAY_OBJECTS',
        raw_data=raw_data,
        stop_after_first_batch=True
    )
    field_type_converter_fields = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter_fields.set_attributes(
        conversion_method='BY_FIELD',
        field_type_converter_configs=field_type_converter_configs
    )
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_type_converter_fields >> wiretap.destination
    pipeline = pipeline_builder.build('Field Type Converter unix timestamp to date pipeline')
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    # assert the fields coming out of the Field Type Converter
    field_output = wiretap.output_records
    expected_datetime_for_milliseconds = datetime.utcfromtimestamp(timestamp_in_milliseconds / 1000.0)
    expected_datetime_for_seconds = datetime.utcfromtimestamp(timestamp_in_seconds)
    final_fields_and_values = {
        'timestamp_in_milliseconds': expected_datetime_for_milliseconds,
        'string_timestamp_in_milliseconds': expected_datetime_for_milliseconds,
        'timestamp_in_seconds': expected_datetime_for_seconds,
        'string_timestamp_in_seconds': expected_datetime_for_seconds
    }

    for field, value in final_fields_and_values.items():
        assert field_output[0].field[field].type == 'DATETIME'
        assert field_output[0].field[field] == value


def test_field_zip(sdc_builder, sdc_executor):
    """Test field zip processor. The pipeline would look like:

        dev_raw_data_source >> field_zip >> wiretap

    With given config, /basics and /additional will zip to /all path. For e.g., /all will have be a list of
    2 zipped maps of {id: 23, inventory:80}, {color: 20005, cost:5} similarly for /itemID and /cost.
    """
    raw_data = """
        [
          {
            "order": 23523482,
            "itemID": [2, 113, 954, 6502],
            "cost": [89.95, 8.95],
            "basics": [{"id": 23, "color": 20005}],
            "additional": [{"inventory": 80, "cost": 5}]
          },
          {
            "order": 23523481,
            "basics": [{"id": 23, "color": 20005}],
            "additional": [{"inventory": 80, "cost": 5}]
          }
        ]
    """
    raw_list = json.loads(raw_data)
    result_key_1 = 'purchase'
    result_key_2 = 'all'
    fields_to_zip_configs = [
        {
            'zippedFieldPath': f'/{result_key_1}',
            'firstField': '/itemID',
            'secondField': '/cost'
        }, {
            'zippedFieldPath': f'/{result_key_2}',
            'firstField': '/basics',
            'secondField': '/additional'
        }
    ]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data)
    field_zip = pipeline_builder.add_stage('Field Zip')
    field_zip.set_attributes(field_does_not_exist='CONTINUE', fields_to_zip=fields_to_zip_configs,
                             zip_values_only=False)
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_zip >> wiretap.destination

    pipeline = pipeline_builder.build('Field Zip pipeline')
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'data_batch_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        record_result = wiretap.output_records[0].field
        # assert we got expected number of merge fields
        assert len(raw_list[0]) + len(fields_to_zip_configs) == len(record_result)
        # assert data is merged as expected
        raw_merge = list(zip(raw_list[0]['itemID'], raw_list[0]['cost']))
        record_field_result = record_result[result_key_1]
        record_field_merge = [tuple(float(b.value) for b in a.values()) for a in record_field_result]
        assert raw_merge == record_field_merge
        # assert the missing record fields do not merge anything
        assert result_key_1 not in wiretap.output_records[1].field
        assert result_key_2 not in wiretap.output_records[1].field
    finally:
        sdc_executor.remove_pipeline(pipeline)


@pytest.mark.parametrize('first_field,second_field,field_data_type', [
    ('list1', 'list2', 'list'),
    ('list', 'map', 'list_map'),
    ('map1', 'map2', 'map'),
])
def test_field_zip_fields_to_zip(sdc_builder, sdc_executor, first_field, second_field, field_data_type):
    """Test field zip processor. The pipeline would look like:

        dev_raw_data_source >> field_zip >> wiretap

    With given config, /first_field and /second_field will zip to /zipped path.
    """
    if field_data_type=='list':
      raw_data = """
              [
                {
                  "list1": [1,2],
                  "list2": [11,12,13,14]
                }
              ]
          """
    elif field_data_type=='list_map':
      raw_data = """
              [
                {
                  "list": [1,2,3],
                  "map": [
                            {"mapField1":"11"},
                            {"mapField2":"12"},
                            {"mapField3":"13"}
                         ]
                }
              ]
          """
    elif field_data_type=='map':
          raw_data = """
              [
                {
                  "map1": [
                            {"mapField1":"1"},
                            {"mapField2":"2"},
                            {"mapField3":"3"}
                          ],
                  "map2": [
                            {"mapField11":"11"},
                            {"mapField12":"12"},
                            {"mapField13":"13"}
                         ]
                }
              ]
          """

    raw_list = json.loads(raw_data)
    fields_to_zip_configs = [
        {
            'zippedFieldPath': '/zipped',
            'firstField': f'/{first_field}',
            'secondField': f'/{second_field}'
        }
    ]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       json_content='ARRAY_OBJECTS',
                                       raw_data=raw_data)
    field_zip = pipeline_builder.add_stage('Field Zip')
    field_zip.set_attributes(field_does_not_exist='TO_ERROR',
                             fields_to_zip=fields_to_zip_configs,
                             zip_values_only=False)
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_zip >> wiretap.destination

    pipeline = pipeline_builder.build('Field Zip Test Fields To Zip pipeline')
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'data_batch_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        record_result = wiretap.output_records[0].field

        # assert we got expected number of merge fields
        assert len(raw_list[0]) + len(fields_to_zip_configs) == len(record_result), "Got wrong number of merge fields"

        # assert data is merged as expected
        raw_merge = list(zip(raw_list[0][first_field], raw_list[0][second_field]))
        record_field_result = record_result['zipped']

        if field_data_type=='list':
          record_field_merge = [tuple(int(b.value) for b in a.values()) for a in record_field_result]
        else:
          record_field_merge = [tuple(value for value in a.values()) for a in record_field_result]
        assert raw_merge == record_field_merge, "Got wrong merge field record result"
    finally:
        sdc_executor.remove_pipeline(pipeline)


def test_value_replacer(sdc_builder, sdc_executor):
    """Test Value Replacer processor replacing values in fields. The pipeline would look like:

        dev_raw_data_source >> value_replacer >> wiretap
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
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)
    value_replacer = pipeline_builder.add_stage('Value Replacer', type='processor')
    value_replacer.set_attributes(conditionally_replace_values=[{
        'fieldNames': ['/contact/address/home/state', '/contact/state'],
        'operator': 'ALL',
        'comparisonValue': 'North Carolina',
        'replacementValue': expected_state_value
    }], replace_null_values=[{
        'fields': ['/contact/password'],
        'newValue': expected_password_value
    }], fields_to_null=[{
        'fieldsToNull': ['/contact/*name'],
        'condition': "${record:value('/contact/id') > 0}"
    }])
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> value_replacer >> wiretap.destination

    pipeline = pipeline_builder.build('Value Replacer pipeline')
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    new_value = wiretap.output_records[0].field['contact']
    # assert fields to null
    assert new_value['fname'].value is new_value['lname'].value is None
    # assert replace null values
    assert expected_password_value == new_value['password'].value
    # assert conditionally replace values
    assert expected_state_value == new_value['state'].value
    assert expected_state_value == new_value['address']['home']['state'].value


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
      operate_on = 'FIELD_VALUES',
      conditional_expression = '${f:name() == "key"}',
      mapping_expression = '${record:value("/")}'
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
      operate_on = 'FIELD_PATHS',
      conditional_expression = '${f:type() == \'INTEGER\' and str:startsWith(f:name(), \'value\')}',
      mapping_expression = '/outputs/max',
      aggregation_expression = '${max(fields)}',
      maintain_original_paths = True
    )
    field_mapper_min = pipeline_builder.add_stage('Field Mapper', type='processor')
    field_mapper_min.set_attributes(
      operate_on = 'FIELD_PATHS',
      conditional_expression = '${f:type() == \'INTEGER\' and str:startsWith(f:name(), \'value\')}',
      mapping_expression = '/outputs/min',
      aggregation_expression = '${min(fields)}',
      maintain_original_paths = True
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
      operate_on = 'FIELD_PATHS',
      conditional_expression = '${str:contains(str:toLower(f:name()), \'dave\') or' +
        '(f:type() == \'STRING\' and str:contains(str:toLower(f:value()), \'dave\'))}',
      mapping_expression = '/outputs/daves',
      aggregation_expression = '${map(fields, fieldByPreviousPath())}',
      maintain_original_paths = True
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
      operate_on = 'FIELD_NAMES',
      mapping_expression = '${str:replaceAll(f:name(), \'z\', \'2\')}',
      maintain_original_paths = False
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
      operate_on = 'FIELD_NAMES',
      conditional_expression = "${str:matches(f:name(),'req_.*') == false}",
      mapping_expression = 'data_${f:name()}'
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
      operate_on = 'FIELD_VALUES',
      conditional_expression = '${f:type() == \'DOUBLE\'}',
      mapping_expression = '${math:ceil(f:value())}',
      maintain_original_paths = False
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
