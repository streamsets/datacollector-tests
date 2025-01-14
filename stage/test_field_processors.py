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
from collections import OrderedDict

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
