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
    decimal_str_val = '11235813213455.55342113853211'
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


@sdc_min_version('5.10.0')
def test_field_type_converter_decimal_headers(sdc_builder, sdc_executor):
    decimal_val = '1125.3211'
    raw_data = json.dumps({"decimalAsString": "1125.3211", "decimalAsNumber": 1125.3211})
    field_type_converter_configs = [
        {
            'fields': ['/decimalAsString'],
            'targetType': 'DECIMAL',
            'dataLocale': 'en,US',
            'scale': -1,
            'decimalScaleRoundingStrategy': 'ROUND_UNNECESSARY'
        }, {
            'fields': ['/decimalAsNumber'],
            'targetType': 'DECIMAL',
            'dataLocale': 'en,US',
            'scale': -1,
            'decimalScaleRoundingStrategy': 'ROUND_UNNECESSARY'
        }
    ]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='MULTIPLE_OBJECTS', raw_data=raw_data,
                                       stop_after_first_batch=True)

    field_type_converter_fields = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter_fields.set_attributes(conversion_method='BY_FIELD',
                                               field_type_converter_configs=field_type_converter_configs)

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_type_converter_fields >> wiretap.destination
    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    record = wiretap.output_records[0]

    assert 'decimalAsString' in record.field, 'decimalAsString not found in record'
    assert 'decimalAsNumber' in record.field, 'decimalAsNumber not found in record'

    decimal_as_string = record.field['decimalAsString']
    decimal_as_number = record.field['decimalAsNumber']
    # assert the type
    assert decimal_as_string.type == 'DECIMAL', f'Expected Decimal, but got {decimal_as_string.type}'
    assert decimal_as_number.type == 'DECIMAL', f'Expected Decimal, but got {decimal_as_number.type}'
    # value
    assert decimal_as_string.value == Decimal(decimal_val), 'Not matching value for decimal'
    assert decimal_as_number.value == Decimal(decimal_val), 'Not matching value for decimal'
    # that headers exist
    assert 'scale' in decimal_as_string.attributes, 'Scale not found in headers for decimal_as_string'
    assert 'precision' in decimal_as_string.attributes, 'Precision not found in headers for decimal_as_string'
    assert 'scale' in decimal_as_number.attributes, 'Scale not found in headers for decimal_as_number'
    assert 'precision' in decimal_as_number.attributes, 'Precision not found in headers for decimal_as_number'
    # and their value
    assert decimal_as_string.attributes[
               'scale'] == '4', f'Got scale {decimal_as_string.attributes["scale"]}, expected 4'
    assert decimal_as_string.attributes[
               'precision'] == '8', f'Got precision {decimal_as_string.attributes["precision"]}, expected 8'
    assert decimal_as_number.attributes[
               'scale'] == '4', f'Got scale {decimal_as_number.attributes["scale"]}, expected 4'
    assert decimal_as_number.attributes[
               'precision'] == '8', f'Got precision {decimal_as_number.attributes["precision"]}, expected 8'


# SDC-11561: File Type Converter doesn't work properly with null in MAP and LIST types
@sdc_min_version('3.9.0')  # For the JavaScript processor use
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
