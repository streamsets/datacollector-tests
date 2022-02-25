# Copyright 2020 StreamSets Inc.
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
import string

import pytest
from streamsets.testframework.markers import salesforce, sdc_min_version
from streamsets.testframework.utils import get_random_string

from ..utils.utils_salesforce import (BULK_PIPELINE_TIMEOUT_SECONDS, clean_up,
                                      check_ids, get_ids, DATA_TYPES, set_field_permissions,
                                      STANDARD_FIELDS, compare_values)

logger = logging.getLogger(__name__)


@salesforce
@sdc_min_version('4.5.0')
@pytest.mark.parametrize('type_data', DATA_TYPES, ids=[datatype['metadata']['type'] for datatype in DATA_TYPES])
def test_data_types(sdc_builder, sdc_executor, salesforce, type_data):
    object_name = get_random_string(string.ascii_lowercase, 20)

    client = salesforce.client
    mdapi = client.mdapi

    record_ids = []

    try:
        custom_object = mdapi.CustomObject(
            fullName=f'{object_name}__c',
            label=f'{object_name}',
            pluralLabel=f'{object_name}',
            nameField=mdapi.CustomField(
                label='Name',
                type=mdapi.FieldType('Text')
            ),
            fields=[mdapi.CustomField(
                # This syntax combines the standard fields we
                # need for every type of custom field with the
                # fields specific to this data type
                **{**STANDARD_FIELDS, **type_data['metadata']}
            )],
            deploymentStatus=mdapi.DeploymentStatus('Deployed'),
            sharingModel=mdapi.SharingModel('Read')
        )
        logger.info(f'Creating object {object_name} in Salesforce ...')
        mdapi.CustomObject.create(custom_object)

        set_field_permissions(mdapi, object_name, f'{STANDARD_FIELDS["label"]}')

        logger.info('Adding two records into Salesforce ...')
        record = {
            'Name': '1',
        }
        # Not every data type wants data - e.g. auto number field
        if type_data.get('data_to_insert'):
            if type_data.get('compound_field'):
                record.update(type_data['data_to_insert'])
            else:
                record[f'{STANDARD_FIELDS["fullName"]}'] = type_data['data_to_insert']

        object_type = getattr(client, f'{object_name}__c')
        result = object_type.create(record)
        record_ids.append({'Id': result['id']})

        # And a record without a value for the field
        result = object_type.create({
            'Name': '2'
        })
        record_ids.append({'Id': result['id']})

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('Dev Raw Data Source')
        origin.data_format = 'JSON'
        origin.raw_data = '{"id": 1}\n{"id": 2}'
        origin.stop_after_first_batch = True

        lookup = builder.add_stage('Salesforce Bulk API 2.0 Lookup')
        fields = ",".join(type_data['expected_value'].keys()) \
            if type_data.get('compound_field') else f"{STANDARD_FIELDS['fullName']}"
        lookup.soql_query = (f"SELECT {fields} FROM {object_name}__c "
                             "WHERE Name = '${record:value(\"/id\")}'")
        lookup.field_mappings = [dict(dataType='USE_SALESFORCE_TYPE',
                                      salesforceField=f'{STANDARD_FIELDS["fullName"]}',
                                      sdcField=f'/{STANDARD_FIELDS["label"]}')]

        wiretap = builder.add_wiretap()

        origin >> lookup >> wiretap.destination

        pipeline = builder.build().configure_for_environment(salesforce)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=BULK_PIPELINE_TIMEOUT_SECONDS)

        assert len(wiretap.output_records) == 2
        record = wiretap.output_records[0]
        null_record = wiretap.output_records[1]

        if type_data.get('compound_field'):
            for key in type_data['expected_value'].keys():
                assert type_data['expected_type'] == record.field[key].type
            for key in type_data['expected_value'].keys():
                assert type_data['expected_value'][key] == record.field[key]._data['value']

            for key in type_data['null_value'].keys():
                assert type_data['expected_type'] == null_record.field[key].type
            for key in type_data['null_value'].keys():
                assert type_data['null_value'][key] == null_record.field[key]._data['value']
        else:
            assert type_data['expected_type'] == record.field[f'{STANDARD_FIELDS["label"]}'].type
            assert compare_values(type_data['expected_value'],
                                  record.field[f'{STANDARD_FIELDS["label"]}']._data['value'],
                                  type_data['metadata']['type'])

            assert type_data['expected_type'] == null_record.field[f'{STANDARD_FIELDS["label"]}'].type
            assert compare_values(type_data.get('null_value'),
                                  null_record.field[f'{STANDARD_FIELDS["label"]}']._data['value'],
                                  type_data['metadata']['type'])
    finally:
        try:
            clean_up(sdc_executor, pipeline, client, record_ids, f'{object_name}__c')
        finally:
            # mdapi.CustomObject.create() doesn't return a value, so we don't
            # have a reliable way to know if the object was created or not.
            # Just try to delete it and ignore any errors.
            try:
                mdapi.CustomObject.delete(f'{object_name}__c')
            except:
                pass


@salesforce
@sdc_min_version('4.5.0')
def test_object_names(sdc_builder, sdc_executor, salesforce):
    pytest.skip("The Salesforce Bulk 2.0 Lookup Processor doesn't generate queries - it only takes user "
                "input, thus user is responsible to properly escape or enclose names and therefore there "
                "is not much for us to test here.")


@salesforce
@sdc_min_version('4.5.0')
def test_multiple_batches(sdc_builder, sdc_executor, salesforce):
    # TODO - RAISE BACK TO 1000!
    batch_size = 40
    batches = 50
    object_name = get_random_string(string.ascii_lowercase, 20)

    client = salesforce.client
    mdapi = client.mdapi

    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Dev Data Generator')
    origin.batch_size = batch_size
    origin.fields_to_generate = [{
        "type": "LONG_SEQUENCE",
        "field": "seq"
    }]

    expression = builder.add_stage('Expression Evaluator')
    expression.field_expressions = [ {
        'fieldToSet': '/lookup',
        'expression': '${record:value("/seq") % 3 + 1}'
      } ]

    lookup = builder.add_stage('Salesforce Bulk API 2.0 Lookup')
    lookup.soql_query = (f"SELECT {STANDARD_FIELDS['fullName']} FROM {object_name}__c "
                         "WHERE Name = '${record:value(\"/lookup\")}'")
    lookup.field_mappings = [dict(dataType='USE_SALESFORCE_TYPE',
                                  salesforceField=f'{STANDARD_FIELDS["fullName"]}',
                                  sdcField=f'/{STANDARD_FIELDS["label"]}')]

    wiretap = builder.add_wiretap()
    origin >> expression >> lookup >> wiretap.destination

    pipeline = builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    record_ids = []
    try:
        custom_object = mdapi.CustomObject(
            fullName=f'{object_name}__c',
            label=f'{object_name}',
            pluralLabel=f'{object_name}',
            nameField=mdapi.CustomField(
                label='Name',
                type=mdapi.FieldType('Text')
            ),
            fields=[mdapi.CustomField(
                label=f'{STANDARD_FIELDS["label"]}',
                fullName=f'{STANDARD_FIELDS["fullName"]}',
                required=False,
                type='Number',
                precision=5,
                scale=0
            )],
            deploymentStatus=mdapi.DeploymentStatus('Deployed'),
            sharingModel=mdapi.SharingModel('Read')
        )
        logger.info(f'Creating object {object_name} in Salesforce ...')
        mdapi.CustomObject.create(custom_object)
        bulk_object_type = getattr(client.bulk, f'{object_name}__c')

        set_field_permissions(mdapi, object_name, f'{STANDARD_FIELDS["label"]}')

        logger.info(f'Inserting data into {object_name} ...')
        records = [{'Name': str(n), f'{STANDARD_FIELDS["fullName"]}': n * 10} for n in range(1, 4)]
        record_ids = check_ids(get_ids(bulk_object_type.insert(records), 'id'))

        # Wiretap generates one extra record per batch
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count((batches + 1) * batch_size)
        sdc_executor.stop_pipeline(pipeline)

        # Now the pipeline will write some amount of records that will be larger, so we get precise count from metrics
        history = sdc_executor.get_pipeline_history(pipeline)
        record_count = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count
        logger.info(f"Detected {record_count} output records")
        # Sanity check
        assert record_count >= batch_size * batches

        records = wiretap.output_records
        assert len(records) == record_count

        # Verify each record
        def sortFunc(entry):
            return entry.field['seq'].value

        records.sort(key=sortFunc)

        expected_number = 0
        for record in records:
            assert record.field['seq'] == expected_number
            assert record.field['lookup'] == expected_number % 3 + 1
            assert record.field[f'{STANDARD_FIELDS["label"]}'] == (expected_number % 3 + 1) * 10
            expected_number += 1

    finally:
        try:
            clean_up(sdc_executor, pipeline, client, record_ids, f'{object_name}__c')
        finally:
            # mdapi.CustomObject.create() doesn't return a value, so we don't
            # have a reliable way to know if the object was created or not.
            # Just try to delete it and ignore any errors.
            try:
                mdapi.CustomObject.delete(f'{object_name}__c')
            except:
                pass


@salesforce
@sdc_min_version('4.5.0')
def test_data_format(sdc_builder, sdc_executor, salesforce):
    pytest.skip("Salesforce Bulk 2.0 Lookup Processor doesn't deal with data formats")


@salesforce
@sdc_min_version('4.5.0')
def test_dataflow_events(sdc_builder, sdc_executor, salesforce):
    pytest.skip("Salesforce Bulk 2.0 Lookup processor does not support events today")
