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

"""The tests in this module follow a pattern of creating pipelines with
:py:obj:`testframework.sdc_models.PipelineBuilder` in one version of SDC and then importing and running them in
another.
"""

import logging
import os
import time
from string import ascii_letters

from testframework.markers import salesforce

logger = logging.getLogger(__name__)

CSV_DATA_TO_INSERT = ['firstName,lastName,email',
                      'Test1,User1,xtest1@example.com',
                      'Test2,User2,xtest2@example.com',
                      'Test3,User3,xtest3@example.com']

DATA_TO_INSERT = [{'FirstName': 'Test1', 'LastName': 'User1', 'Email': 'xtest1@example.com'},
                  {'FirstName': 'Test2', 'LastName': 'User2', 'Email': 'xtest2@example.com'},
                  {'FirstName': 'Test3', 'LastName': 'User3', 'Email': 'xtest3@example.com'}]


@salesforce
def test_salesforce_destination(sdc_builder, sdc_executor, salesforce):
    """
    Send text to Salesforce destination from Dev Raw Data Source and
    confirm that Salesforce destination successfully reads them using Salesforce client.

    The pipeline looks like:
        dev_raw_data_source >> salesforce_destination
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(CSV_DATA_TO_INSERT))

    salesforce_destination = pipeline_builder.add_stage('Salesforce', type='destination')
    field_mapping = [{'sdcField': '/firstName', 'salesforceField': 'FirstName'},
                     {'sdcField': '/lastName', 'salesforceField': 'LastName'},
                     {'sdcField': '/email', 'salesforceField': 'Email'}]
    salesforce_destination.set_attributes(default_operation='INSERT',
                                          field_mapping=field_mapping,
                                          sobject_type='Contact')

    dev_raw_data_source >> salesforce_destination

    pipeline = pipeline_builder.build(title='Salesforce destination').configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)
    read_ids = None

    try:
        # Produce Salesforce records using pipeline.
        logger.info('Starting Salesforce destination pipeline and waiting for it to produce records...')
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(1)

        sdc_executor.stop_pipeline(pipeline).wait_for_stopped()

        # Using Salesforce connection, read the contents in the Salesforce destination.
        client = salesforce.client
        # Changing " with ' and vice versa in following string makes the query execution fail.
        query_str = "SELECT Id, FirstName, LastName, Email FROM Contact WHERE Email LIKE 'xtest%' ORDER BY Id"
        result = client.query(query_str)

        read_data = [f'{item["FirstName"]},{item["LastName"]},{item["Email"]}'
                     for item in result['records']]
        # Following is used later to delete these records.
        read_ids = [{'Id': item['Id']} for item in result['records']]

        assert CSV_DATA_TO_INSERT[1:] == read_data

    finally:
        if client is not None and read_ids is not None:
            logger.info('Deleting records ...')
            client.bulk.Contact.delete(read_ids)

@salesforce
def test_salesforce_origin(sdc_builder, sdc_executor, salesforce):
    """
    Create data using Salesforce client
    and then check if Salesforce origin receives them using snapshot.

    The pipeline looks like:
        salesforce_origin >> trash
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
    # Changing " with ' and vice versa in following string makes the query execution fail.
    query_str = ("SELECT Id, FirstName, LastName, Email FROM Contact "
                 "WHERE Id > '000000000000000' AND "
                 "Email LIKE 'xtest%' "
                 "ORDER BY Id")
    salesforce_origin.set_attributes(soql_query=query_str,
                                     subscribe_for_notifications=False)

    trash = pipeline_builder.add_stage('Trash')
    salesforce_origin >> trash
    pipeline = pipeline_builder.build(title='Salesforce Origin').configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    client = salesforce.client
    inserted_ids = None

    try:
        # Using Salesforce client, create  rows in Contact
        logger.info('Creating rows using Salesforce client...')
        client.bulk.Contact.insert(DATA_TO_INSERT)

        # Start pipeline and verify correct rows are received using snaphot.
        logger.info('Starting pipeline and snapshot')
        snapshot_command = sdc_executor.capture_snapshot(pipeline, start_pipeline=True)
        sdc_executor.get_status_pipeline(pipeline).wait_for_status('RUNNING')
        snapshot = snapshot_command.wait_for_finished().snapshot
        rows_from_snapshot = [record.value['value']
                              for record in snapshot[salesforce_origin].output]

        inserted_ids = [dict([(rec['sqpath'].strip('/'), rec['value'])
                             for rec in item if rec['sqpath'] == '/Id'])
                        for item in rows_from_snapshot]

        data_from_snapshot = [dict([(rec['sqpath'].strip('/'), rec['value'])
                                   for rec in item if rec['sqpath'] != '/Id'])
                              for item in rows_from_snapshot]

        assert data_from_snapshot == DATA_TO_INSERT

    finally:
        if client is not None and inserted_ids is not None:
            logger.info('Deleting records...')
            client.bulk.Contact.delete(inserted_ids)
