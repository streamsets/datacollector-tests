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

# A module providing utils for working with Salesforce

import string
import logging
from streamsets.testframework.utils import get_random_string
from time import sleep

logger = logging.getLogger(__name__)

TIMEOUT = 300

TEST_DATA = {}

def set_up_random(salesforce):
    """" This function is used to generate unique set of values for each test.
    Every time this function is used, generates a unique RANDOM string to
    set up the values used in every test."""

    TEST_DATA['STR_15_RANDOM'] = get_random_string(string.ascii_letters,15)
    logger.info(f"STR_15_RANDOM : '{TEST_DATA['STR_15_RANDOM']}'")

    TEST_DATA['DATA_TO_INSERT'] = [
        {'FirstName': 'Test1', 'LastName': TEST_DATA['STR_15_RANDOM'], 'Email': 'xtest1@example.com',
         'LeadSource': 'Advertisement'},
        {'FirstName': 'Test2', 'LastName': TEST_DATA['STR_15_RANDOM'], 'Email': 'xtest2@example.com',
         'LeadSource': 'Partner'},
        {'FirstName': 'Test3', 'LastName': TEST_DATA['STR_15_RANDOM'], 'Email': 'xtest3@example.com',
         'LeadSource': 'Web'}]

    # For testing of SDC-7548
    # Since email is used in WHERE clause in lookup processory query,
    # create data containing 'from' word in emails to verify the bug is fixed.
    TEST_DATA['DATA_WITH_FROM_IN_EMAIL'] = [{'FirstName': 'Test1', 'LastName': TEST_DATA['STR_15_RANDOM'],
                                             'Email': 'FROMxtest1@example.com', 'LeadSource': 'Advertisement'},
                                            {'FirstName': 'Test2', 'LastName': TEST_DATA['STR_15_RANDOM'],
                                             'Email': 'xtefromst2@example.com', 'LeadSource': 'Partner'},
                                            {'FirstName': 'Test3', 'LastName': TEST_DATA['STR_15_RANDOM'],
                                             'Email': 'xtes3@example.comFROM', 'LeadSource': 'Web'}]
    TEST_DATA['CSV_DATA_TO_INSERT'] = [','.join(TEST_DATA['DATA_TO_INSERT'][0].keys())] + [','.join(item.values()) for
                                                                                           item in
                                                                                           TEST_DATA['DATA_TO_INSERT']]

    # When the server has too many queries it returns temporary unavailable
    # This is sleep is to avoid that error
    sleep(10)
    # Log to know if limits are reached
    client = salesforce.client
    limits = client.limits()
    log_limits = f'Limits: DailyApiRequests: {limits["DailyApiRequests"]["Remaining"]}, ' \
                 f'DailyBulkApiRequests: {limits["DailyBulkApiRequests"]["Remaining"]}, ' \
                 f'DailyDurableStreamingApiEvents: {limits["DailyDurableStreamingApiEvents"]["Remaining"]}, ' \
                 f'HourlyPublishedStandardVolumePlatformEvents: ' \
                 f'{limits["HourlyPublishedStandardVolumePlatformEvents"]["Remaining"]}, ' \
                 f'MonthlyPlatformEvents: {limits["MonthlyPlatformEvents"]["Remaining"]} '

    logger.info(log_limits)


def get_dev_raw_data_source(pipeline_builder, raw_data):
    """Utility method to create a Dev Raw Data Origin containing delimited data

    Args:
        pipeline_builder (:py:class:`streamsets.sdk.sdc_models.PipelineBuilder`): Pipeline builder instance
        raw_data (obj:`list`): Data as a list of comma-separated values, including a header row

    Returns:
        The Dev Raw Data origin as a :py:class:`streamsets.sdk.sdc_models.Stage`
    """
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(raw_data),
                                       stop_after_first_batch=True)
    return dev_raw_data_source


def verify_snapshot(snapshot, stage, expected_data, sort=True):
    """Utility method to verify that a snapshot matches the expected data

    Args:
        snapshot (:py:class:`streamsets.sdk.sdc_models.Snapshot`): Snapshot containing data to be verified
        stage (:py:class:`streamsets.sdk.sdc_models.Stage`): Stage after which data is to be verified
        expected_data (obj:`list`): Expected data as a list of dicts

    Returns:
        (:obj:`list`) of inserted record Ids in form [{'Id':'001000000000001'},...]
    """
    # SDC-10773 - source IDs must be unique
    source_ids = {record.header['sourceId'] for record in snapshot[stage].output}
    assert len(source_ids) == len(snapshot[stage].output)

    rows_from_snapshot = [record.field
                          for record in snapshot[stage].output]

    data_from_snapshot = [{field:record[field] for field in record if field not in ['Id', 'SystemModstamp']}
                          for record in rows_from_snapshot]

    if data_from_snapshot and sort:
        data_from_snapshot = sorted(data_from_snapshot,
                                    key=lambda k:k['FirstName' if 'FirstName' in data_from_snapshot[0] else 'surName'].value)

    if data_from_snapshot:
        assert data_from_snapshot == expected_data


def verify_by_snapshot(sdc_executor, pipeline, stage, expected_data, salesforce, data_to_insert, sort=True):
    """Utility method to insert data into Salesforce, start a pipeline, capture a snapshot, verify that the snapshot
    matches the expected data, and clean up the inserted records.

    Args:
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        pipeline (:py:class:`streamsets.sdk.sdc_models.Pipeline`): Pipeline instance
        stage (:py:class:`streamsets.sdk.sdc_models.Stage`): Stage after which data is to be verified
        expected_data (obj:`list`): Expected data as a list of dicts
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
        data_to_insert (obj:`list`): Data to be inserted, as a list of dicts

    Returns:
        (:obj:`list`) of inserted record Ids in form [{'Id':'001000000000001'},...]
    """
    client = salesforce.client
    inserted_ids = None
    try:
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        inserted_ids = get_ids(client.bulk.Contact.insert(data_to_insert), 'id')

        logger.info('Starting pipeline and snapshot')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, timeout_sec=TIMEOUT).snapshot

        verify_snapshot(snapshot, stage, expected_data, sort)

    finally:
        clean_up(sdc_executor, pipeline, client, inserted_ids)


def get_ids(records, key):
    """Utility method to extract list of Ids from Bulk API insert/query result.

    Args:
        records (:obj:`list`): List of records from a Bulk API insert or SOQL query.
        key (:obj:`str`): Key to extract - 'Id' for queries or 'id' for inserted data.

    Returns:
        (:obj:`list`) of inserted record Ids in form [{'Id':'001000000000001'},...]
    """
    return [{'Id': record[key]}
            for record in records]


def clean_up(sdc_executor, pipeline, client, contact_ids):
    """Utility method to delete inserted contacts and stop the pipeline

    Args:
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        pipeline (:py:class:`streamsets.sdk.sdc_models.Pipeline`): Pipeline instance to be stopped
        client (:py:class:`simple_salesforce.Salesforce`): Salesforce client
        contact_ids (:obj:`list`): List of contacts to be deleted in form [{'Id':'001000000000001'},...]
    """
    if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
        logger.info('Stopping pipeline')
        sdc_executor.stop_pipeline(pipeline)

    try:
        if contact_ids:
            logger.info('Deleting records ...')
            client.bulk.Contact.delete(contact_ids)
    except:
        logger.error('Unable to delete records...')
