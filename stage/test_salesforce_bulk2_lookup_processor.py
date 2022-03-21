# Copyright 2021 StreamSets Inc.
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

import copy
import logging

import pytest
from streamsets.testframework.markers import salesforce, sdc_min_version

from .utils.utils_salesforce import (insert_data_and_verify_using_wiretap, get_dev_raw_data_source,
                                     set_up_random, TEST_DATA)

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def _set_up_random(salesforce):
    set_up_random(salesforce)


@salesforce
@sdc_min_version('5.0.0')
@pytest.mark.parametrize('data_with_from_email', [False, True])  # Testing of SDC-7548
@pytest.mark.parametrize('query_with_time', [True, False])  # Testing of SDC-10207
def test_salesforce_lookup_processor(sdc_builder, sdc_executor, salesforce, data_with_from_email, query_with_time):
    """Simple Salesforce Lookup processor test.
    Pipeline will enrich records with the 'FirstName' of contacts by adding a field as 'surName'.

    The pipeline looks like:
        dev_raw_data_source >> salesforce_lookup >> wiretap

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
        data_with_from_email (:obj:`list`): Dataset to use in test
        query_with_time (:obj:`bool`): Whether or not to filter results by time
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Parametrizing directly the global variables does not work
    if data_with_from_email:
        TEST_DATA['DATA_TO_INSERT'] = TEST_DATA['DATA_WITH_FROM_IN_EMAIL']
    else:
        TEST_DATA['DATA_TO_INSERT'] = TEST_DATA['DATA_TO_INSERT']

    lookup_data = ['Email'] + [row['Email'] for row in TEST_DATA['DATA_TO_INSERT']]
    dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, lookup_data)

    salesforce_lookup = pipeline_builder.add_stage('Salesforce Bulk API 2.0 Lookup')
    # Changing " with ' and vice versa in following string makes the query execution fail.
    query_str = ("SELECT Id, FirstName, LastName, LeadSource FROM Contact "
                 "WHERE Email = '${record:value(\"/Email\")}' AND "
                 f"Lastname = '{TEST_DATA['STR_15_RANDOM']}'")

    if query_with_time:
        # Testing of SDC-10207 - select records that changed before an hour from now -
        # i.e. all of them! We're just checking here that the time functions are supported
        query_str += (" AND LastModifiedDate < ${time:extractStringFromDate("
                      "     time:millisecondsToDateTime(time:dateTimeToMilliseconds("
                      "         time:now()) + 3600000), \"yyyy-MM-dd'T'HH:mm:ss.SSSXXX\")}")

    field_mappings = [dict(dataType='USE_SALESFORCE_TYPE',
                           salesforceField='LastName',
                           sdcField='/surName')]
    salesforce_lookup.set_attributes(soql_query=query_str,
                                     field_mappings=field_mappings)

    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> salesforce_lookup >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    LOOKUP_EXPECTED_DATA = copy.deepcopy(TEST_DATA['DATA_TO_INSERT'])
    for record in LOOKUP_EXPECTED_DATA:
        record['surName'] = record.pop('LastName')
    insert_data_and_verify_using_wiretap(sdc_executor, pipeline, wiretap, LOOKUP_EXPECTED_DATA,
                                         salesforce, TEST_DATA['DATA_TO_INSERT'])