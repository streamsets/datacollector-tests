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

# A module providing utils for working with SDC activation

import logging
import os
import re
import requests
import time

from bs4 import BeautifulSoup

from streamsets.testframework.utils import parse_email

logger = logging.getLogger(__name__)

REGISTRATION_ENDPOINT = 'https://test.registration.streamsets.com/register'
ACTIVATION_SUPPORT_SDC_MIN_VERSION = '3.15.0'


def register_sdc(sdc):
    # use email alias to be able to find the correct one
    email_id = os.environ['SDC_ACTIVATION_TEST_EMAIL_ID'].replace('@', f'+{sdc.id}@')
    data = {
        "activationUrl": REGISTRATION_ENDPOINT,
        "company": "Streamsets",
        "country": "USA",
        "email": email_id,
        "firstName": "STF",
        "id": sdc.id,
        "lastName": "SDC",
        "role": "Data Engineer",
        "type": "DATA_COLLECTOR",
        "version": sdc.version
    }
    registration_result_command = sdc.api_client.register(REGISTRATION_ENDPOINT, data)
    assert registration_result_command.response.status_code == requests.codes.no_content
    logger.info(f'SDC [{sdc.id}] is successfully registered.')


def register_and_activate_sdc(sdc):
    # use email alias to be able to find the correct one
    email_id = os.environ['SDC_ACTIVATION_TEST_EMAIL_ID'].replace('@', f'+{sdc.id}@')
    email_password = os.environ['SDC_ACTIVATION_TEST_EMAIL_PASSWORD']

    register_sdc(sdc)

    # Wait for some time to land the email in inbox
    time.sleep(20)

    # parse_email to get activation_key
    parsed_email = parse_email(email_id, email_password)
    email_soup = BeautifulSoup(parsed_email['body'], 'lxml')
    activation_key = email_soup.find(string=re.compile('^--------SDC ACTIVATION KEY--------'))
    logger.debug('activation_key')
    logger.debug(activation_key)

    # activate SDC
    sdc.api_client.activate(activation_key)
