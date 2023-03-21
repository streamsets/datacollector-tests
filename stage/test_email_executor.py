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

import email
import imaplib
import logging
import pytest as pytest
import string
import traceback

from streamsets.sdk.sdc_api import RunError
from streamsets.testframework.utils import get_random_string, wait_for_condition
from streamsets.testframework.markers import sdc_min_version, smtp

logger = logging.getLogger(__name__)

SMTP_SERVER = "smtp.gmail.com"


@pytest.fixture(scope='module')
def sdc_common_hook(smtp):
    def hook(data_collector):
        data_collector.sdc_properties['mail.smtp.auth'] = 'true'
        data_collector.sdc_properties['mail.smtp.host'] = 'smtp.gmail.com'
        data_collector.sdc_properties['mail.smtp.port'] = '25'
        data_collector.sdc_properties['mail.smtp.ssl.protocols'] = 'TLSv1.2'
        data_collector.sdc_properties['mail.smtp.starttls.enable'] = 'true'
        data_collector.sdc_properties['mail.transport.protocol'] = 'smtp'
        data_collector.sdc_properties['xmail.from.address'] = smtp['mail']
        data_collector.sdc_properties['xmail.password'] = smtp['password_code']
        data_collector.sdc_properties['xmail.username'] = smtp['mail']
    return hook


@smtp
@sdc_min_version('5.4.0')
@pytest.mark.parametrize(
    'email_subject',
    [get_random_string(string.ascii_letters, 10), '']
)
@pytest.mark.parametrize(
    'email_body',
    ['Hello World!!', None]
)
def test_email_executor(sdc_builder, sdc_executor, smtp, email_subject, email_body):
    """
    Tests that emails are properly sent with the Email Executor.
    """

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='TEXT',
        raw_data='Test Data',
        stop_after_first_batch=True
    )

    email_executor = pipeline_builder.add_stage('Email')
    email_executor.set_attributes(
        email_configuration=[
            {
                "email": [
                    smtp['mail']
                ],
                "condition": "true",
                "subject": email_subject,
                "body": email_body
            }
        ]
    )

    dev_raw_data_source >> email_executor

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    logger.debug("Pipeline finished, waiting for response")

    received_email = read_and_delete_email(smtp, email_subject)

    assert received_email['subject'] == email_subject
    assert received_email.get_payload() == '' if email_body is None else email_body + '\r\n'


@smtp
@sdc_min_version('5.4.0')
def test_email_executor_invalid_email_id(sdc_builder, sdc_executor):
    """
    Tests that mails are not sent with invalid email ID.
    """

    email_subject = get_random_string(string.ascii_letters, 10)
    email_body = 'Hello World!!'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='TEXT',
        raw_data='Test Data',
        stop_after_first_batch=True
    )

    email_executor = pipeline_builder.add_stage('Email')
    email_executor.set_attributes(
        email_configuration=[
            {
                "email": [
                    ""
                ],
                "condition": "true",
                "subject": email_subject,
                "body": email_body
            }
        ]
    )

    dev_raw_data_source >> email_executor

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
    except RunError:
        pass
    else:
        assert False, "An error should have been arisen as the email ID is invalid"


def read_and_delete_email(smtp, subject=None):
    try:
        mail = imaplib.IMAP4_SSL(SMTP_SERVER)
        mail.login(
            smtp['mail'],
            smtp['password_code']
        )
        mail.select('inbox')

        if subject is '':
            email_id = int(mail.search(None, 'ALL')[1][0].split()[-1])
        else:
            email_id = int(mail.search(None, f'SUBJECT "{subject}"')[1][0].split()[-1])

        data = mail.fetch(str(email_id), '(RFC822)')
        for response_part in data:
            if isinstance(response_part[0], tuple):
                mail.store(str(email_id).encode(), '+FLAGS', '\\Deleted')
                return email.message_from_string(str(response_part[0][1], 'utf-8'))

    except Exception as e:
        traceback.print_exc()
        print(str(e))
