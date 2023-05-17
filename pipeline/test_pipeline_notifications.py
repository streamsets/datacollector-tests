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

import logging
import imaplib
import email
import traceback
from string import ascii_letters
from time import sleep

import pytest as pytest
from streamsets.sdk.sdc_api import RunError, RunningError
from streamsets.sdk.utils import get_random_string
from streamsets.testframework.markers import sdc_min_version, smtp

pytestmark = smtp

logger = logging.getLogger(__name__)

SMTP_SERVER = "smtp.gmail.com"

PIPELINE_NOTIFICATION_EMAIL_SUBJECT = 'StreamSets Data Collector Alert - '
FULL_ERROR_INFORMATION_EMAIL_SUBJECT = \
    'StreamSets Data Collector Alert - ERROR: ' \
    'com.streamsets.pipeline.api.base.OnRecordErrorException: ' \
    'PIPELINE_FINISHER_001 - Unsatisfied stage condition: The record is not an event'
NO_ERROR_DETAILS_EMAIL_SUBJECT = 'StreamSets Data Collector Alert - ERROR: PIPELINE_FINISHER_001'
NO_ERROR_INFORMATION_EMAIL_SUBJECT = 'StreamSets Data Collector Alert - ERROR'

RUN_ERROR_STATE = 'RUN_ERROR'
RUN_ERROR_STATE_SUBJECT_ENDING = ' - ERROR'

STOPPED_STATE = 'STOPPED'
STOPPED_STATE_SUBJECT_ENDING = ' - STOPPED'
STOPPED_STATE_EMAIL_KEY_PHRASE = 'was stopped'

FINISHED_STATE = 'FINISHED'
FINISHED_STATE_SUBJECT_ENDING = ' - FINISHED'
FINISHED_STATE_EMAIL_KEY_PHRASE = 'finished executing'

RUNNING_STATE = 'RUNNING'
RUNNING_STATE_SUBJECT_ENDING = ' - RUNNING'
RUNNING_STATE_EMAIL_KEY_PHRASE = 'started executing'

ERROR_CODE = 'PIPELINE_FINISHER_001'
ERROR_MESSAGE = 'Unsatisfied stage condition: The record is not an event'


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


@sdc_min_version('5.5.0')
@pytest.mark.parametrize(
    'error_information_level, email_subject',
    [
        ['FULL_ERROR_INFORMATION', FULL_ERROR_INFORMATION_EMAIL_SUBJECT],
        ['NO_ERROR_DETAILS', NO_ERROR_DETAILS_EMAIL_SUBJECT],
        ['NO_ERROR_INFORMATION', NO_ERROR_INFORMATION_EMAIL_SUBJECT]
    ]
)
def test_pipeline_error_email_alerts(
        sdc_builder,
        sdc_executor,
        smtp,
        error_information_level,
        email_subject
):
    """
    Tests that mails are properly sent with Pipeline Alerts.

    Please refer to the following confluence pages for information on how to run this test:
    https://streamsets.atlassian.net/wiki/spaces/DC/pages/2823652604/How+to+set+up+an+SDC+to+send+emails
    https://streamsets.atlassian.net/wiki/spaces/EP/pages/319094897/SCH+-+EP+Specifics#SCH-EPSpecifics-SubscriptionVerificationService
    """

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='TEXT',
        raw_data='Oh! Bello papaguena! Tu le Bella comme le papaya.',
        stop_after_first_batch=True
    )

    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    pipeline_finisher.set_attributes(
        react_to_events=True,
        event_type='no-more-data',
        on_record_error='STOP_PIPELINE'
    )

    dev_raw_data_source >> pipeline_finisher

    pipeline = pipeline_builder.build()
    pipeline._data.get('pipelineRules')['configuration'] = [
        {'name': 'emailIDs', 'value': [sdc_executor.sdc_configuration.get('xmail.username')]},
        {'name': 'errorInformationLevel', 'value': error_information_level},
        {'name': 'webhookConfigs', 'value': []}
    ]
    pipeline_title = f"test_pipeline_error_email_alerts[{error_information_level}]_"\
                     + get_random_string(ascii_letters, 5)
    pipeline._data.get('pipelineConfig')['title'] = pipeline_title

    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert False, "An error should have been arisen as the record received is not an event"
    except (RunningError, RunError):
        pass

    logger.debug("Pipeline finished, waiting for response")

    received_email = read_and_delete_email(smtp, email_subject)
    assert received_email is not None, "The email has not been found"

    email_body = received_email.get_payload()
    if error_information_level == 'FULL_ERROR_INFORMATION':
        assert ERROR_CODE in email_body
        assert ERROR_MESSAGE in email_body
    elif error_information_level == 'NO_ERROR_DETAILS':
        assert ERROR_CODE in email_body
        assert ERROR_MESSAGE not in email_body
    elif error_information_level == 'NO_ERROR_INFORMATION':
        assert ERROR_CODE not in email_body
        assert ERROR_MESSAGE not in email_body


@sdc_min_version('5.5.0')
@pytest.mark.parametrize(
    'error_information_level',
    ['FULL_ERROR_INFORMATION', 'NO_ERROR_DETAILS', 'NO_ERROR_INFORMATION']
)
@pytest.mark.parametrize('pipeline_state', [RUNNING_STATE, FINISHED_STATE, STOPPED_STATE])
def test_pipeline_notifications(
        sdc_builder,
        sdc_executor,
        smtp,
        error_information_level,
        pipeline_state
):
    """
    Tests that mails are properly sent with Pipeline Notifications.

    Please refer to the following confluence pages for information on how to run this test:
    https://streamsets.atlassian.net/wiki/spaces/DC/pages/2823652604/How+to+set+up+an+SDC+to+send+emails
    https://streamsets.atlassian.net/wiki/spaces/EP/pages/319094897/SCH+-+EP+Specifics#SCH-EPSpecifics-SubscriptionVerificationService
    """

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='TEXT',
        raw_data='Oh! Bello papaguena! Tu le Bella comme le papaya.',
        stop_after_first_batch=(pipeline_state != STOPPED_STATE)
    )

    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    pipeline_finisher.set_attributes(
        react_to_events=(pipeline_state == STOPPED_STATE),
        event_type='no-more-data',
        on_record_error='DISCARD'
    )

    dev_raw_data_source >> pipeline_finisher

    pipeline = pipeline_builder.build()
    pipeline_title = f"test_pipeline_notifications[{pipeline_state}][{error_information_level}]_"\
                     + get_random_string(ascii_letters, 5)
    pipeline._data.get('pipelineConfig')['title'] = pipeline_title
    pipeline_config = pipeline._data.get('pipelineConfig')['configuration']

    email_set = False
    error_information_level_set = False
    for item in pipeline_config:
        if item['name'] == 'emailIDs':
            item['value'] = [sdc_executor.sdc_configuration.get('xmail.username')]
            email_set = True
        elif item['name'] == 'errorInformationLevel':
            item['value'] = error_information_level
            error_information_level_set = True
        elif item['name'] == 'notifyOnStates':
            item['value'] = [pipeline_state]

        if email_set and error_information_level_set:
            break

    sdc_executor.add_pipeline(pipeline)
    execution = sdc_executor.start_pipeline(pipeline)

    if pipeline_state == STOPPED_STATE:
        sdc_executor.stop_pipeline(pipeline)
    else:
        execution.wait_for_finished()

    logger.debug("Pipeline finished, waiting for response")

    expected_email_subject = create_email_notification_subject(pipeline_state, pipeline_title)
    received_email = read_and_delete_email(smtp, expected_email_subject)

    assert received_email is not None, "The email has not been found"
    assert received_email['subject'].replace('\r\n', '') == expected_email_subject
    assert get_state_email_notification_key_phrase(pipeline_state) in received_email.get_payload()


@sdc_min_version('5.5.0')
@pytest.mark.parametrize(
    'error_information_level',
    ['FULL_ERROR_INFORMATION', 'NO_ERROR_DETAILS', 'NO_ERROR_INFORMATION']
)
def test_pipeline_error_notifications(
        sdc_builder,
        sdc_executor,
        smtp,
        error_information_level
):
    """
    Tests that mails are properly sent with Pipeline Notifications.

    Please refer to the following confluence pages for information on how to run this test:
    https://streamsets.atlassian.net/wiki/spaces/DC/pages/2823652604/How+to+set+up+an+SDC+to+send+emails
    https://streamsets.atlassian.net/wiki/spaces/EP/pages/319094897/SCH+-+EP+Specifics#SCH-EPSpecifics-SubscriptionVerificationService
    """

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='TEXT',
        raw_data='Oh! Bello papaguena! Tu le Bella comme le papaya.',
        stop_after_first_batch=True
    )

    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    pipeline_finisher.set_attributes(
        react_to_events=True,
        event_type='no-more-data',
        on_record_error='STOP_PIPELINE'
    )

    dev_raw_data_source >> pipeline_finisher

    pipeline = pipeline_builder.build()
    pipeline_title = f"test_pipeline_error_notifications[{error_information_level}]_"\
                     + get_random_string(ascii_letters, 5)
    pipeline._data.get('pipelineConfig')['title'] = pipeline_title
    pipeline_config = pipeline._data.get('pipelineConfig')['configuration']

    email_set = False
    error_information_level_set = False
    for item in pipeline_config:
        if item['name'] == 'emailIDs':
            item['value'] = [sdc_executor.sdc_configuration.get('xmail.username')]
            email_set = True
        elif item['name'] == 'errorInformationLevel':
            item['value'] = error_information_level
            error_information_level_set = True
        elif item['name'] == 'notifyOnStates':
            item['value'] = [RUN_ERROR_STATE]

        if email_set and error_information_level_set:
            break

    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert False, "An error should have been arisen as the record received is not an event"
    except (RunningError, RunError):
        pass

    logger.debug("Pipeline finished, waiting for response")

    expected_email_subject = create_email_notification_subject(RUN_ERROR_STATE, pipeline_title)
    received_email = read_and_delete_email(smtp, expected_email_subject)

    assert received_email is not None, "The email has not been found"
    assert received_email['subject'].replace('\r\n', '') == expected_email_subject

    email_body = received_email.get_payload()
    if error_information_level == 'FULL_ERROR_INFORMATION':
        assert ERROR_CODE in email_body
        assert ERROR_MESSAGE in email_body
    elif error_information_level == 'NO_ERROR_DETAILS':
        assert ERROR_CODE in email_body
        assert ERROR_MESSAGE not in email_body
    elif error_information_level == 'NO_ERROR_INFORMATION':
        assert ERROR_CODE not in email_body
        assert ERROR_MESSAGE not in email_body


def create_email_notification_subject(pipeline_state, pipeline_title):
    subject = PIPELINE_NOTIFICATION_EMAIL_SUBJECT + pipeline_title

    if pipeline_state == RUN_ERROR_STATE:
        subject += RUN_ERROR_STATE_SUBJECT_ENDING
    elif pipeline_state == STOPPED_STATE:
        subject += STOPPED_STATE_SUBJECT_ENDING
    elif pipeline_state == FINISHED_STATE:
        subject += FINISHED_STATE_SUBJECT_ENDING
    elif pipeline_state == RUNNING_STATE:
        subject += RUNNING_STATE_SUBJECT_ENDING

    return subject


def get_state_email_notification_key_phrase(pipeline_state):
    key_phrase = None

    if pipeline_state == RUN_ERROR_STATE:
        key_phrase = STOPPED_STATE_EMAIL_KEY_PHRASE
    elif pipeline_state == STOPPED_STATE:
        key_phrase = STOPPED_STATE_EMAIL_KEY_PHRASE
    elif pipeline_state == FINISHED_STATE:
        key_phrase = FINISHED_STATE_EMAIL_KEY_PHRASE
    elif pipeline_state == RUNNING_STATE:
        key_phrase = RUNNING_STATE_EMAIL_KEY_PHRASE

    return key_phrase


def read_and_delete_email(smtp, subject=None):
    try:
        # Allow the email time to be sent and received
        sleep(10)

        mail = imaplib.IMAP4_SSL(SMTP_SERVER)
        mail.login(
            smtp['mail'],
            smtp['password_code']
        )
        mail.select('inbox')

        if subject is None:
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
