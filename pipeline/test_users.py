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

import logging
import time

import pytest

from requests.models import HTTPError
from streamsets.sdk.exceptions import BadRequestError
from streamsets.testframework import sdc

logger = logging.getLogger(__name__)


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.add_user('jarcec', roles=['admin'], groups=['jarcec', 'employee'])
        data_collector.add_user('dima', roles=['admin'], groups=['dima', 'employee'])
        data_collector.add_user('bryan', roles=['manager', 'creator'], groups=['bryan', 'contractor'])
        data_collector.add_user('arvind', roles=['guest'], groups=['arvind', 'guests'])
        data_collector.add_user('santhosh', password='santhosh', roles=['manager'], groups=['manager'])
    return hook


@pytest.fixture(scope='module')
def pipeline(sdc_executor):
    builder = sdc_executor.get_pipeline_builder()

    dev_data_generator = builder.add_stage('Dev Data Generator')
    trash = builder.add_stage('Trash')

    dev_data_generator >> trash

    pipeline = builder.build()
    sdc_executor.set_user('admin')
    sdc_executor.add_pipeline(pipeline)

    yield pipeline


# Validate "current" user switching and getting the proper groups and roles.
def test_current_user(sdc_executor):
    sdc_executor.set_user('admin')
    user = sdc_executor.current_user
    assert user.name == 'admin'

    sdc_executor.set_user('jarcec')
    user = sdc_executor.current_user
    assert user.name == 'jarcec'
    assert user.groups == ['all', 'jarcec', 'employee']
    assert user.roles == ['admin']


# Validate change password
def test_change_password(sdc_executor):
    # Switch to user-santhosh with the password
    sdc_executor.set_user(username='santhosh', password='santhosh')
    user = sdc_executor.current_user
    assert user.name == 'santhosh'

    # Bad current password
    try:
        sdc_executor.change_password(old_password='santhosh-wrong-password', new_password='new-wrong-password')
        assert pytest.fail('Should not reach as the password is not right')
    except BadRequestError as b:
        assert 'Invalid old password' in str(b)

    try:
        # Change password with correct old and new password
        command = sdc_executor.change_password(old_password='santhosh', new_password='santhosh-new')
        assert 204 == command.response.status_code

        # Switch user
        sdc_executor.set_user('admin')
        user = sdc_executor.current_user
        assert user.name == 'admin'

        # Give couple of seconds before testing the change in password
        # to allow Jetty to hot reload the form-realm.properties file
        time.sleep(2)

        # Login with right password
        sdc_executor.set_user(username='santhosh', password='santhosh-new')
        user = sdc_executor.current_user
        assert user.name == 'santhosh'
        conf_command = sdc_executor.api_client.get_sdc_configuration()
        assert 200 == conf_command.response.status_code

        # Switch user
        sdc_executor.set_user('admin')
        user = sdc_executor.current_user
        assert user.name == 'admin'

        # Login with old(wrong) password
        sdc_executor.set_user(username='santhosh', password='santhosh')
        try:
            sdc_executor.api_client.get_sdc_configuration()
            pytest.fail('Should not reach as old password is specified')
        except HTTPError as h:
            assert 401 == h.response.status_code
    finally:
        try:
            sdc_executor.set_user(username='santhosh', password='santhosh-new')
            # Reset to old password
            command = sdc_executor.change_password(old_password='santhosh-new', new_password='santhosh')
            assert 204 == command.response.status_code
        finally:
            # Reset the current user
            sdc_executor.set_user('admin')
            user = sdc_executor.current_user
            assert user.name == 'admin'


# Ensure that the operations are indeed executed by the current user.
def test_pipeline_history(sdc_executor, pipeline):
    sdc_executor.set_user('jarcec')
    sdc_executor.start_pipeline(pipeline)

    sdc_executor.set_user('dima')
    sdc_executor.stop_pipeline(pipeline)

    history = sdc_executor.get_pipeline_history(pipeline)

    # History is in descending order.

    entry = history.entries[0]
    assert entry['user'] == 'dima'
    assert entry['status'] == 'STOPPED'

    entry = history.entries[1]
    assert entry['user'] == 'dima'
    assert entry['status'] == 'STOPPING'

    entry = history.entries[2]
    assert entry['user'] == 'jarcec'
    assert entry['status'] == 'RUNNING'

    entry = history.entries[3]
    assert entry['user'] == 'jarcec'
    assert entry['status'] == 'STARTING'

    entry = history.entries[4]
    assert entry['user'] == 'admin'
    assert entry['status'] == 'EDITED'
