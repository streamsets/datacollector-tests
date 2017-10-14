# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

import pytest

from testframework import sdc

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@pytest.fixture(scope='module')
def data_collector():
    data_collector = sdc.DataCollector()
    data_collector.add_user('jarcec', roles=['admin'], groups=['jarcec', 'employee'])
    data_collector.add_user('dima', roles=['admin'], groups=['dima', 'employee'])
    data_collector.add_user('bryan', roles=['manager', 'creator'], groups=['bryan', 'contractor'])
    data_collector.add_user('arvind', roles=['guest'], groups=['arvind', 'guests'])
    data_collector.start()
    yield data_collector
    if data_collector.tear_down_on_exit:
        data_collector.tear_down()


@pytest.fixture(scope='module')
def pipeline(data_collector):
    builder = data_collector.get_pipeline_builder()

    dev_data_generator = builder.add_stage('Dev Data Generator')
    trash = builder.add_stage('Trash')

    dev_data_generator >> trash

    pipeline = builder.build()
    data_collector.set_user('admin')
    data_collector.add_pipeline(pipeline)

    yield pipeline


# Validate "current" user switching and getting the proper groups and roles.
def test_current_user(data_collector):
    data_collector.set_user('admin')
    user = data_collector.current_user()
    assert user.name == 'admin'

    data_collector.set_user('jarcec')
    user = data_collector.current_user()
    assert user.name == 'jarcec'
    assert user.groups == ['all', 'jarcec', 'employee']
    assert user.roles == ['admin']


# Ensure that the operations are indeed executed by the current user.
def test_pipeline_history(data_collector, pipeline):
    data_collector.set_user('jarcec')
    data_collector.start_pipeline(pipeline)

    data_collector.set_user('dima')
    data_collector.stop_pipeline(pipeline)

    history = data_collector.pipeline_history(pipeline)

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
