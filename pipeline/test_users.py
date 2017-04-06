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
from os.path import dirname, join

import pytest

from testframework import sdc, sdc_models

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@pytest.fixture(scope='module')
def dc(args, pipeline):
    dc = sdc.DataCollector(version=args.sdc_version)
    dc.add_user("jarcec", roles=["admin"], groups=["jarcec", "employee"])
    dc.add_user("dima", roles=["admin"], groups=["dima", "employee"])
    dc.add_user("bryan", roles=["manager", "creator"], groups=["bryan", "contractor"])
    dc.add_user("arvind", roles=["guest"], groups=["arvind", "guests"])
    # The way our framework works, this pipeline will always be created as user "admin"
    dc.add_pipeline(pipeline)
    dc.start()
    yield dc
    dc.tear_down()

@pytest.fixture(scope='module')
def pipeline():
    pipeline = sdc_models.Pipeline(join(dirname(__file__),
                                   'pipelines',
                                   'random_expression_trash.json'))
    yield pipeline

# Validate "current" user switching and getting the proper groups and roles.
def test_current_user(dc):
    dc.set_user("admin")
    user = dc.current_user()
    assert user.name == "admin"

    dc.set_user("jarcec")
    user = dc.current_user()
    assert user.name == "jarcec"
    assert user.groups == ["all", "jarcec", "employee"]
    assert user.roles == ["admin"]

# Ensure that the operations are indeed executed by the current user.
def test_pipeline_history(dc, pipeline):
    dc.set_user("jarcec")
    dc.start_pipeline(pipeline).wait_for_status('RUNNING')

    dc.set_user("dima")
    dc.stop_pipeline(pipeline).wait_for_stopped()

    history = dc.pipeline_history(pipeline)

    # History is in descending order.

    entry = history.entries[0]
    assert entry["user"] == "dima"
    assert entry["status"] == "STOPPED"

    entry = history.entries[1]
    assert entry["user"] == "dima"
    assert entry["status"] == "STOPPING"

    entry = history.entries[2]
    assert entry["user"] == "jarcec"
    assert entry["status"] == "RUNNING"

    entry = history.entries[3]
    assert entry["user"] == "jarcec"
    assert entry["status"] == "STARTING"

    entry = history.entries[4]
    assert entry["user"] == "admin"
    assert entry["status"] == "EDITED"

