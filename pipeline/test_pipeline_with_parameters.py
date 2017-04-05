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

import json
import logging
from os.path import dirname, join

import pytest

from testframework import sdc, sdc_models

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@pytest.fixture(scope='module')
def dc(args, pipeline):
    dc = sdc.DataCollector(version=args.sdc_version)
    dc.add_pipeline(pipeline)
    dc.start()
    logger.debug(dc.server_host)
    yield dc
    # After all tests or after an exception is encountered, stop and remove the SDC instance
    dc.tear_down()


@pytest.fixture(scope='module')
def pipeline():
    pipeline = sdc_models.Pipeline(join(dirname(__file__),
                                        'pipelines',
                                        'pipeline_with_parameters.json'))
    yield pipeline


def test_pipeline_start_with_parameters(dc, pipeline):
    runtime_parameters = {'fields': 'x', 'fromField': '/x', 'toField': '/changedField'}
    dc.start_pipeline(pipeline, runtime_parameters).wait_for_status(status='RUNNING', timeout_sec=300)

    # Verify running pipeline's status
    pipeline_status = dc.api_client.get_pipeline_status(pipeline.name).response.json()
    status = pipeline_status.get('status')
    assert status == 'RUNNING'

    attributes = pipeline_status.get('attributes')
    assert attributes is not None
    assert attributes.get('RUNTIME_PARAMETERS') is not None
    assert attributes.get('RUNTIME_PARAMETERS').get('toField') is not None
    assert attributes.get('RUNTIME_PARAMETERS').get('toField') == '/changedField'

    # Stop the pipeline and verify pipeline's status
    dc.stop_pipeline(pipeline).wait_for_stopped()
    current_status = dc.api_client.get_pipeline_status(pipeline.name).response.json().get('status')
    assert current_status == 'STOPPED'

