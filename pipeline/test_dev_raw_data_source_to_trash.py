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
import requests
from time import sleep
from os.path import dirname, join

import pytest

from testframework import sdc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture(scope='module')
def dc(args, pipeline):
    dc = sdc.DataCollector(version=args.sdc_version)
    dc.add_pipeline(pipeline)
    dc.start()
    yield dc
    # After all tests or after an exception is encountered, stop and remove the SDC instance
    dc.tear_down()


@pytest.fixture(scope='module')
def pipeline():
    pipeline = sdc.Pipeline(join(dirname(__file__),
                                 'pipelines',
                                 'dev_raw_data_source_to_trash.json'))
    yield pipeline


def test_pipeline_status(dc, pipeline):
    dc.start_pipeline(pipeline).wait_for_status(status='RUNNING', timeout_sec=300)

    # Verify running pipeline's status
    current_status = dc.api_client.get_pipeline_status(pipeline.name).response.json().get('status')
    assert current_status == 'RUNNING'

    # Stop the pipeline and verify pipeline's status
    dc.stop_pipeline(pipeline).wait_for_stopped()
    current_status = dc.api_client.get_pipeline_status(pipeline.name).response.json().get('status')
    assert current_status == 'STOPPED'


def test_pipeline_definitions(dc, pipeline):
    dc.start_pipeline(pipeline).wait_for_status(status='RUNNING', timeout_sec=300)

    # Verify definitions
    running_pipeline_definitions = dc.api_client.get_definitions()
    assert running_pipeline_definitions is not None

    # Stop the pipeline and verify definitions do not change
    dc.stop_pipeline(pipeline).wait_for_stopped()
    stopped_pipeline_definitions = dc.api_client.get_definitions()
    assert stopped_pipeline_definitions is not None
    assert running_pipeline_definitions == stopped_pipeline_definitions


def test_pipeline_metrics(dc, pipeline):
    dc.start_pipeline(pipeline).wait_for_status(status='RUNNING', timeout_sec=300)

    # Verify pipeline metrics
    first_metrics_json = dc.api_client.get_pipeline_metrics(pipeline.name)
    assert first_metrics_json is not None
    sleep(15)
    second_metrics_json = dc.api_client.get_pipeline_metrics(pipeline.name)
    assert second_metrics_json is not None
    assert first_metrics_json != second_metrics_json

    # Stop the pipeline and verify stopped pipeline's metrics
    dc.stop_pipeline(pipeline).wait_for_stopped()
    assert dc.api_client.get_pipeline_metrics(pipeline.name) == {}


def test_invalid_execution_mode(dc, pipeline):
    # Set executionMode to invalid value and add that as a new pipeline
    pipeline.configuration['executionMode'] = 'Invalid_Execution_Mode'
    pipeline.name = 'Invalid_Execution_Mode Pipeline'
    dc.add_pipeline(pipeline)

    with pytest.raises(requests.exceptions.HTTPError):
        dc.start_pipeline(pipeline)
