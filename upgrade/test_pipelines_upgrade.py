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
"""The tests in this module are used to test pipeline upgrades. Assumption is that there is only one SDC version
provided for running the upgrade against.
"""

import json
import logging
from os.path import dirname
from pathlib import Path
from uuid import uuid4

import pytest
from streamsets.sdk import sdc_models
from streamsets.sdk.utils import pipeline_json_encoder

from streamsets.testframework.markers import upgrade

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DIR_TO_READ = Path(f'{dirname(__file__)}/pipelines')


@pytest.fixture(scope='module')
# sdc_executor_hook cannot be used instead as the tests in this module are designed to run against one given SDC
# version and providing 2 would skip sdc_executor_hook, hence usage of sdc_builder_hook.
def sdc_builder_hook(args):
    def hook(data_collector):
        if not args.run_sdc_upgrade_tests:
            logger.info(f'Configuring SDC for pipeline JSON files of {DIR_TO_READ} ...')
            for pipeline_file in DIR_TO_READ.glob('**/*.json'):
                logger.debug(f'Configuring SDC for pipeline {pipeline_file}')
                pipeline = sdc_models.Pipeline(pipeline_file)
                data_collector.configure_for_pipeline(pipeline)
        else:
            pytest.skip('Test only runs for one given SDC version.')
    return hook


@upgrade
def test_pipeline_upgrade(sdc_builder):
    """Test pipeline upgrades by importing JSON files against a given SDC version and asserting that they
    have no pipeline import issues."""
    issue_pipelines = []
    logger.info(f'Importing and checking pipeline JSON files from {DIR_TO_READ} ...')
    for pipeline_file in DIR_TO_READ.glob('**/*.json'):
        uuid = str(uuid4())
        print(f'File is {pipeline_file}')
        logger.info(f'Using pipeline id {uuid} for file {pipeline_file}')
        pipeline = sdc_models.Pipeline(pipeline_file)
        pipeline.id = uuid
        sdc_builder.add_pipeline(pipeline)
        issues = sdc_builder.api_client.export_pipeline(pipeline.id)['pipelineConfig']['issues']
        if issues['issueCount']:
            issue_pipelines.append(pipeline_file)
            print(f'\nFor pipeline {pipeline_file} the issue(s) are ...')
            print(json.dumps(issues, indent=4, default=pipeline_json_encoder))

    assert not issue_pipelines, f'Number of pipelines having import issues: {len(issue_pipelines)}'
