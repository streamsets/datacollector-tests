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

import glob
import json
import logging
from os.path import dirname, join, realpath
from pathlib import Path
from uuid import uuid4

import pytest
from streamsets.sdk import sdc_models
from streamsets.sdk.utils import pipeline_json_encoder
from streamsets.testframework.markers import upgrade

logger = logging.getLogger(__name__)

DIR_TO_READ = Path(f'{dirname(__file__)}/pipelines')

ONLY_JDK8_PIPELINES = [
    'sdc_1.1.0_pipeline_Dev_Data_Trash.json',
    'sdc_1.1.0_pipeline_HadoopFS_trash.json',
    'sdc_1.1.0_pipeline_Kafka_consumer_trash.json',
    'sdc_1.1.0_pipeline_Omniture.json',
    'sdc_1.1.0_pipeline_Source_Destination.json',
    'sdc_1.6.0.0_pipeline_DevData_Trash.json',
    'sdc_1.6.0.0_pipeline_HadoopFS_Tarsh.json',
    'sdc_1.6.0.0_pipeline_Source_Destination.json',
    'sdc_1.6.0.0_pipeline_Source_Proccesors.json',
    'sdc_2.0.0.0_pipeline_Dev_Data_Trash.json',
    'sdc_2.0.0.0_pipeline_HadoopFS_trash.json',
    'sdc_2.0.0.0_pipeline_source_destination.json',
    'sdc_2.1.0.0_pipeline_HadoopFS_Trash.json',
    'sdc_2.1.0.0_pipeline_Source_destination.json',
    'sdc_2.1.0.0_pipeline_Source_processor.json',
    'sdc_2.2.0.0_pipeline_Event_executor.json',
    'sdc_2.2.0.0_pipeline_HadoopFS_Trash.json',
    'sdc_2.2.0.0_pipeline_KafkaConsumer_Trash.json',
    'sdc_2.2.0.0_pipeline_KinesisConsumer_Trash.json',
    'sdc_2.2.0.0_pipeline_Orgin_Processor.json',
    'sdc_2.2.0.0_pipeline_Origin_Destination.json',
    'sdc_2.2.0.0_pipeline_RedisConsumer_Trash.json'

]


@pytest.fixture(scope='module')
# sdc_executor_hook cannot be used instead as the tests in this module are designed to run against one given SDC
# version and providing 2 would skip sdc_executor_hook, hence usage of sdc_builder_hook.
def sdc_builder_hook(args):
    def hook(data_collector):
        if not args.run_sdc_upgrade_tests:
            logger.info(f'Configuring SDC for pipeline JSON files of {DIR_TO_READ} ...')
            for pipeline_file in DIR_TO_READ.glob('**/*.json'):
                logger.debug(f'Configuring SDC for pipeline {pipeline_file}')
                with open(pipeline_file) as f:
                    exported_pipeline = json.loads(f.read())
                pipeline = sdc_models.Pipeline(exported_pipeline)
                data_collector.configure_for_pipeline(pipeline)
        else:
            pytest.skip('Test only runs for one given SDC version.')
    return hook


@upgrade
def test_pipeline_upgrade(sdc_builder, pipeline_full_path):
    """Test pipeline upgrades by importing JSON files against a given SDC version and asserting that they
    have no pipeline import issues."""
    
    # There is a set of pipelines that can not be loaded in SDC when executing over JDK17 because are using
    # stages that are disabled. The JDK version was included in the health report in the SDC 5.5.0. For previous 
    # versions there is no way to know what java version is being used so defaulting to True.
    sdc_host_info = sdc_builder.api_client.get_health_report('HealthHostInformation').response.json()
    is_jdk8 = sdc_host_info['hostInformation']['jdkVersion'] == 8 if 'hostInformation' in sdc_host_info else True

    if not is_jdk8 and pipeline_full_path.split('/')[-1] in ONLY_JDK8_PIPELINES:
        pytest.skip(f'Skippingg upgrade tests for pipeline {pipeline_full_path}. It is not supported in java 17')
    
    logger.info('Importing and checking pipeline JSON file %s', pipeline_full_path)
    uuid = str(uuid4())
    logger.debug('Using pipeline id %s for file %s', uuid, pipeline_full_path)
    with open(pipeline_full_path) as f:
        exported_pipeline = json.loads(f.read())
    pipeline = sdc_models.Pipeline(exported_pipeline)
    pipeline.id = uuid
    sdc_builder.add_pipeline(pipeline)
    issues = sdc_builder.api_client.export_pipeline(pipeline.id)['pipelineConfig']['issues']
    if issues['issueCount']:
        pytest.fail(json.dumps(issues, indent=4, default=pipeline_json_encoder))


# pytest_generate_tests helps to parametrize pipelines which we will read from disk.
# More about pytest parametrization at http://doc.pytest.org/en/latest/parametrize.html
def pytest_generate_tests(metafunc):
    pipelines = glob.iglob(join(dirname(realpath(__file__)), DIR_TO_READ, '**', '*.json'),
                           recursive=True)
    metafunc.parametrize('pipeline_full_path', pipelines)
