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

import json
import logging
import pytest

from streamsets.testframework.markers import cluster, sdc_min_version
from streamsets.testframework.sdc import DataCollector
from streamsets.testframework.utils import parse_multi_versions, wait_for_condition

# Skip all tests in this module if --sdc-version < 3.1.0.0
pytestmark = sdc_min_version('3.1.0.0-SNAPSHOT')

logger = logging.getLogger(__name__)

# Set of properties that needs to be enabled to perform the validation as we need to
ENABLE_CLASSPATH_VALIDATION = {
    # Make sure that the validation is enabled
    'stagelibs.classpath.validation.enable': 'true',
    # But never die, this test will validate result over REST
    'stagelibs.classpath.validation.terminate': 'false'
}

# List of stage libraries that have their own tests rather then the generic one
EXCLUDE_LIBS = {
    # Disable all MapR by default
    'streamsets-datacollector-mapr_5_1-lib',
    'streamsets-datacollector-mapr_5_2-lib',
    'streamsets-datacollector-mapr_6_0-lib',
    'streamsets-datacollector-mapr_6_1-lib',
    'streamsets-datacollector-mapr_6_0-mep4-lib',
    'streamsets-datacollector-mapr_6_0-mep5-lib',
    'streamsets-datacollector-mapr_6_1-mep6-lib',
    'streamsets-datacollector-mapr_spark_2_1_mep_3_0-lib'
}


def pytest_generate_tests(metafunc):
    versions = parse_multi_versions(metafunc.config.getoption('sdc_version'))
    if versions.pre_upgrade_version != versions.post_upgrade_version:
        pytest.skip('Classpath validation tests are not run as upgrade tests')
    # We do this logic to handle automation-friendly cases like `stf test --sdc-version '3.13.0 > 3.13.0'`,
    # which result versions.pre_upgrade_version being set.
    version = versions.pre_upgrade_version or versions.version

    # To generate the list of test cases, we temporarily start a Data Collector instance and query its
    # stageLibraries/list endpoint for all stage libraries (excluding legacy or enterprise libs).
    if 'stagelib' in metafunc.fixturenames:
        with DataCollector(version=version, tear_down_on_exit=True) as data_collector:
            data_collector.start()
            # We need to use wait_for_condition as it was observed that calling the stageLibraries/list method
            # immediately after Data Collector starts sometimes results in an empty list being returned.
            # By saving the result into the config instance, we cache the list of stage libraries for when we
            # need to add them to the SDC instance during start.
            def stage_libs_loaded(data_collector, config):
                config.all_stage_libs = sorted([stage_library.id for stage_library in data_collector.stage_libraries
                                                if not stage_library._data['legacy']
                                                and stage_library._repository_manifest['repoLabel'] != 'enterprise'
                                                and stage_library.id not in EXCLUDE_LIBS])
                return config.all_stage_libs
            wait_for_condition(stage_libs_loaded, [data_collector, metafunc.config])

        metafunc.parametrize('stagelib', metafunc.config.all_stage_libs)


@pytest.fixture(scope='module')
def sdc_common_hook(request):
    def hook(data_collector):
        data_collector.add_stage_lib(*request.config.all_stage_libs)
        data_collector.sdc_properties.update(ENABLE_CLASSPATH_VALIDATION)
    return hook


def test_classpath(sdc_executor, stagelib):
    # Validate that
    if sdc_executor.server_url:
        pytest.skip('This test is only applicable to Docker-based SDC test.')

    # Validate that we can get classpath health report from the rest
    result_list = sdc_executor.api_client.get_classpath_health()
    assert result_list

    # And that we have report for our current stage
    result_stage = [r for r in result_list if r['name'] == stagelib]
    assert result_stage

    result_stage = result_stage[0]
    logger.info('Health report: %s', json.dumps(result_stage, indent=4))
    assert not result_stage['unparseablePaths']
    assert not result_stage['versionCollisions']
    assert result_stage['valid'] == True


@pytest.mark.skip(reason="See explanation in SDC-10319.")
@cluster('mapr')
def test_mapr_classpath(sdc_executor, cluster):
    for stagelib in cluster.sdc_stage_libs:
        test_classpath(sdc_executor, stagelib)
