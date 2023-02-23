# Copyright 2020 StreamSets Inc.
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
import pytest

from streamsets.testframework.markers import sdc_min_version


# Skip all tests in this module if --sdc-version < 3.20.0
pytestmark = sdc_min_version('3.20.0')

logger = logging.getLogger(__name__)


def test_inspector_list(sdc_executor):
    """Validate that all expected inspectors are available."""
    inspectors = sdc_executor.api_client.get_health_categories().response.json()
    assert len(inspectors) == 4

    # Convert the list to names only so that we can assert things are there
    names = [i['className'] for i in inspectors]

    # Assert that we have all the expected inspectors regardless of the reported order
    assert 'ConfigurationHealthCategory' in names
    assert 'JvmInstanceHealthCategory' in names
    assert 'MachineHealthCategory' in names
    assert 'NetworkHealthCategory' in names


@pytest.mark.parametrize('entry_name', [
    'Max Batch Size for Preview',
    'Max Batch Size',
    'Max Error Records Per Stage',
    'Max Pipeline Errors',
    'Max Log Tailers',
    'Max Private ClassLoader',
    'Max Runner Size',
    'Max Pipeline Runner Size',
])
def test_configuration_category(sdc_executor, entry_name):
    """All configuration checks should be green by default - no point in shipping configuration that is read/yellow."""
    report = sdc_executor.api_client.get_health_report('ConfigurationHealthCategory').response.json()
    assert len(report['categories']) == 1

    result = report['categories'][0]
    assert result is not None

    check = _find_health_check(result, entry_name)
    assert check is not None
    assert check['severity'] == 'GREEN'
    assert check['value'] is not None
    assert check['description'] is not None
    assert check['details'] is None


@pytest.mark.parametrize('entry_name,severity,details', [
    ('Thread count','GREEN', False),
    ('Deadlocked threads', 'GREEN', False),
    ('JVM Memory Max', 'RED', False),
    ('JVM Memory Utilization', 'GREEN', False),
    ('System Memory Max', 'GREEN', False),
    ('System Memory Utilization', 'GREEN', False),
    ('Child Processes', 'GREEN', True)
])
def test_jvm_instance_category(sdc_executor, entry_name, severity, details):
    report = sdc_executor.api_client.get_health_report('JvmInstanceHealthCategory').response.json()
    assert len(report['categories']) == 1

    result = report['categories'][0]
    assert result is not None

    check = _find_health_check(result, entry_name)
    assert check is not None
    assert check['severity'] == severity
    assert check['value'] is not None
    assert check['description'] is not None
    if details:
        assert check['details'] is not None
    else:
        assert check['details'] is None


@pytest.mark.parametrize('entry_name,severity,details', [
    ('Data Dir Available Space', 'GREEN', False),
    ('Runtime Dir Available Space', 'GREEN', False),
    ('Log Dir Available Space', 'GREEN', False),
    ('File Descriptors', 'GREEN', False),
    ('SDC User Processes', 'RED', True),
])
def test_machine_category(sdc_executor, entry_name, severity, details):
    report = sdc_executor.api_client.get_health_report('MachineHealthCategory').response.json()
    assert len(report['categories']) == 1

    result = report['categories'][0]
    assert result is not None

    check = _find_health_check(result, entry_name)
    # Check alwas must exists
    assert check is not None
    # Severity always must be present, but we will check it only if specified in the parametrized
    assert check['severity'] is not None
    if severity:
        assert check['severity'] == severity
    # And we're expecting a value only if we know result of the severity
    if severity:
        assert check['value'] is not None
    else:
        assert check['value'] is None
    # Description must exists in any case
    assert check['description'] is not None
    # Details are again conditional on the check itself
    if details:
        assert check['details'] is not None
    else:
        assert check['details'] is None


@sdc_min_version('5.4.0')
@pytest.mark.parametrize('entry_name,description', [
    ('Ping', 'Ping to www.streamsets.com'),
    ('Traceroute', 'Traceroute to www.streamsets.com'),
    ('Ping', 'Ping to Control Hub'),
    ('Traceroute', 'Traceroute to Control Hub'),
])
def test_network_inspector(sdc_executor, entry_name, description):
    report = sdc_executor.api_client.get_health_report('NetworkHealthCategory').response.json()
    assert len(report['categories']) == 1

    result = report['categories'][0]
    assert result is not None

    check = _find_health_check_by_desc(result, description)
    assert check is not None
    assert check['name'] is not None
    if entry_name:
            assert check['name'] == entry_name
    assert check['severity'] is not None
    assert check['value'] is None
    assert check['details'] is not None

@sdc_min_version('5.5.0')
def test_health_host_information(sdc_executor):
    report = sdc_executor.api_client.get_health_report('HealthHostInformation').response.json()

    host_information = report['hostInformation']
    assert host_information is not None

    assert 'operativeSystemDistribution' in host_information, 'operativeSystemDistribution not in hostInformation'
    assert host_information['operativeSystemDistribution'] is not None

    assert 'operativeSystemVersion' in host_information, 'operativeSystemVersion not in hostInformation'
    assert host_information['operativeSystemVersion'] is not None

    assert 'jdkVersion' in host_information, 'jdkVersion not in hostInformation'
    assert host_information['jdkVersion'] is not None

    assert 'timeZone' in host_information, 'jdkVersion not in hostInformation'
    assert host_information['timeZone'] is not None

    assert 'offset' in host_information, 'offset not in hostInformation'
    assert host_information['offset'] is not None

    assert 'uptimeInformation' in host_information, 'uptimeInformation not in hostInformation'
    assert host_information['uptimeInformation'] is not None



def _find_health_check(result, name):
    for check in result['healthChecks']:
        if check['name'] == name:
            return check

    return None

def _find_health_check_by_desc(result, description):
    for check in result['healthChecks']:
        if check['description'] == description:
            return check

    return None