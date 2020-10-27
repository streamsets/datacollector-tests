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
    inspectors = sdc_executor.get_health_inspectors()
    assert len(inspectors) == 4

    # Convert the list to names only so that we can assert things are there
    names = [i.class_name for i in inspectors]

    # Assert that we have all the expected inspectors regardless of the reported order
    assert 'ConfigurationInspector' in names
    assert 'JvmInstanceInspector' in names
    assert 'MachineInspector' in names
    assert 'NetworkInspector' in names


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
def test_configuration_inspector(sdc_executor, entry_name):
    """All configuration checks should be green by default - no point in shipping configuration that is read/yellow."""
    report = sdc_executor.run_health_inspectors('ConfigurationInspector')
    assert len(report.results) == 1

    result = report.result_for('ConfigurationInspector')
    assert result is not None

    entry = result.entry_for(entry_name)
    assert entry is not None
    assert entry.severity == 'GREEN'
    assert entry.value is not None
    assert entry.description is not None
    assert entry.details is None


@pytest.mark.parametrize('entry_name,severity,details', [
    ('Thread count','GREEN', False),
    ('Deadlocked threads', 'GREEN', False),
    ('JVM Memory Max', 'RED', False),
    ('JVM Memory Utilization', 'GREEN', False),
    ('System Memory Max', 'GREEN', False),
    ('System Memory Utilization', 'GREEN', False),
    ('Child Processes', 'GREEN', True)
])
def test_jvm_instance_inspector(sdc_executor, entry_name, severity, details):
    report = sdc_executor.run_health_inspectors('JvmInstanceInspector')
    assert len(report.results) == 1

    result = report.result_for('JvmInstanceInspector')
    assert result is not None

    entry = result.entry_for(entry_name)
    assert entry is not None
    assert entry.severity == severity
    assert entry.value is not None
    assert entry.description is not None
    if details:
        assert entry.details is not None
    else:
        assert entry.details is None


@pytest.mark.parametrize('entry_name,details', [
    ('Data Dir Available Space', False),
    ('Runtime Dir Available Space', False),
    ('Log Dir Available Space', False),
    ('File Descriptors', False),
    ('SDC User Processes', True),
])
def test_machine_inspector(sdc_executor, entry_name, details):
    report = sdc_executor.run_health_inspectors('MachineInspector')
    assert len(report.results) == 1

    result = report.result_for('MachineInspector')
    assert result is not None

    entry = result.entry_for(entry_name)
    assert entry is not None
    assert entry.severity == 'GREEN'
    assert entry.value is not None
    assert entry.description is not None
    if details:
        assert entry.details is not None
    else:
        assert entry.details is None


@pytest.mark.parametrize('entry_name', [
    'Ping',
    'Traceroute',
])
def test_network_inspector(sdc_executor, entry_name):
    report = sdc_executor.run_health_inspectors('NetworkInspector')
    assert len(report.results) == 1

    result = report.result_for('NetworkInspector')
    assert result is not None

    entry = result.entry_for(entry_name)
    assert entry is not None
    assert entry.severity == 'GREEN'
    assert entry.value is None
    assert entry.description is not None
    assert entry.details is not None