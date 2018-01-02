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
import textwrap

from testframework.markers import rpmpackaging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

SDC_LIBS_FOLDER = '/opt/streamsets-datacollector/streamsets-libs'

pytestmark = [rpmpackaging]


def test_rpm_related_directories(sdc_builder):
    """Tests if expected folders for RPM are created with expected groups and owners.
    Test uses Jython Evaluator processor. This avoids the need to have SSH access to SDC instances.
    The dev_raw_data_cource contains records with expected folders.
    The main processing script checks for folder existence. Then checks for group, owners are done and
    accordingly results are added as new attributes in record. These are later used for verification.

    The pipeline would look like:
        dev_raw_data_source >> jython_evaluator >> trash
    """
    # Generate data for folder names
    expected_records = [dict(dir_name='/etc/sdc', group='root', owner='root'),
                        dict(dir_name='/var/log/sdc', group='sdc', owner='sdc'),
                        dict(dir_name='/var/lib/sdc', group='sdc', owner='sdc'),
                        dict(dir_name='/var/lib/sdc-resources', group='sdc', owner='sdc'),
                        dict(dir_name=SDC_LIBS_FOLDER, group='root', owner='root')]
    raw_data = json.dumps([{'dir_name': rec['dir_name']} for rec in expected_records])

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       json_content='ARRAY_OBJECTS',
                                       raw_data=raw_data)
    jython_evaluator = pipeline_builder.add_stage('Jython Evaluator', type='processor')
    # In the script for every record processing, check if folder exists and
    # if exists, get its owner and group; and create record attributes for group and owner with respective values
    script = """
        import grp
        import pwd
        import os

        for record in records:
            try:
                cur_dir_name = record.value['dir_name']
                if os.path.isdir(cur_dir_name):
                    stat_info = os.stat(cur_dir_name)
                    record.value['owner'] = pwd.getpwuid(stat_info.st_uid)[0]
                    record.value['group'] = grp.getgrgid(stat_info.st_gid)[0]
                output.write(record)
            except Exception as e:
                error.write(record, str(e))
    """
    # textwrap.dedent helps to strip leading whitespaces for valid Python indentation
    jython_evaluator.set_attributes(record_processing_mode='BATCH',
                                    script=textwrap.dedent(script))
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> jython_evaluator >> trash
    pipeline = pipeline_builder.build('Folders check pipeline')
    sdc_builder.add_pipeline(pipeline)

    snapshot = sdc_builder.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_builder.stop_pipeline(pipeline)

    output_records = snapshot[jython_evaluator.instance_name].output  # is a list of output records
    actual_records = [record.value2 for record in output_records]
    assert expected_records == actual_records


def test_default_stagelibs_exist(sdc_builder):
    """When SDC is installed, certain stage libs are installed be default.
    This test verifies if expected default stage libs are present.
    Test uses Jython Evaluator processor. This avoids the need to have SSH access to SDC instances.
    The dev_raw_data_cource contains records with expected stage libs.
    In the main processing script, checks are done for existence of stage libs specified in record and
    accordingly record is added to output if stage lib exists. These are later used for verification.

    The pipeline would look like:
        dev_raw_data_source >> jython_evaluator >> trash
    """
    stage_lib_names = ['streamsets-datacollector-basic-lib',
                       'streamsets-datacollector-dev-lib',
                       'streamsets-datacollector-stats-lib',
                       'streamsets-datacollector-windows-lib']
    expected_records = [{'stage_lib_name': f'{SDC_LIBS_FOLDER}/{item}'} for item in stage_lib_names]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       json_content='ARRAY_OBJECTS',
                                       raw_data=json.dumps(expected_records))
    jython_evaluator = pipeline_builder.add_stage('Jython Evaluator', type='processor')
    # In the script for every record processing, check if stage lib specified in record exists and
    # if yes, add the record to output.
    script = """
        import os

        for record in records:
            try:
                if os.path.isdir(record.value['stage_lib_name']):
                    output.write(record)
            except Exception as e:
                error.write(record, str(e))
    """
    # textwrap.dedent helps to strip leading whitespaces for valid Python indentation
    jython_evaluator.set_attributes(record_processing_mode='BATCH',
                                    script=textwrap.dedent(script))
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> jython_evaluator >> trash
    pipeline = pipeline_builder.build('Stage libs check pipeline')
    sdc_builder.add_pipeline(pipeline)

    snapshot = sdc_builder.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_builder.stop_pipeline(pipeline)

    output_records = snapshot[jython_evaluator.instance_name].output  # is a list of output records
    actual_records = [record.value2 for record in output_records]
    assert expected_records == actual_records
