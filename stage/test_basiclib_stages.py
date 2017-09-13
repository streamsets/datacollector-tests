# Copyright 2017 StreamSets Inc.
#
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
import string

import pytest

from testframework import sdc
from testframework.utils import get_random_string
from testframework.sdc_models import Configuration

logger = logging.getLogger(__name__)

#
# Shell executor
#

@pytest.fixture(scope='module')
def sdc_shell(args):
    sdc_shell = sdc.DataCollector(version=args.pre_upgrade_sdc_version or args.sdc_version)
    # Blocked by TEST-128
    #sdc_shell.sdc_properties['stage.conf_com.streamsets.pipeline.stage.executor.shell.impersonation_mode'] = 'current_user'

    sdc_shell.start()

    yield sdc_shell

    if sdc_shell.tear_down_on_exit:
        sdc_shell.tear_down()


@pytest.fixture(scope='module')
def pipeline_shell_generator(sdc_shell):
    builder = sdc_shell.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = '{}'

    shell_executor = builder.add_stage('Shell')
    shell_executor.environment = Configuration(property_key='key', file='${FILE}')
    shell_executor.script = 'echo `whoami` > $file'

    dev_raw_data_source >> shell_executor

    executor_pipeline = builder.build()
    executor_pipeline.add_parameters(FILE='/')
    sdc_shell.add_pipeline(executor_pipeline)

    yield executor_pipeline


@pytest.fixture(scope='module')
def pipeline_shell_read(sdc_shell):
    builder = sdc_shell.get_pipeline_builder()

    file_source = builder.add_stage('File Tail')
    file_source.data_format = 'TEXT'
    file_source.file_to_tail = [
        dict(fileRollMode='REVERSE_COUNTER', patternForToken='.*', fileFullPath='${FILE}')
    ]

    trash1 = builder.add_stage('Trash')
    trash2 = builder.add_stage('Trash')

    file_source >> trash1
    file_source >> trash2

    read_pipeline = builder.build()
    read_pipeline.add_parameters(FILE='/')
    sdc_shell.add_pipeline(read_pipeline)

    yield read_pipeline


def test_shell_executor_impersonation(sdc_shell, pipeline_shell_generator, pipeline_shell_read):
    """Test proper impersonation on the Shell executor side.
       This is a dual pipeline test to test the executor side effect."""

    # Use this file to exchange data between the executor and our test
    runtime_parameters = {'FILE': "/tmp/{}".format(get_random_string(string.ascii_letters, 30))}

    # Run the pipeline with executor exactly once
    sdc_shell.start_pipeline(pipeline_shell_generator,
                             runtime_parameters=runtime_parameters).wait_for_pipeline_batch_count(1)
    sdc_shell.stop_pipeline(pipeline_shell_generator)

    # And retrieve its output
    snapshot = sdc_shell.capture_snapshot(pipeline=pipeline_shell_read,
                                          runtime_parameters=runtime_parameters,
                                          start_pipeline=True).wait_for_finished().snapshot
    sdc_shell.stop_pipeline(pipeline_shell_read)


    records = snapshot[pipeline_shell_read.origin_stage].output_lanes[pipeline_shell_read.origin_stage.output_lanes[0]]
    assert len(records) == 1
    # Blocked by TEST-128, should be different user
    assert records[0].value['value']['text']['value'] == 'sdc'


