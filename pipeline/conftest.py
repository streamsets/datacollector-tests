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

import logging
from collections import namedtuple

import pytest

logger = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def random_expression_pipeline_builder(sdc_builder):
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(delay_between_batches=10000,
                                      batch_size = 10)
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    wiretap = pipeline_builder.add_wiretap()

    dev_data_generator >> expression_evaluator >> wiretap.destination

    yield namedtuple('PipelineBuilder', ['pipeline_builder',
                                         'dev_data_generator',
                                         'expression_evaluator',
                                         'wiretap'])(pipeline_builder,
                                                     dev_data_generator,
                                                     expression_evaluator,
                                                     wiretap)
