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

"""The tests in this module follow a pattern of creating pipelines with
:py:obj:`testframework.sdc_models.PipelineBuilder` in one version of SDC and then importing and running them in
another.
"""

import logging

import test_apache
from testframework.markers import solr

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@solr
def test_solr_target(sdc_builder, sdc_executor, solr):
    """Test Solr target pipeline in Apache Solr (6.x) environment. The assumption is that standalone Solr
    has the collection/core pre-created and is passed part of Solr URI.
    """
    test_apache.basic_solr_target('apache', sdc_builder, sdc_executor, solr)
