# Copyright 2024 StreamSets Inc.
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
import pytest
from streamsets.testframework.markers import sdc_min_version, jira

from stage.utils.common import cleanup
from stage.utils.utils_migration import LegacyHandler as PipelineHandler
from datetime import datetime

import logging
import json

RELEASE_VERSION = "5.11.0"

logger = logging.getLogger(__name__)
pytestmark = [sdc_min_version(RELEASE_VERSION)]

def create_jira_issue(jira, testname):
    jira_issue = jira.create_issue({
                              "fields": {
                                 "project":
                                 {
                                    "key": "GTMS"
                                 },
                                 "summary": f'{testname} created Jira Issue',
                                 "description": "Creating issue using the Jira REST API in integration test.",
                                 "issuetype": {
                                    "name": "Task"
                                 }
                             }
                          })

    return jira_issue

