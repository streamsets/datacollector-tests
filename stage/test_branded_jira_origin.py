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

from stage.utils.common import cleanup, test_name
from stage.utils.utils_migration import LegacyHandler as PipelineHandler
from datetime import datetime
from stage.utils.utils_jira import create_jira_issue

import logging
import json

RELEASE_VERSION = "5.11.0"
JIRA = "Jira (Preview)"
ORIGIN = "origin"
DEFAULT_TIMEOUT_IN_SEC = 300

logger = logging.getLogger(__name__)
pytestmark = [jira, sdc_min_version(RELEASE_VERSION)]

@pytest.mark.parametrize('include_comments', [False, True])
def test_branded_jira_origin_search_issue(sdc_builder, sdc_executor, cleanup, test_name, jira, include_comments):
    """
    Test for Jira Origin search issue action using jql query. First, create jira issue and then search
    for that issue using issue key.
    """
    #create jira issue
    issue = create_jira_issue(jira, test_name)

    #search jira issue using jql query
    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    jira_origin = pipeline_builder.add_stage(JIRA, type=ORIGIN)
    jira_origin.set_attributes(
        library="streamsets-datacollector-webclient-impl-okhttp-lib",
        jira_instance = jira.url,
        authentication_scheme = 'Basic',
        user_email = jira.username,
        api_token = jira.api_key,
        search_using_jql = f'project=GTMS and issue={issue["key"]}',
        include_comments = include_comments
    )

    wiretap = pipeline_builder.add_wiretap()
    jira_origin >> wiretap.destination

    pipeline = pipeline_builder.build(test_name)
    try:
        work = handler.add_pipeline(pipeline)
        cleanup(handler.stop_work, work)
        handler.start_work(work)
        handler.wait_for_metric(work, "input_record_count", 1, timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

        output_records = wiretap.output_records
        jira_issue = output_records[0]

        assert issue["key"] == jira_issue.field["key"].value
        if include_comments:
           assert jira_issue.field["fields"]["Comment"] is not None
        else:
           assert('Comment' not in jira_issue.field["fields"]), "Comment key should not be present"
    finally:
        #cleanup jira issue created
        jira.delete_issue(issue["key"])