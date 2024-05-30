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
DESTINATION = "destination"
DEFAULT_TIMEOUT_IN_SEC = 300

logger = logging.getLogger(__name__)
pytestmark = [jira, sdc_min_version(RELEASE_VERSION)]


def test_branded_jira_create_issue(sdc_builder, sdc_executor, cleanup, test_name, jira):
    """
    Test for Jira Destination create issue action. First, create jira issue and then verify issue is created using search
    for that issue using issue summary used.
    """
    issue_timestamp = str(datetime.now())
    jira_issue_summary= f'Test Summary {issue_timestamp}'

    #create jira issue
    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    raw_data = {"summary":jira_issue_summary}
    dev_raw_data_source = pipeline_builder.add_stage("Dev Raw Data Source")
    raw_data = json.dumps(raw_data)
    dev_raw_data_source.set_attributes(data_format="JSON", stop_after_first_batch=True, raw_data=raw_data)

    jira_destination = pipeline_builder.add_stage(JIRA, type="destination")
    jira_destination.set_attributes(
        library="streamsets-datacollector-webclient-impl-okhttp-lib",
        jira_instance = jira.url,
        authentication_scheme = 'Basic',
        user_email = jira.username,
        api_token = jira.api_key,
        action = 'Create',
        project = 'GTMS',
        summary = "${record:value('/summary')}",
        priority = "Low",
        issue_type = "Task",
        description = "Test Description"
    )

    dev_raw_data_source >> jira_destination
    pipeline = pipeline_builder.build(test_name)

    issue_key = None
    try:
        work = handler.add_pipeline(pipeline)
        cleanup(handler.stop_work, work)
        handler.start_work(work)
        handler.wait_for_status(work, "FINISHED", timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

        #verify that issue got created by searching jira issue using issue summary
        jql = f'project=GTMS and summary~"{jira_issue_summary}"'
        jira_issue = jira.search_jql(jql)
        issue_key =  jira_issue["issues"][0]["key"]

        assert jira_issue is not None
        jira_issue["issues"][0]["fields"]["summary"] = jira_issue_summary
    finally:
        #cleanup jira issue created
        jira.delete_issue(issue_key)


def test_branded_jira_update_issue(sdc_builder, sdc_executor, cleanup, test_name, jira):
    """
    Test for Jira Destination update issue action. First, create jira issue and then
     and then use issue key to update issue.
    """

    #create jira issue
    jira_issue = create_jira_issue(jira, test_name)

    #update jira issue
    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    updated_description = "GTMS description updated"
    raw_data = {"description": updated_description}
    dev_raw_data_source = pipeline_builder.add_stage("Dev Raw Data Source")
    raw_data = json.dumps(raw_data)
    dev_raw_data_source.set_attributes(data_format="JSON", stop_after_first_batch=True, raw_data=raw_data)

    jira_destination = pipeline_builder.add_stage(JIRA, type="destination")
    jira_destination.set_attributes(
        library="streamsets-datacollector-webclient-impl-okhttp-lib",
        jira_instance = jira.url,
        authentication_scheme = 'Basic',
        user_email = jira.username,
        api_token = jira.api_key,
        action = 'Update',
        issue_id = jira_issue["key"],
        new_description = "${record:value('/description')}"
    )

    dev_raw_data_source >> jira_destination
    pipeline = pipeline_builder.build(test_name)

    try:
        work = handler.add_pipeline(pipeline)
        cleanup(handler.stop_work, work)
        handler.start_work(work)
        handler.wait_for_status(work, "FINISHED", timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

        #verify that issue got created by searching jira issue using issue summary
        issue = jira.get_issue(jira_issue["key"])
        assert issue is not None
        assert updated_description == issue["fields"]["description"]
    finally:
        #cleanup jira issue created
        jira.delete_issue(jira_issue["key"])


def test_branded_jira_delete_issue(sdc_builder, sdc_executor, cleanup, test_name, jira):
    """
    Test for Jira Destination update issue action. First, create jira issue and then get
    that issue using issue key and then use issue key to delete issue.
    """

    #create jira issue
    issue = create_jira_issue(jira, test_name)

    #verify issue exists
    issue = jira.get_issue(issue["key"])
    assert issue is not None

    #delete jira issue
    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    raw_data = {"issueId":issue["key"]}
    dev_raw_data_source = pipeline_builder.add_stage("Dev Raw Data Source")
    raw_data = json.dumps(raw_data)
    dev_raw_data_source.set_attributes(data_format="JSON", stop_after_first_batch=True, raw_data=raw_data)

    jira_destination = pipeline_builder.add_stage(JIRA, type="destination")
    jira_destination.set_attributes(
        library="streamsets-datacollector-webclient-impl-okhttp-lib",
        jira_instance = jira.url,
        authentication_scheme = 'Basic',
        user_email = jira.username,
        api_token = jira.api_key,
        action = 'Delete',
        issue_id = "${record:value('/issueId')}"
    )

    dev_raw_data_source >> jira_destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)
    cleanup(handler.stop_work, work)
    handler.start_work(work)
    handler.wait_for_status(work, "FINISHED", timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

    #verify that issue got deleted by getting no jira issue using issue id
    try:
      jira_issue = jira.get_issue(issue["key"])
      assert jira_issue is None
    except Exception as ex:
      assert ex.args[0]['errorMessages'][0] == 'Issue does not exist or you do not have permission to see it.'