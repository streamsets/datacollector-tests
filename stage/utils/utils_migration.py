# Copyright 2022 StreamSets Inc.
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
r"""Tools used to ease migration to next testing."""

from abc import ABC, abstractmethod


class PipelineHandler(ABC):
    @abstractmethod
    def add_pipeline(self, pipeline):
        pass

    @abstractmethod
    def execute_shell(self, *args, **kwargs):
        pass

    @abstractmethod
    def get_pipeline_builder(self):
        pass

    @abstractmethod
    def start_work(self, work):
        pass

    @abstractmethod
    def stop_work(self, work):
        pass

    @abstractmethod
    def validate_pipeline(self, pipeline):
        pass

    @abstractmethod
    def wait_for_metric(self, *args, **kwargs):
        pass

    @abstractmethod
    def wait_for_status(self, *args, **kwargs):
        pass

    # ToDo Mikel: wiretap


class LegacyHandler(PipelineHandler):
    def __init__(self, sdc_builder, sdc_executor, database, cleanup, test_name, logger):
        self.sdc_builder = sdc_builder
        self.sdc_executor = sdc_executor
        self.database = database
        self.cleanup = cleanup
        self.test_name = test_name
        self.logger = logger

    def add_pipeline(self, pipeline):
        self.sdc_executor.add_pipeline(pipeline)
        return pipeline

    def execute_shell(self, *args, **kwargs):
        return self.sdc_executor.execute_shell(*args, **kwargs)

    def get_pipeline_builder(self):
        return self.sdc_builder.get_pipeline_builder()

    def start_work(self, work):
        self.sdc_executor.start_pipeline(work)
        return self

    def stop_work(self, work):
        return self.sdc_executor.stop_pipeline(work)

    def validate_pipeline(self, pipeline):
        return self.sdc_executor.validate_pipeline(pipeline)

    def wait_for_metric(self, *args, **kwargs):
        return self.sdc_executor.wait_for_pipeline_metric(*args, **kwargs)

    def wait_for_status(self, *args, **kwargs):
        return self.sdc_executor.wait_for_pipeline_status(*args, **kwargs)


class NextHandler(PipelineHandler):
    def __init__(self):
        pass

    def add_pipeline(self, pipeline):
        pass

    def execute_shell(self, *args, **kwargs):
        pass

    def get_pipeline_builder(self):
        pass

    def start_work(self, work):
        pass

    def stop_work(self, work):
        pass

    def validate_pipeline(self, pipeline):
        pass

    def wait_for_metric(self, *args, **kwargs):
        pass

    def wait_for_status(self, *args, **kwargs):
        pass
