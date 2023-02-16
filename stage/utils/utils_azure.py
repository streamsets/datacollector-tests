# Copyright 2023 StreamSets Inc.
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

# A module providing utils for working with Azure
import logging
from azure.core.exceptions import ResourceExistsError

logger = logging.getLogger(__name__)

def create_blob_container(azure, container_name):
    logger.info(f'Creating container {container_name} on storage account {azure.storage.account_name}')
    try:
        azure.storage.create_blob_container(container_name)
    except ResourceExistsError:
        logger.warning(f'A container with name "{container_name}" already exists')