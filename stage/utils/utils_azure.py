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
import string
from azure.core.exceptions import ResourceExistsError
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

# Reference:
# https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-shares--directories--files--and-metadata
AZURE_OBJECT_NAMES = [
    ('max_size', get_random_string(string.ascii_lowercase, 255)),
    ('min_size', get_random_string(string.ascii_lowercase, 1)),
    ('lowercase', get_random_string(string.ascii_lowercase, 20)),
    ('uppercase', get_random_string(string.ascii_uppercase, 20)),
    ('mixedcase', get_random_string(string.ascii_letters, 20) + get_random_string(string.ascii_letters, 20)),
    ('numbers', get_random_string(string.ascii_letters, 5) + "1234567890" + get_random_string(string.ascii_letters, 5))
#    TODO: Re-enable when COLLECTOR-4896 is fixed
#    ('special', get_random_string(string.ascii_letters, 5) + "!@·$%&()='¡¿[]`^+{}´¨,;-_" + get_random_string(string.ascii_letters, 5))
]

def create_blob_container(azure, container_name):
    logger.info(f'Creating container {container_name} on storage account {azure.storage.account_name}')
    try:
        azure.storage.create_blob_container(container_name)
    except ResourceExistsError:
        logger.warning(f'A container with name "{container_name}" already exists')