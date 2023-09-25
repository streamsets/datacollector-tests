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

# A module providing utils for working with Postgres

def compare_database_server_version(version1, version_2_major, version_2_minor, version_2_patch):

    if version1.major > version_2_major:
        return 1
    elif version1.major < version_2_major:
        return -1

    if version1.minor > version_2_minor:
        return 1
    elif version1.minor < version_2_minor:
        return -1

    if version1.patch > version_2_patch:
        return 1
    elif version1.patch < version_2_patch:
        return -1

    return 0  # Versions are equal
