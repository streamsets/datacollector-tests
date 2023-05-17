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

from contextlib import ExitStack
import logging
import pytest

logger = logging.getLogger(__name__)


@pytest.fixture()
def cleanup(request):
    """Provide an ExitStack to manage cleanup function execution.
    Callbacks are executed LIFO when the importing function exits.
    Args and kwargs are passed in the following way:
    table.drop(table_name, conn=connection) -> cleanup.callback(table.drop, table_name, conn=connection)
    The standard *args **kwargs way is also valid.
    """
    try:
        with ExitStack() as exit_stack:
            yield exit_stack
    except Exception as exception:
        logger.warning(f"Error during cleanup of {request.node.name}: {exception}")
