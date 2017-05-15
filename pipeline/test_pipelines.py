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

import logging
import os
import pytest
from uuid import uuid4
from os.path import dirname, join, realpath

from testframework.markers import *
from testframework import environment, sdc, sdc_api, sdc_models

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#
# Utility functions
#

def pipeline_file_path(file, dir='pipelines'):
    return join(dirname(realpath(__file__)), dir, file)

#
# Strict impersonation (SDC-3704).
#

@cluster('cdh')
def test_strict_impersonation_hdfs(args):
    cluster = environment.Cluster(cluster_server=args.cluster_server)
    pipeline = sdc_models.Pipeline(
        pipeline_file_path('test_strict_impersonation_hdfs.json')
    ).configure_for_environment(cluster)

    hdfs_path = os.path.join(os.sep, "tmp", str(uuid4()))
    pipeline.stages['HadoopFS_01'].output_path = hdfs_path

    dc = sdc.DataCollector(version=args.sdc_version)
    dc.set_user('admin')
    dc.add_pipeline(pipeline)
    dc.sdc_properties['stage.conf_hadoop.always.impersonate.current.user'] = 'true'

    # Run at least one batch of data (write something).
    dc.start()
    dc.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished()
    dc.stop_pipeline(pipeline).wait_for_stopped()

    # Validate that the files were created with proper user name.
    entries = cluster.hdfs.client.list(hdfs_path)
    assert len(entries) == 1

    status = cluster.hdfs.client.status("{0}/{1}".format(hdfs_path, entries[0]))
    assert status['owner'] == 'admin'
