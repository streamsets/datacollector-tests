<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

StreamSets Data Collector tests
=================
This project hosts StreamSets Test Framework-based tests that target StreamSets Data Collector.

Prerequisites
-----------
Please see [Prerequisites of STF](https://jenkins.streamsets.net/job/testframework-Docs-Build/Test_Framework_Docs/user/install.html#prerequisite-for-stf).

Documentation
-----------
For full documentation, including installation, quickstart examples of tests and API documentation,
please see [Docs](https://jenkins.streamsets.net/job/testframework-Docs-Build/Test_Framework_Docs/).

Execution of sample test
----------------------
1)The following test does not need any environment and runs locally.

```
$ cd datacollector-tests
datacollector-tests$ testframework_run pytest --sdc-version='2.5.0.0-SNAPSHOT' stage/test_dev_raw_data_source_stage.py
============================================= test session starts =============================================
platform linux -- Python 3.5.2, pytest-3.0.4, py-1.4.33, pluggy-0.4.0
rootdir: /root/tests, inifile:
collected 4 items

stage/test_dev_raw_data_source_stage.py ....

========================================== 4 passed in 75.41 seconds ==========================================
```

2)The following test needs CDH environment.
```
datacollector-tests$ testframework_run pytest -v --cluster-server=cm://node-1.cluster --sdc-version=2.6.0.0 stage/test_cdh_stages.py
```

3)To see all the parameters available for running tests, run the following command.
```
datacollector-tests$ testframework_run pytest -h
```

Folder structure for tests
----------------------

```
datacollector-tests
├── LICENSE
├── README.md
├── pipeline
│   ├── test_drift_synchronization.py
├── stage
│   ├── test_cdh_stages.py
│   └── test_jdbc_stages.py
└── upgrade
    ├── pipelines
    │   ├── sdc_1.1.0
    │   │   ├── AmazonS3_trash
    │   │   │   └── sdc_1.1.0_pipeline_AmazonS3_trash.json
    │   │   ├── Dev_Data_Trash
    │   │       └── sdc_1.1.0_pipeline_Dev_Data_Trash.json
    │   ├── sdc_2.1.0.0
    │   │   ├── AmazonS3_Trash
    │   │   │   └── sdc_2.1.0.0_pipeline_AmazonS3_Trash.json
    │   │   ├── DevDataGenerator_Trash
    │   │       └── sdc_2.1.0.0_pipeline_DevDataGenerator_Trash.json
    │   └── sdc_2.2.0.0
    │       ├── AmazonS3_Trash
    │           └── sdc_2.2.0.0_pipeline_AmazonS3_Trash.json
    └── test_pipelines_upgrade.py
```

+ **pipeline/** - This folder contains Python modules to test StreamSets Data Collector (SDC).
            These tests cater to end-to-end scenarios for pipelines already created in SDC.
            e.g. single or multithreaded pipelines.

+ **stage/** - This folder contains Python modules to test a stage in SDC.

+ **upgrade/** - This folder contains Python modules to test legacy SDC pipeline upgrades. Unless there's a really
            good reason to do so, don't add new tests to this folder.

+ **upgrade/pipelines/** - This folder contains JSON files defining the pipelines which are required by tests in folder
            /upgrade. To generate these JSON files, pipelines under test are created using SDC and exported as JSON
            files.
