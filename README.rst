===============================
StreamSets Data Collector tests
===============================
 
This repository houses StreamSets Test Framework (STF) tests for StreamSets Data Collector.

Quickstart
==========

To run tests, `install the StreamSets Test Framework`_ and then, after cloning this repository,
``cd`` into it and execute ``stf test``. By default, this will attempt to run all tests using
the `default SDC version`_. If you're just trying STF out for the first time, it might be a better idea
to limit your test run to a module with no external dependencies (e.g. ``stage.test_dev_stages``). It's also
usually desirable to specify an SDC version and add ``-s`` to send test output to the console.
Putting it all together:

.. _install the StreamSets Test Framework: https://streamsets.com/documentation/stf/latest/installation.html

.. _default SDC version: https://streamsets.com/documentation/stf/latest/api.html#streamsets.testframework.sdc.DEFAULT_SDC_VERSION

.. code-block:: console

    $ git clone https://github.com/streamsets/datacollector-tests.git
    Cloning into 'datacollector-tests'...
    remote: Enumerating objects: 192, done.
    remote: Counting objects: 100% (192/192), done.
    remote: Compressing objects: 100% (185/185), done.
    remote: Total 1827 (delta 109), reused 89 (delta 6), pack-reused 1635
    Receiving objects: 100% (1827/1827), 741.96 KiB | 0 bytes/s, done.
    Resolving deltas: 100% (1254/1254), done.
    $ cd datacollector-tests/
    $ stf test --sdc-version 3.4.0 -s stage/test_dev_stages.py
    2018-09-24 11:47:15 AM [INFO] [streamsets.testframework.cli] Pulling Docker image streamsets/testframework:master ...
    ============================= test session starts ==============================
    platform linux -- Python 3.6.6, pytest-3.0.4, py-1.6.0, pluggy-0.4.0
    benchmark: 3.1.1 (defaults: timer=time.perf_counter disable_gc=False min_rounds=5 min_time=0.000005 max_time=1.0 calibration_precision=10 warmup=False warmup_iterations=100000)
    rootdir: /root/tests, inifile:
    plugins: benchmark-3.1.1
    collected 6 items

    stage/test_dev_stages.py 2018-09-24 11:47:39 AM [INFO] [streamsets.testframework.sdc] Starting StreamSets Data Collector 3.4.0 container ...
    2018-09-24 11:47:52 AM [INFO] [streamsets.testframework.sdc] SDC is now running. SDC container (43d14e8197e4:18630) can be followed along on http://localhost:32955
    2018-09-24 11:47:53 AM [WARNING] [streamsets.sdk.sdc_models] PipelineBuilder missing error stage. Will use 'Discard.'
    2018-09-24 11:47:53 AM [INFO] [streamsets.sdk.sdc] Importing pipeline Pipeline35fe51c8-ca73-4a75-9ebc-8332ab49adff...
    2018-09-24 11:47:53 AM [INFO] [streamsets.sdk.sdc_api] Waiting for status EDITED ...
    2018-09-24 11:47:53 AM [INFO] [streamsets.sdk.sdc_api] Pipeline Pipeline35fe51c8-ca73-4a75-9ebc-8332ab49adff reached status EDITED (took 0.00 s).
    2018-09-24 11:47:53 AM [INFO] [streamsets.sdk.sdc] Starting pipeline Pipeline35fe51c8-ca73-4a75-9ebc-8332ab49adff ...
    2018-09-24 11:47:53 AM [INFO] [streamsets.sdk.sdc_api] Waiting for status ['RUNNING', 'FINISHED'] ...
    ...

Folder structure
================

The folder structure of this repository (visualized below using ``tree``) is as follows:::

    ├── datacollector
    ├── fault
    ├── package
    ├── performance
    ├── pipeline
    ├── resources
    │   ├── protobuf
    │   └── tcp_server
    ├── stage
    └── upgrade
        └── pipelines
            ├── sdc_1.1.0
            ├── sdc_1.6.0.0
            ├── sdc_2.0.0.0
            ├── sdc_2.1.0.0
            └── sdc_2.2.0.0

* **datacollector/**: Tests that exercise DataCollector-wide functionality (e.g. classpath validation).

* **fault/** (in progress): Tests that exercise product resilience in the presence of injected faults.

* **package/** (in progress): Packaging tests.

* **performance/** (in progress): Tests that focus on product performance using the `pytest-benchmark plugin`_.

* **pipeline/**: Tests that exercise end-to-end workflows (e.g. the drift synchronization solution)
  or pipeline-level functionality. If the pipeline you want to test is complex, it should probably
  have a test here.

* **resources/**: Resources to be used within tests (e.g. protobuf object schema).

* **stage/**: Tests that attempt to isolate functionality of individual stages. This tends to be in the form of
  ``<dev origin> >> <stage>`` tests for destinations, ``<dev origin> >> stage >> <trash>`` tests for
  processors, or ``<stage> >> <trash>`` tests for origins.

* **upgrade/**: Legacy SDC pipeline upgrade tests. Unless there's a really good reason to do so,
  don't add new tests to this folder.

.. _pytest-benchmark plugin: https://pytest-benchmark.readthedocs.io/
