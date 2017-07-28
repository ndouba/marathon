# Marathon Performance Tests

These tests can either run against a provisioned DC/OS Cluster, or locally using mesos simulator.

Historically scale testing is a measure of the number of concurrent requests a service can handle or how the performance characteristics change over that load. In the case of Marathon and DCOS, scale specifically refers to the performance characteristics of Marathon and it's ability to handle X number of tasks.   The number of tasks are controlled by

1) Number of Apps or Pods
2) Number of Instances of Apps or Pods
3) The complexity of the definition

## Requirements

* [DC/OS Performance Test Driver](https://github.com/mesosphere/dcos-perf-test-driver)
* One of:
    * A provisioned DC/OS Cluster (Enterprise or Open)
    * A valid development environemnt that can run a development version of marathon

To run the tests you can simply use the `dcos-perf-test-driver` tool. Some configurations require additional variables to be defined, for example:

```
dcos-perf-test-driver ./config/scale-n-apps-n-instances-local.yml \
    --define marathon_dir=(git rev-parse --show-toplevel)
```

If there are errors you would like to investigate you can run the driver in verbose mode:

```
dcos-perf-test-driver ./config/scale-n-apps-n-instances-local.yml \
    --define marathon_dir=(git rev-parse --show-toplevel) \
    --verbose
```

## Scale Tests

There is currently one scale test available:

* `scale-n-apps-n-instances-*.yml` : Explores the deployment time and the HTTP response time of marathon across variable number of instances and apps.

## Test Results

The test is producing a variety of results:

* [dump.json](example/dump.json) - Contains the raw dump of the metrics collected for various parameters during the scale test.
* [results.csv](example/results.csv) - Contains a CSV with the summarised results for every parameter configuration.
* [plot-*.png](example/plot-deploymentTime-mean_err.png) - Image plots with the scale test results. Every metric will have it's own image plot generated.
* _S3 RAW Upload_ - In addition to the local raw dump, the results are uploaded to S3 for long-term archiving.
* _Postgres Database_ - When used in CI, the results are posted in a Postgres database for long-term archiving and plotting through a PostgREST endpoint.
* _Datadog_ - When used in CI, a single "indicator" result will be submitted to Datadog for long-term archiving and alerting.

## Running in CI

All of the CI automation is implenented as a set of `ci_*.sh` scripts. These scripts require some configuration environment variables and are appropriately configuring and launching `dcos-scale-test-driver`.

This section describes the usage and dequirements of each script.


### `ci_install.sh` - Install Depndencies

Install missing dependencies.

#### Requirements

* python3
* python3-pip

#### Environment variables

* `GH_USERNAME` _[Optional]_ : The username to authenticate on GitHub for fetching the dcos-perf-test-driver
* `GH_PASSWORD` _[Optional]_ : The password (or personal access token) for the GitHub user above.



### `ci_run_simulator.sh` - Run tests against simulator

Runs the scale tests against a locally-launched marathon with mesos-simulator back-end.

#### Requirements

* A marathon development environment (java, scala, sbt)

#### Environment variables

* `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` : The AWS Credentials, used to upload the results to S3.
* `DATADOG_API_KEY`, `DATADOG_APP_KEY` : The DataDog Credentials, used to upload the indicators to the datadog dashboard.

* `PLOT_REFERENCE` _[Optional]_ : A URL to the RAW data to use as reference to the plots. If missing, no reference data will be included.
* `PARTIAL_TESTS` _[Optional]_ : A comma-separated list with the names of the tests to run. If missing the full set of tests will run.

### `ci_run_cluster_ee.sh` - Run tests against an EE cluster

Runs the scale tests against a pre-deployed EE cluster.

#### Requirements

* A provisioned Enterprise Edittion DC/OS Cluster for which you have the public URL.

#### Arguments

```
./ci_run_cluster_ee.sh <cluster_url>
```

* `<cluster_url>` : The enterprise cluster URL

#### Environment variables

* `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` : The AWS Credentials, used to upload the results to S3.
* `DATADOG_API_KEY`, `DATADOG_APP_KEY` : The DataDog Credentials, used to upload the indicators to the datadog dashboard.

* `PLOT_REFERENCE` _[Optional]_ : A URL to the RAW data to use as reference to the plots. If missing, no reference data will be included.
* `PARTIAL_TESTS` _[Optional]_ : A comma-separated list with the names of the tests to run. If missing the full set of tests will run.



### `ci_run_cluster_oss.sh` - Run tests against an EE cluster

Runs the scale tests against a pre-deployed OSS cluster.

#### Requirements

* A provisioned Open-Source Edittion DC/OS Cluster for which you have the public URL.

#### Arguments

```
./ci_run_cluster_oss.sh <cluster_url>
```

* `<cluster_url>` : The open-source cluster URL

#### Environment variables

* `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` : The AWS Credentials, used to upload the results to S3.
* `DATADOG_API_KEY`, `DATADOG_APP_KEY` : The DataDog Credentials, used to upload the indicators to the datadog dashboard.

* `PLOT_REFERENCE` _[Optional]_ : A URL to the RAW data to use as reference to the plots. If missing, no reference data will be included.
* `PARTIAL_TESTS` _[Optional]_ : A comma-separated list with the names of the tests to run. If missing the full set of tests will run.
