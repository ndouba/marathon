#!/bin/bash
# This script runs the scale tests against a physical DC/OS cluster provisioned
# by an external tool. This script expects the user to provide the cluster URL
# as the first argument.

# Require the user to provide the cluster URL
if [ -z "$1" ]; then
  echo "ERROR: Please specify the URL to the EE cluster to use"
  exit 1
fi
CLUSTER_URL="$1"
shift

# Check if we have PLOT_REFERENCE and include the correct configuration
REST_ARGS=""
if [ -z "$PLOT_REFERENCE" ]; then
  REST_ARGS="./config/fragments/report-plot.yml"
else
  REST_ARGS="./config/fragments/report-plot-withref.yml -Dplot_ref='${PLOT_REFERENCE}'"
fi

# If we are running in jenkins, append the correct environment variables
if [ -z "$JENKINS_URL" ]; then
  REST_ARGS="${REST_ARGS} -Menv=dev"
else
  REST_ARGS="${REST_ARGS} -Menv=jenkins"
fi

# Execute all the tests in the configuration
for TEST_CONFIG in ./config/test-*.yml; do

  # Get test name by removing 'test-' prefix and '.yml' suffix
  TEST_NAME=$(basename $TEST_CONFIG)
  TRIM_END=${#TEST_NAME}
  let TRIM_END-=9
  TEST_NAME=${TEST_NAME:5:$TRIM_END}

  # If we have partial tests configured, check if this test exists
  # in the PARTIAL_TESTS, otherwise skip
  if [[ ! -z "$PARTIAL_TESTS" && ! "$PARTIAL_TESTS" =~ "$TEST_NAME" ]]; then
    echo "INFO: Skipping test '${TEST_NAME}' according to PARTIAL_TESTS env vaiable"
    continue
  fi
  echo "INFO: Executing test '${TEST_NAME}'"

  # Launch the performance test driver with the correct arguments
  # (NOTE: Using eval to expand REST_ARGS arguments)
  eval dcos-perf-test-driver \
    ./config/using-cluster-ee.yml \
    ./config/ci-specific-config.yml \
    $TEST_CONFIG \
    $REST_ARGS \
    -D "cluster_url=${CLUSTER_URL}" \
    -D "datadog_api_key=${DATADOG_API_KEY}" \
    -D "datadog_app_key=${DATADOG_APP_KEY}" \
    $*
done
