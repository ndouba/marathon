#!/bin/bash
# This script installs possible missing dependencies in order to run
# the scale tests in a CI environment.

# Collect github user credentials if available in the environment
AUTH=""
if [ ! -z "$GH_USERNAME" ]; then
  AUTH="${GH_USERNAME}@"
  if [ ! -z "$GH_PASSWORD" ]; then
    AUTH="${GH_USERNAME}:${GH_PASSWORD}@"
  fi
fi

# Install missing performance test driver
which dcos-perf-test-driver >/dev/null
if [ $? -ne 0 ]; then
  echo "Installing DC/OS Performance Test Driver"
  pip3 install git+https://${AUTH}github.com/mesosphere/dcos-perf-test-driver.git
fi
