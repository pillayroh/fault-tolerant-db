#!/bin/bash

# Script to run GraderFaultTolerance tests in Zookeeper mode
# Make sure Cassandra and Zookeeper are running before executing this script

cd "$(dirname "$0")"

# Build classpath with bin directory and all JARs in lib directory
CLASSPATH="bin:lib/*"

# Run the tests (GIGAPAXOS_MODE will be false by default, so Zookeeper mode is used)
java -cp "$CLASSPATH" GraderFaultTolerance

