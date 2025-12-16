#!/bin/bash

# Script to download Apache Ratis JARs for the Raft implementation
# Usage: ./download_ratis_jars.sh

set -e

# Configuration
RATIS_VERSION="2.4.1"
RATIS_THIRDPARTY_VERSION="0.8.0"
GRPC_VERSION="1.48.0"
PROTOBUF_VERSION="3.19.2"
MAVEN_REPO="https://repo1.maven.org/maven2"

# Create lib directory if it doesn't exist
mkdir -p lib
cd lib

echo "=========================================="
echo "Downloading Apache Ratis JARs..."
echo "=========================================="

# Apache Ratis Core JARs
echo "Downloading ratis-server-${RATIS_VERSION}.jar..."
curl -L -o ratis-server-${RATIS_VERSION}.jar \
  "${MAVEN_REPO}/org/apache/ratis/ratis-server/${RATIS_VERSION}/ratis-server-${RATIS_VERSION}.jar"

echo "Downloading ratis-client-${RATIS_VERSION}.jar..."
curl -L -o ratis-client-${RATIS_VERSION}.jar \
  "${MAVEN_REPO}/org/apache/ratis/ratis-client/${RATIS_VERSION}/ratis-client-${RATIS_VERSION}.jar"

echo "Downloading ratis-common-${RATIS_VERSION}.jar..."
curl -L -o ratis-common-${RATIS_VERSION}.jar \
  "${MAVEN_REPO}/org/apache/ratis/ratis-common/${RATIS_VERSION}/ratis-common-${RATIS_VERSION}.jar"

echo "Downloading ratis-proto-${RATIS_VERSION}.jar..."
curl -L -o ratis-proto-${RATIS_VERSION}.jar \
  "${MAVEN_REPO}/org/apache/ratis/ratis-proto/${RATIS_VERSION}/ratis-proto-${RATIS_VERSION}.jar"

echo "Downloading ratis-grpc-${RATIS_VERSION}.jar..."
curl -L -o ratis-grpc-${RATIS_VERSION}.jar \
  "${MAVEN_REPO}/org/apache/ratis/ratis-grpc/${RATIS_VERSION}/ratis-grpc-${RATIS_VERSION}.jar"

echo "Downloading ratis-thirdparty-${RATIS_THIRDPARTY_VERSION}.jar..."
curl -L -o ratis-thirdparty-${RATIS_THIRDPARTY_VERSION}.jar \
  "${MAVEN_REPO}/org/apache/ratis/ratis-thirdparty/${RATIS_THIRDPARTY_VERSION}/ratis-thirdparty-${RATIS_THIRDPARTY_VERSION}.jar"

# gRPC Dependencies
echo "Downloading gRPC dependencies..."
curl -L -o grpc-core-${GRPC_VERSION}.jar \
  "${MAVEN_REPO}/io/grpc/grpc-core/${GRPC_VERSION}/grpc-core-${GRPC_VERSION}.jar"

curl -L -o grpc-stub-${GRPC_VERSION}.jar \
  "${MAVEN_REPO}/io/grpc/grpc-stub/${GRPC_VERSION}/grpc-stub-${GRPC_VERSION}.jar"

curl -L -o grpc-protobuf-${GRPC_VERSION}.jar \
  "${MAVEN_REPO}/io/grpc/grpc-protobuf/${GRPC_VERSION}/grpc-protobuf-${GRPC_VERSION}.jar"

curl -L -o grpc-api-${GRPC_VERSION}.jar \
  "${MAVEN_REPO}/io/grpc/grpc-api/${GRPC_VERSION}/grpc-api-${GRPC_VERSION}.jar"

curl -L -o grpc-context-${GRPC_VERSION}.jar \
  "${MAVEN_REPO}/io/grpc/grpc-context/${GRPC_VERSION}/grpc-context-${GRPC_VERSION}.jar"

curl -L -o grpc-protobuf-lite-${GRPC_VERSION}.jar \
  "${MAVEN_REPO}/io/grpc/grpc-protobuf-lite/${GRPC_VERSION}/grpc-protobuf-lite-${GRPC_VERSION}.jar"

# Protobuf
echo "Downloading protobuf-java-${PROTOBUF_VERSION}.jar..."
curl -L -o protobuf-java-${PROTOBUF_VERSION}.jar \
  "${MAVEN_REPO}/com/google/protobuf/protobuf-java/${PROTOBUF_VERSION}/protobuf-java-${PROTOBUF_VERSION}.jar"

# Additional dependencies
echo "Downloading additional dependencies..."

# Guava
curl -L -o guava-31.1-jre.jar \
  "${MAVEN_REPO}/com/google/guava/guava/31.1-jre/guava-31.1-jre.jar"

# SLF4J (for logging)
curl -L -o slf4j-api-1.7.36.jar \
  "${MAVEN_REPO}/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar"

curl -L -o slf4j-simple-1.7.36.jar \
  "${MAVEN_REPO}/org/slf4j/slf4j-simple/1.7.36/slf4j-simple-1.7.36.jar"

# Annotations
curl -L -o annotations-4.1.1.4.jar \
  "${MAVEN_REPO}/org/codehaus/mojo/annotations/4.1.1.4/annotations-4.1.1.4.jar"

echo ""
echo "=========================================="
echo "Download complete!"
echo "=========================================="
echo ""
echo "All JARs have been downloaded to the lib/ directory."
echo "You can now compile and run the Raft implementation."
echo ""
echo "Next steps:"
echo "  1. Compile: javac -cp \"lib/*:.\" src/server/faulttolerance/MyDBFaultTolerantServerRaft.java"
echo "  2. Run servers (see RAFT_IMPLEMENTATION_README.md)"
echo ""

echo "Downloaded files:"
ls -lh *.jar | awk '{print "  " $9 " (" $5 ")"}'

cd ..