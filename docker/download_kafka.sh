#!/bin/sh

SCALA_VERSION=${1}
KAFKA_VERSION=${2}
KAFKA_DIR=${3}
JOLOKIA_VERSION=${4}

set -xe

curl -f "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" > "/tmp/kafka_release.tgz"
tar xf /tmp/kafka_release.tgz -C /opt
rm -f /tmp/kafka_release.tgz
mv /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} $KAFKA_DIR

curl -f "http://search.maven.org/remotecontent?filepath=org/jolokia/jolokia-jvm/${JOLOKIA_VERSION}/jolokia-jvm-${JOLOKIA_VERSION}-agent.jar" > "/opt/jolokia-jvm-agent.jar"

