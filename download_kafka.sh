#!/bin/sh

SCALA_VERSION=${1}
KAFKA_VERSION=${2}
KAFKA_DIR=${3}

mirror=$(curl --stderr /dev/null https://www.apache.org/dyn/closer.cgi\?as_json\=1 | sed -rn 's/.*"preferred":.*"(.*)"/\1/p')
url="${mirror}kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"


wget "${url}" -O "/tmp/kafka_release.tgz"
tar xf /tmp/kafka_release.tgz -C /opt
rm -rf /tmp/kafka_release.tgz
mv /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} $KAFKA_DIR
