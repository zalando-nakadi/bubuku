FROM registry.opensource.zalan.do/stups/python:3.5-cd28
MAINTAINER Team Aruha, team-aruha@zalando.de

ENV KAFKA_VERSION="1.1.1" SCALA_VERSION="2.12" JOLOKIA_VERSION="1.3.3"
ENV KAFKA_DIR="/opt/kafka"

RUN apt-get update && apt-get install wget openjdk-8-jre -y --force-yes && apt-get clean
ADD docker/download_kafka.sh /tmp/download_kafka.sh

RUN sh /tmp/download_kafka.sh ${SCALA_VERSION} ${KAFKA_VERSION} ${KAFKA_DIR}

ADD docker/server.properties ${KAFKA_DIR}/config/

RUN wget -O /tmp/jolokia-jvm-agent.jar http://search.maven.org/remotecontent?filepath=org/jolokia/jolokia-jvm/$JOLOKIA_VERSION/jolokia-jvm-$JOLOKIA_VERSION-agent.jar

ENV KAFKA_OPTS="-server -Dlog4j.configuration=file:${KAFKA_DIR}/config/log4j.properties -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=32M -javaagent:/tmp/jolokia-jvm-agent.jar=host=0.0.0.0"
ENV KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"

RUN mkdir -p $KAFKA_DIR/logs/

ENV KAFKA_SETTINGS="${KAFKA_DIR}/config/server.properties"
ADD docker/server.properties ${KAFKA_SETTINGS}
ADD docker/log4j.properties ${KAFKA_DIR}/config/

ENV SRC_PATH="/bubuku"

RUN mkdir -p "${SRC_PATH}"
WORKDIR "${SRC_PATH}"
ADD ./bubuku "${SRC_PATH}/bubuku"
ADD ./requirements.txt "${SRC_PATH}/"
ADD ./setup.py "${SRC_PATH}/"
RUN pip3 install --no-cache-dir -r requirements.txt

EXPOSE 9092 8080 8778 8888

RUN python3 setup.py develop
ENTRYPOINT ["/bin/bash", "-c", "exec bubuku-daemon"]
