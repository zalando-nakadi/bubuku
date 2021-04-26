FROM registry.opensource.zalan.do/library/openjdk-8:latest
MAINTAINER Team Aruha, team-aruha@zalando.de

ENV KAFKA_VERSION="2.7.0" SCALA_VERSION="2.13" JOLOKIA_VERSION="1.6.2"
ENV KAFKA_LOGS_DIR="/data/logs"
ENV KAFKA_DIR="/opt/kafka"
ENV HEALTH_PORT=8080
ENV KAFKA_SETTINGS="${KAFKA_DIR}/config/server.properties"
ENV BUKU_FEATURES="restart_on_exhibitor,rebalance_on_start,graceful_terminate,use_ip_address"
ENV KAFKA_OPTS="-server -Dlog4j.configuration=file:${KAFKA_DIR}/config/log4j.properties -Dkafka.logs.dir=${KAFKA_LOGS_DIR} -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=32M -javaagent:/opt/jolokia-jvm-agent.jar=host=0.0.0.0"
ENV KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"

ADD docker/download_kafka.sh /tmp/download_kafka.sh

RUN sh /tmp/download_kafka.sh ${SCALA_VERSION} ${KAFKA_VERSION} ${KAFKA_DIR} ${JOLOKIA_VERSION} && apt-get update && apt-get install -y python3-pip 

ADD docker/server.properties ${KAFKA_DIR}/config/
ADD docker/server.properties ${KAFKA_SETTINGS}
ADD docker/log4j.properties ${KAFKA_DIR}/config/

ENV SRC_PATH="/bubuku"

ADD ./bubuku "${SRC_PATH}/bubuku"
ADD ./requirements.txt "${SRC_PATH}/"
ADD ./setup.py "${SRC_PATH}/"
RUN mkdir -p $KAFKA_LOGS_DIR/ && \
    cd "${SRC_PATH}" && \
    pip3 install --no-cache-dir -r "requirements.txt" && \
    python3 setup.py develop && \
    chmod -R 777 $KAFKA_LOGS_DIR && \
    chmod 777 ${KAFKA_SETTINGS}

EXPOSE 9092 8080 8778

ENTRYPOINT ["/bin/bash", "-c", "exec bubuku-daemon"]



