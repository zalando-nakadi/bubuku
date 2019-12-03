FROM registry.opensource.zalan.do/stups/openjdk:1.8.0-162-16
MAINTAINER dmitriy.sorokin@zalando.de

ENV KAFKA_VERSION="2.3.0" SCALA_VERSION="2.12" JOLOKIA_VERSION="1.3.3"
ENV KAFKA_LOGS_DIR="/data/logs"
ENV KAFKA_DIR="/opt/kafka"
ENV HEALTH_PORT=8080
ENV KAFKA_SETTINGS="${KAFKA_DIR}/config/server.properties"
ENV BUKU_FEATURES="restart_on_exhibitor,rebalance_on_start,graceful_terminate,use_ip_address"
ENV KAFKA_OPTS="-server -Dlog4j.configuration=file:${KAFKA_DIR}/config/log4j.properties -Dkafka.logs.dir=${KAFKA_LOGS_DIR} -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=32M -javaagent:/opt/jolokia-jvm-agent.jar=host=0.0.0.0"
ENV KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"

ADD download_kafka.sh /tmp/download_kafka.sh

RUN sh /tmp/download_kafka.sh ${SCALA_VERSION} ${KAFKA_VERSION} ${KAFKA_DIR} ${JOLOKIA_VERSION}

ADD server.properties ${KAFKA_DIR}/config/
ADD run_bubuku_daemon.sh /
ADD server.properties ${KAFKA_SETTINGS}
ADD log4j.properties ${KAFKA_DIR}/config/

RUN mkdir -p $KAFKA_LOGS_DIR/ && \
    apt-get update && \
    apt-get install -y python3-pip && \
    pip3 install --upgrade kazoo boto3 && \
    pip3 install --upgrade bubuku==0.10.48 && \
    chmod 777 /run_bubuku_daemon.sh && \
    chmod -R 777 $KAFKA_LOGS_DIR && \
    chmod 777 ${KAFKA_SETTINGS}

WORKDIR $KAFKA_DIR

ENTRYPOINT ["/bin/bash", "/run_bubuku_daemon.sh"]

EXPOSE 9092 8004 8080

