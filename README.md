[![Build Status](https://travis-ci.org/zalando-incubator/bubuku.svg)](https://travis-ci.org/zalando-incubator/bubuku)

# Introduction

Bubuku - supervisor for kafka


Google translate with automatic language detection says that it means 'day' in
Xhosa language.


The purpose of bubuku is to start, monitor and rebalance kafka cluster in AWS setup, handling these actions in 
coordinated manner.
 
Bubuku assumes that kafka is already installed on a machine. Version of kafka that is tested to be working
with current release: 0.9.0.1

# Installation
```
pip3 install bubuku
```

# Running
```
KAFKA_DIR=/opt/kafka KAFKA_SETTINGS=/opt/kafka/config/server.properties ZOOKEEPER_STACK_NAME=my_zookeeper \
ZOOKEEPER_PREFIX=/test BROKER_ID_POLICY=ip BUKU_FEATURES=restart_on_exhibitor,graceful_terminate \
HEALTH_PORT=8888 bubuku 
```

# Configuration

Bubuku can be configured using environment properties:

 - `KAFKA_DIR` - kafka root directory (for example /opt/kafka) 
 - `KAFKA_SETTINGS` - Path to kafka settings template file. Bubuku will add (or replace/delete) it's own 
 properties to this file and write the contents to `${KAFKA_DIR}/config/server.properties`. Kafka will be started 
  against generated file.
 - `ZOOKEEPER_STACK_NAME` - AWS load balancer name for zookeeper stack
 - `ZOOKEEPER_PREFIX` - Prefix for all the nodes in zk for kafka and bubuku
 - `BROKER_ID_POLICY` - Policy for generating broker id. Possible values are `ip` and `auto`
 - `BUKU_FEATURES` - List of optional bubuku features, see [features](#features) section
 - `HEALTH_PORT` - Port for health checks
 
# Features #

Bubuku provides:
    
 - Ip address discovery using AWS
 - Exhibitor discovery using AWS load balancer name
 - Rebalance partitions on startup
 - React on exhibitor topology change
 - Automatic kafka restart in case if broker is considered dead
 - Graceful broker termination in case of supervisor stop
 - Broker start/stop/restart synchronization across cluster
 
## <a name="features"></a> Pluggable features
Pluggable features are defined in configuration and are disabled by default. List of features:
 
 - `restart_on_exhibitor` - restart kafka broker on zookeeper address list change. Kafka by itself do not support
 zookeeper list provider, so in order to override list of zookeeper instances in runtime there will be configuration
 change and broker restart. This change is made one by one for each broker in cluster (so kafka instances won't die
 all at the same time)
 - `rebalance_on_start` - Rebalance partition distribution across cluster (using partition count and leader count
 per broker as optimization strategy) during initial broker startup
 - `graceful_terminate` - In case when bubuku is killed, try to gracefully terminate kafka process.
 - `use_ip_address` - Use ip address when registering kafka instance. By default kafka registers itself in 
 zookeeper using hostname. Sometimes (for example on migration between AWS regions) it makes sense to use ip 
 address instead of hostname.
 
# How to contribute

If you have any features or bugfixes - make pull request providing feature/bugfix and tests that will test your 
feature/bugfix.

# Reporting issues

If you experiencing problems with bubuku and you know that it can be improved - please fill free to post issue
to github. Please provide full description of feature or bug (optionally with unit test), so it can be fixed 
faster.

# License

Copyright (c) 2016 Zalando SE

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit
persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.