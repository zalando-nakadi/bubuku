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
Start supervisor:
```
KAFKA_DIR=/opt/kafka KAFKA_SETTINGS=/opt/kafka/config/server.properties ZOOKEEPER_STACK_NAME=my_zookeeper \
ZOOKEEPER_PREFIX=/test BROKER_ID_POLICY=ip BUKU_FEATURES=restart_on_exhibitor,graceful_terminate \
HEALTH_PORT=8888 bubuku-daemon
```
Run commands on cluster:
```
export KAFKA_DIR=/opt/kafka
export KAFKA_SETTINGS=/opt/kafka/config/server.properties 
export ZOOKEEPER_STACK_NAME=my_zookeeper 
export ZOOKEEPER_PREFIX=/test 
export BROKER_ID_POLICY=ip

# Restart kafka on current node
bubuku-cli restart
# Restart kafka on some other node (broker id must be known)
bubuku-cli restart --broker=12324

# Invoke partitions rebalance
bubuku-cli rebalance
```
It is important to have all properties provided, because command processing is made over zookeeper stack. 

# Configuration

Bubuku can be configured using environment properties:

 - `KAFKA_DIR` - kafka root directory (for example /opt/kafka) 
 - `KAFKA_SETTINGS` - Path to kafka settings template file. Bubuku will add (or replace/delete) it's own 
 properties to this file and write the contents to `${KAFKA_DIR}/config/server.properties`. Kafka will be started 
  against generated file.
 - `ZOOKEEPER_STACK_NAME` - AWS load balancer name for zookeeper stack
 - `ZOOKEEPER_STATIC_IPS_PORT` - (overrides ZOOKEEPER_STACK_NAME) - static list 
 of ips/port of zookeeper stack in the following form: 127.0.0.1,127.0.0.2,127.0.0.3:2181 - several ips and 1 port 
 - `ZOOKEEPER_PREFIX` - Prefix for all the nodes in zk for kafka and bubuku
 - `BROKER_ID_POLICY` - Policy for generating broker id. Possible values are `ip` and `auto`
 - `BUKU_FEATURES` - List of optional bubuku features, see [features](#features) section
 - `HEALTH_PORT` - Port for health checks
 - `FREE_SPACE_DIFF_THRESHOLD_MB` - Threshold for starting `balance_data_size` feature, if it's enabled
 - `STARTUP_TIMEOUT_TYPE`, `STARTUP_TIMEOUT_INITIAL`, `STARTUP_TIMEOUT_STEP` - The way bubuku manages [time to start for kafka](#startup_timeout).
 
# Features #

Bubuku provides:
    
 - Ip address discovery using AWS
 - Exhibitor discovery using AWS load balancer name
 - Rebalance partitions on different events
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
 - `rebalance_on_brokers_change` - Rebalance partition distribution across cluster (using partition count and leader 
 count per broker as optimization strategy) on any broker list change (new broker started, old broker died)
 - `graceful_terminate` - In case when bubuku is killed, try to gracefully terminate kafka process.
 - `use_ip_address` - Use ip address when registering kafka instance. By default kafka registers itself in 
 zookeeper using hostname. Sometimes (for example on migration between AWS regions) it makes sense to use ip 
 address instead of hostname.
 - `balance_data_size` - Swap partitions one by one by one if imbalance in size on brokers is bigger than 
 `FREE_SPACE_DIFF_THRESHOLD_MB` megabytes.
 
## <a name="startup_timeout"></a> Timeouts for startup
 Each time when bubuku tries to start kafka, it uses special startup timeout. This means, that if kafka broker id 
 is not found within this timeout in zookeeper node `/broker/ids/{id}`, kafka process will be forcibly killed, timeout 
 for start updated, and startup will be retried. 
  
  There are two ways to increase timeout - linear and progressive. Linear adds the same amount of time after each 
  failed start. Progressive adds time, that is relative to current timeout. Configuration for that is provided by 
  `STARTUP_TIMEOUT_TYPE`, `STARTUP_TIMEOUT_INITIAL`, `STARTUP_TIMEOUT_STEP` parameters.
  ```
  # Linear timeout configuration 
  # initial timeout=300 seconds, after each failed start increase by 60 seconds (360, 420 and so on)
  export STARTUP_TIMEOUT_TYPE="linear"
  export STARTUP_TIMEOUT_INITIAL="300"
  export STARTUP_TIMEOUT_STEP="60"
  ```
  ```
  # Progressive timeout configuration
  # Initial timeout=300 seconds, after each failed start increase by timeout * 0.5 (450, 675 and so on)
  export STARTUP_TIMEOUT_TYPE="progressive"
  export STARTUP_TIMEOUT_INITIAL="300"
  export STARTUP_TIMEOUT_STEP="0.5"
  ```

 Default values for timeout are
 ```
  export STARTUP_TIMEOUT_TYPE="linear"
  export STARTUP_TIMEOUT_INITIAL="300"
  export STARTUP_TIMEOUT_STEP="60"
 ```
 
# Command Line Interface #

Bubuku provides a command line tool `bubuku-cli` which should be used directly on the instance. Available commands:

[comment]: # (bubuku-cli help below is auto-generated, do not edit manually)

#### actions delete

```
Usage: bubuku-cli actions delete [OPTIONS]

  Remove all actions of specified type on broker(s)

Options:
  --action TEXT  Action to delete
  --broker TEXT  Broker id to delete actions on. By default actions are
                 deleted on all brokers
  --help         Show this message and exit.

```
#### actions list

```
Usage: bubuku-cli actions list [OPTIONS]

  List all the actions on broker(s)

Options:
  --broker TEXT  Broker id to list actions on. By default all brokers are
                 enumerated
  --help         Show this message and exit.

```
#### migrate

```
Usage: bubuku-cli migrate [OPTIONS]

  Replace one broker with another for all partitions

Options:
  --from TEXT            List of brokers to migrate from (separated with ",")
  --to TEXT              List of brokers to migrate to (separated with ",")
  --shrink               Whether or not to shrink replaced broker ids form
                         partition assignment  [default: False]
  --broker TEXT          Optional broker id to execute check on
  --throttle INTEGER     Upper bound on bandwidth (in bytes/sec) used for
                         reassigning partitions
  --parallelism INTEGER  Amount of partitions to move in a single migration
                         step  [default: 1]
  --remove-throttle      Don't trigger rebalance but remove throttling
                         configuration from all the brokers and topics
  --help                 Show this message and exit.

```
#### rebalance

```
Usage: bubuku-cli rebalance [OPTIONS]

  Run rebalance process on one of brokers. If rack-awareness is enabled,
  replicas will only be move to other brokers in the same rack

Options:
  --broker TEXT          Broker instance on which to perform rebalance. By
                         default, any free broker will start it
  --empty_brokers TEXT   Comma-separated list of brokers to empty. All
                         partitions will be moved to other brokers
  --exclude_topics TEXT  Comma-separated list of topics to exclude from
                         rebalance
  --bin-packing          Use bean packing approach instead of one way
                         processing
  --parallelism INTEGER  Amount of partitions to move in a single rebalance
                         step  [default: 1]
  --throttle INTEGER     Upper bound on bandwidth (in bytes/sec) used for
                         rebalance
  --remove-throttle      Don't trigger rebalance but remove throttling
                         configuration from all the brokers and topics
  --help                 Show this message and exit.

```
#### restart

```
Usage: bubuku-cli restart [OPTIONS]

  Restart kafka instance

Options:
  --broker TEXT  Broker id to restart. By default current broker id is
                 restarted
  --help         Show this message and exit.

```
#### rolling-restart

```
Usage: bubuku-cli rolling-restart [OPTIONS]

  Rolling restart of Kafka cluster

Options:
  --image-tag TEXT      Docker image to run Kafka broker
  --instance-type TEXT  AWS instance type to run Kafka broker on
  --scalyr-key TEXT     Scalyr account key
  --scalyr-region TEXT  Scalyr region to use
  --kms-key-id TEXT     Kms key id to decrypt data with
  --cool-down INTEGER   Number of seconds to wait before passing the restart
                        task to another broker, after cluster is stable
                        [default: 20]
  --help                Show this message and exit.

```
#### stats

```
Usage: bubuku-cli stats [OPTIONS]

  Display statistics about brokers

Options:
  --help  Show this message and exit.

```
#### swap_fat_slim

```
Usage: bubuku-cli swap_fat_slim [OPTIONS]

  Move one partition from fat broker to slim one

Options:
  --threshold INTEGER  Threshold in kb to run swap  [default: 100000]
  --help               Show this message and exit.

```
#### validate replication

```
Usage: bubuku-cli validate replication [OPTIONS]

  Returns all partitions whose ISR size differs from the replication factor
  or have not registered broker ids

Options:
  --factor INTEGER  Replication factor  [default: 3]
  --help            Show this message and exit.

```

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
