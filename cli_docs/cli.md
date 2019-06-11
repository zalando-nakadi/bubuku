# Bubuku command line interface

Bubuku provides a command line tool `bubuku-cli` which should be used directly on the instance. Available commands:

#### preferred-replica-election
```
Usage: bubuku-cli preferred-replica-election [OPTIONS]

  Do preferred replica election, as command line tool from kafka have a
  number of limitations. Only partitions, that are improperly allocated will
  be affected. In case if size of resulting json is too big, it will be
  split into several parts, and they will be executed one after another.

Options:
  --dry-run                Do not apply the changes. Instead just prepare json
                           file(s)
  --max-json-size INTEGER  Maximum size of json data in bytes to write to zk
                           [default: 512000]
  --help                   Show this message and exit.
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

#### swap_fat_slim
```
Usage: bubuku-cli swap_fat_slim [OPTIONS]

  Move one partition from fat broker to slim one

Options:
  --threshold INTEGER  Threshold in kb to run swap  [default: 100000]
  --help               Show this message and exit.
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

#### stats
```
Usage: bubuku-cli stats [OPTIONS]

  Display statistics about brokers

Options:
  --help  Show this message and exit.
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

