## Usage

The script helps to manage upgrades of Bubuku(Kafka) without terminating EBS volume.

```
export PYTHONPATH=$PROJECTS/bubuku
export CONFIG_PATH=$PROJECTS/clusters_config.json
cd $PROJECTS/bubuku/instance_control
chmod u+x bubuku_cluster.py
```

### Create

```
bubuku_cluster.py create \
                --cluster-size 1 \
                --cluster-config $CONFIG_PATH
```

### Terminate

```
bubuku_cluster.py terminate \
                --cluster-config $CONFIG_PATH \
                --ip 10.246.2.11 \
                --user adyachkov \
                --odd odd-transfereu-central-1.aruha-test.zalan.do
```

### Attach

```
bubuku_cluster.py attach \
                --volume-id vol-0e995bfffc31384c2 \
                --cluster-config $CONFIG_PATH
```

### Upgrade

```
bubuku_cluster.py upgrade \
                --cluster-config $CONFIG_PATH \
                --ip 10.246.2.11 \
                --user adyachkov \
                --odd odd-transfereu-central-1.aruha-test.zalan.do
```

### Get

```
bubuku_cluster.py get \
                --cluster-config $CONFIG_PATH
```