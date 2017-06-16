## Usage

The script helps to manage upgrades of Bubuku(Kafka) without terminating EBS volume.

### Environment set up
#### Dependencies:
```
requests
boto3
netaddr
```

#### Configuration
```
export PYTHONPATH=$PROJECTS/bubuku
export CONFIG_PATH=$PROJECTS/clusters_config.json
cd $PROJECTS/bubuku/instance_control
chmod u+x bubuku_cluster.py
```

### Create
The command is used to create a complete new cluster or add new nodes to the existing cluster (--cluster-size defines how many nodes you want to create/add to the cluster for provided config)
```
bubuku_cluster.py create \
                --cluster-size 1 \
                --cluster-config $CONFIG_PATH
```

### Terminate
The command teminates an instance by provided IP address. After termination the volume is left in the state, available to be attached to another instance (the volume itself is not deleted).
```
bubuku_cluster.py terminate \
                --cluster-config $CONFIG_PATH \
                --ip 10.246.2.11 \
                --user adyachkov \
                --odd odd-transfereu-central-1.aruha-test.zalan.do
```

### Attach
The command creates a new instance and attaches provided volume to that instance.
```
bubuku_cluster.py attach \
                --volume-id vol-0e995bfffc31384c2 \
                --cluster-config $CONFIG_PATH
```

### Upgrade
The command terminates an instance, detaches a volume, creates a new volume and attaches the volume to the created instance. 
```
bubuku_cluster.py upgrade \
                --cluster-config $CONFIG_PATH \
                --ip 10.246.2.11 \
                --user adyachkov \
                --odd odd-transfereu-central-1.aruha-test.zalan.do
```

### Get
The command lists the cluster nodes info.
```
bubuku_cluster.py get \
                --cluster-config $CONFIG_PATH
```
