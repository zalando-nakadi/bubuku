## Usage

```
export PYTHONPATH=$PROJECTS/bubuku/tests
export CONFIG_PATH=$PROJECTS/clusters_config.json
cd $PROJECTS/bubuku/tests/instance_control
```

### Create

```
python3 test_ebs_cli.py create \
                --cluster-name bubuku-1 \
                --cluster-size 1 \
                --cluster-config $CONFIG_PATH
```

### Terminate

```
python3 test_ebs_cli.py terminate \
                --cluster-name bubuku-1 \
                --cluster-config $CONFIG_PATH \
                --ip 10.246.2.11 \
                --user adyachkov \
                --odd odd-transfereu-central-1.aruha-test.zalan.do
```

### Attach

```
python3 test_ebs_cli.py attach \
                --cluster-name bubuku-1 \
                --volume-id vol-0e995bfffc31384c2 \
                --cluster-config $CONFIG_PATH
```

### Upgrade

```
python3 test_ebs_cli.py upgrade \
                --cluster-name bubuku-1 \
                --cluster-config $CONFIG_PATH \
                --ip 10.246.2.11 \
                --user adyachkov \
                --odd odd-transfereu-central-1.aruha-test.zalan.do
```

### Get

```
python3 test_ebs_cli.py get \
                --cluster-name bubuku-1 \
                --cluster-config $CONFIG_PATH
```