# Worker-Bunch

... is a task/rule engine for use in a smarthome environment.

*Worker-Bunch* provides a programming infrastructure for create your own rules/tasks with your own functionality.

These rules/tasks are called "workers" here. Each worker runs as a separate thread. The

Features:
- Workers listen to timer events (even with a cron syntax)
- Workers listen and send MQTT messages (prepacked MQTT client)
- MQTT messages get debounced. 
- Provides a JSON schema based configuration
- Runs as linux service.
- Prepacked Postgres connectors.


## Startup

### Prepare python environment
```bash
cd /opt
sudo mkdir worker-bunch
sudo chown <user>:<user> worker-bunch  # type in your user
git clone https://github.com/rosenloecher-it/worker-bunch worker-bunch

cd worker-bunch
virtualenv -p /usr/bin/python3 venv

# activate venv
source ./venv/bin/activate

# check python version >= 3.8
python --version

# install required packages
pip install -r requirements.txt
```

### Configuration

```bash
# cd ... goto project dir
cp ./worker-bunch.yaml.sample ./worker-bunch.yaml

# security concerns: make sure, no one can read the stored passwords
chmod 600 ./worker-bunch.yaml
```

Edit your `worker-bunch.yaml`. See comments there.

### Run

```bash
# see command line options
./worker-bunch.sh --help

# prepare your own config file based on ./worker-bunch.yaml.sample
# the embedded json schema may contain additional information
./worker-bunch.sh --json-schema
# (the JSON schema get dynamically adapted by configured workers.)

# create database schema manually analog to ./scripts/*.sql or let the app do it
./worker-bunch.sh --create --print-logs --config-file ./worker-bunch.yaml

# start the logger
./worker-bunch.sh --print-logs --config-file ./worker-bunch.yaml
# abort with ctrl+c

```

## Register as systemd service
```bash
# prepare your own service script based on worker-bunch.service.sample
cp ./worker-bunch.service.sample ./worker-bunch.service

# edit/adapt pathes and user in worker-bunch.service
vi ./worker-bunch.service

# install service
sudo cp ./worker-bunch.service /etc/systemd/system/
# alternativ: sudo cp ./worker-bunch.service.sample /etc/systemd/system//worker-bunch.service
# after changes
sudo systemctl daemon-reload

# start service
sudo systemctl start worker-bunch

# check logs
journalctl -u worker-bunch
journalctl -u worker-bunch --no-pager --since "5 minutes ago"

# enable autostart at boot time
sudo systemctl enable worker-bunch.service
```

## Additional infos

### MQTT broker related infos

If no messages get logged check your broker.
```bash
sudo apt-get install mosquitto-clients

# prepare credentials
SERVER="<your server>"

# start listener
mosquitto_sub -h $SERVER -d -t smarthome/#

# send single message
mosquitto_pub -h $SERVER -d -t smarthome/test -m "test_$(date)"

# just as info: clear retained messages
mosquitto_pub -h $SERVER -d -t smarthome/test -n -r -d
```


## Maintainer & License

MIT © [Raul Rosenlöcher](https://github.com/rosenloecher-it)

The code is available at [GitHub][home].

[home]: https://github.com/rosenloecher-it/worker-bunch
