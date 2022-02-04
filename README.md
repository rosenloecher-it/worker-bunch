# Worker-Bunch

... is a tasks/jobs/rules engine, primarily intended for use in a smarthome environment.

*Worker-Bunch* provides a programming infrastructure for creating tasks/jobs/rules with proprietary functionality.
These tasks/jobs/rules are called "workers" here. Each worker runs as a separate thread.

The worker base class is supposed to get overwritten. The most functionality goes into 2 functions with limited scope:
`subscribe_notifications` and `_work`. See
[dummy_worker.py](https://github.com/rosenloecher-it/worker-bunch/blob/master/app/dummy_worker.py) and
[main.py](https://github.com/rosenloecher-it/worker-bunch/blob/master/app/main.py).

The following infrastructure parts are already implemented:
- Starting and stopping the workers
- Logging
- Configuration and validation of configuration file (extendable for your job configuration; JSON schema based)
- Subscriptions to timer and cron events.
- Subscriptions to MQTT topics and publish MQTT messages. MQTT messages get debounced (configurable time span).
- Command line arguments

Other characteristics:
- Runs as Linux service.
- Additional prepacked is a Postgres and MQTT client.
  This is a quite opinionated decision due to the special lifecycle of the MQTT client (among others).
- Ready to use is a database worker, which is fully configurable (cron, sql statements, sql scripts, text replacements).
  See [database_worker](https://github.com/rosenloecher-it/worker-bunch/blob/master/worker_bunch/database/database_worker.py).


## Usage

Have a look at  [Worker-Bunch-Sample](https://github.com/rosenloecher-it/worker-bunch-sample)


## Maintainer & License

MIT © [Raul Rosenlöcher](https://github.com/rosenloecher-it)

The code is available at [GitHub][home].

[home]: https://github.com/rosenloecher-it/worker-bunch
