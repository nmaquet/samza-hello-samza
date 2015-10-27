* Install Yarn, Kafka, Zookeeper
```
$ bin/grid bootstrap
```

* Check out the Yarn UI at http://localhost:8088

* Build the Samza jobs
```
mvn clean package && rm -rf deploy/samza/ && mkdir -p deploy/samza && tar -xvf ./target/hello-samza-0.9.1-dist.tar.gz -C deploy/samza/
```

* Run the jobs
```
$ ./deploy/samza/bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/smurf-position-writer.properties
```

```
$ ./deploy/samza/bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/smurf-mood-writer.properties
```

```
$ ./deploy/samza/bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/smurf-profiler.properties
```
