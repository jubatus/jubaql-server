JubaQL
======

How to get started with JubaQL
------------------------------

### Development Setup

* Get a Hadoop-enabled version of Spark 1.1.1:  
  `wget http://d3kbcqa49mib13.cloudfront.net/spark-1.1.1-bin-hadoop2.4.tgz`  
  and unpack it somewhere:  
  `tar -xzf spark-1.1.1-bin-hadoop2.4.tgz && export SPARK_DIST="$(pwd)/spark-1.1.1-bin-hadoop2.4/"`
* Install Jubatus.
* Get JubaQLClient and JubaQLServer (consists of JubaQLProcessor and JubaQLGateway):  
  `git clone https://github.com/jubatus/jubaql-client.git`  
  `git clone https://github.com/jubatus/jubaql-server.git`
* Build the JubaQL components:
    * JubaQLClient:  
      `cd jubaql-client && sbt start-script && cd ..`
    * JubaQLProcessor:  
      `cd jubaql-server/processor && sbt assembly && cd ../..`
    * JubaQLGateway:  
      `cd jubaql-server/gateway && sbt assembly && cd ../..`
* Start the JubaQLGateway:  
  `cd jubaql-server && java -Dspark.distribution="$SPARK_DIST" -Djubaql.processor.fatjar=processor/target/scala-2.10/jubaql-processor-assembly-1.2.0.jar -jar gateway/target/scala-2.10/jubaql-gateway-assembly-1.2.0.jar -i 127.0.0.1`
* In a different shell, start the JubaQLClient:  
  `./jubaql-client/target/start`
* You will see the prompt `jubaql>` in the shell and you will in fact be able to type your commands there, but until the JubaQLProcessor is up and running correctly, you will get the message "Unexpected response status: 503".

In order to test that your setup is working correctly, you can do a simple classification using the data from the [shogun example](https://github.com/jubatus/jubatus-example/tree/master/shogun). Run the following JubaQL commands in the client:

* `CREATE CLASSIFIER MODEL test WITH (label: "label", datum: "name") config = '{"method": "AROW","converter": {  "num_filter_types": {},  "num_filter_rules": [],  "string_filter_types": {},   "string_filter_rules": [],    "num_types": {},  "num_rules": [],"string_types": {"unigram": { "method": "ngram", "char_num": "1" }},"string_rules": [{ "key": "*", "type": "unigram", "sample_weight": "bin", "global_weight": "bin" } ]},"parameter": {"regularization_weight" : 1.0}}'`
* `CREATE DATASOURCE shogun (label string, name string) FROM (STORAGE: "file://data/shogun_data.json")`
* `UPDATE MODEL test USING train FROM shogun`
* `ANALYZE '{"name": "慶喜"}' BY MODEL test USING classify`
* `SHUTDOWN`

The JSON returned by the `ANALYZE` statement should indicate that the label "徳川" has the highest score.

### Run on YARN with local gateway

* Set up a Hadoop cluster with YARN and HDFS in place.
* Install Jubatus on all cluster nodes.
* Get JubaQL and compile it as described above. (This time, Jubatus is not required locally.)
* Install the [Jubatus on YARN](https://github.com/jubatus/jubatus-on-yarn) libraries in HDFS as described in [the instructions](https://github.com/jubatus/jubatus-on-yarn/blob/master/document/%E3%83%93%E3%83%AB%E3%83%89%E3%83%BB%E5%88%A9%E7%94%A8%E6%89%8B%E9%A0%86%E6%9B%B8.md#%E5%AE%9F%E8%A1%8C%E3%81%AB%E5%BF%85%E8%A6%81%E3%81%AA%E3%83%95%E3%82%A1%E3%82%A4%E3%83%AB%E3%81%AE%E6%BA%96%E5%82%99). Make sure that the HDFS directory `/jubatus-on-yarn/application-master/jubaconfig/` exists and is writeable by the user running the JubaQLProcessor application.
* To test the setup, also copy the file `shogun-data.json` from the JubaQL source tree's `data/` directory to `/jubatus-on-yarn/sample/shogun_data.json` in HDFS.
* Copy the files `core-site.xml`, `yarn-site.xml`, `hdfs-site.xml` containing your Hadoop setup description from one of your cluster nodes to some directory and point the environment variable `HADOOP_CONF_DIR` to that directory.
* Get your local computer's IP address that points towards the cluster. On Linux, given the IP address of one of your cluster nodes, this should be possible with something like:  
  `export MY_IP=$(ip route get 12.34.56.78 | grep -Po 'src \K.+')`  
  Make sure that this IP address can be connected to from the cluster nodes and no firewall rules etc. are blocking access.
* Get the addresses of your Zookeeper nodes and concatenate their `host:port` locations with a comma:  
  `export MY_ZOOKEEPER=zk1:2181,zk2:2181`
* Start the JubaQLGateway:  
  `cd jubaql-server`
  `java -Drun.mode=production -Djubaql.zookeeper=$MY_ZOOKEEPER -Dspark.distribution="$SPARK_DIST" -Djubaql.processor.fatjar=processor/target/scala-2.10/jubaql-processor-assembly-1.2.0.jar -jar gateway/target/scala-2.10/jubaql-gateway-assembly-1.2.0.jar -i $MY_IP`
* In a different shell, start the JubaQLClient:  
  `./jubaql-client/target/start`
* You will see the prompt `jubaql>` in the shell and you will in fact be able to type your commands there, but until the JubaQLProcessor is up and running correctly, you will get the message "Unexpected response status: 503".

In order to test that your setup is working correctly, you can do a simple classification using the `shogun-data.json` file you copied to HDFS before. Run the following JubaQL commands in the client:

* `CREATE CLASSIFIER MODEL test WITH (label: "label", datum: "name") config = '{"method": "AROW","converter": {  "num_filter_types": {},  "num_filter_rules": [],  "string_filter_types": {},   "string_filter_rules": [],    "num_types": {},  "num_rules": [],"string_types": {"unigram": { "method": "ngram", "char_num": "1" }},"string_rules": [{ "key": "*", "type": "unigram", "sample_weight": "bin", "global_weight": "bin" } ]},"parameter": {"regularization_weight" : 1.0}}'`
* `CREATE DATASOURCE shogun (label string, name string) FROM (STORAGE: "hdfs:///jubatus-on-yarn/sample/shogun_data.json")`
* `UPDATE MODEL test USING train FROM shogun`
* `ANALYZE '{"name": "慶喜"}' BY MODEL test USING classify`
* `SHUTDOWN`

The JSON returned by the `ANALYZE` statement should indicate that the label "徳川" has the highest score.

Note:
* When the JubaQLProcessor is started using `spark-submit` as outlined above, it will first upload the `spark-assembly-1.1.1-hadoop2.4.0.jar` and `jubaql-processor-assembly-1.2.0.jar` to the cluster and add them to HDFS, from where they will be downloaded by each executor.
* It is possible to skip the upload of the Spark libraries by copying the Spark jar file to HDFS manually and adding the parameter `-Dspark.yarn.jar=hdfs:///path/to/spark-assembly-1.1.1-hadoop2.4.0.jar` when starting the JubaQLGateway.
* In theory, it is also possible to do the same for the JubaQLProcessor application jar file. However, at the moment we rely on extracting a `log4j.xml` file from that jar locally before upload, so there is no support for also storing that file in HDFS, yet.

### Run on YARN with remote gateway

In general, this setup is very similar to the setup in the previous section. The only difference is that the execution of the gateway takes place on a remote host. Therefore, the jar files for JubaQLProcessor and JubaQLGateway as well as the Hadoop configuration files must be copied there and the JubaQLGateway started there. Also, pass the `-h hostname` parameter to the JubaQLClient to connect to the remote server.
