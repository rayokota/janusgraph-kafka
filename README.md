# Kafka Storage Adapter for JanusGraph

[JanusGraph](http://janusgraph.org) is an [Apache TinkerPop](http://tinkerpop.apache.org) enabled graph database that supports a variety of storage and indexing backends. This project adds [Kafka](https://kafka.apache.org) to the supported list of backends.

## Installing the adapter from a binary release
Binary releases can be found on [GitHub](http://github.com/rayokota/janusgraph-kafka/releases).

This installation procedure will copy the necessary libraries, properties, and Gremlin Server configuration files into your JanusGraph installation.

1. Download the JanusGraph [release](https://github.com/JanusGraph/janusgraph/releases).
2. Download the Kafka storage adapter release.
3. Unzip the storage adapter zip file and run `./install.sh $YOUR_JANUSGRAPH_INSTALL_DIRECTORY`

Assuming you have a Kafka cluster up and running, you can connect from the Gremlin console by running:

`gremlin> graph = JanusGraphFactory.open('conf/janusgraph-kafka.properties')`

To start Gremlin Server run `gremlin-server.sh` directly or `bin/janusgraph.sh start` which will also start a local Elasticsearch instance.

## Installing from source

Follow these steps if you'd like to use the latest version built from source.
1. Clone the repository.
    `git clone http://github.com/rayokota/janusgraph-kafka`
2. Build the distribution package.
    `mvn package -DskipTests`
3. Follow the binary installation steps starting at step 3.

# Configuration Options

|Property|Description|Default|
|-|-|-|
|`storage.conf-file`|Path to the configuration file for KCache.||
|`storage.kafka.bootstrap-servers`|List of Kafka brokers to connect to.  Overrides the value in the KCache configuration file.||
|`storage.kafka.topic-prefix`|The prefix to be used for topic names.  Defaults to the graph name.||
|`storage.kafka.kafkacache.backing.cache`|The backing cache for KCache, one of `memory`, `bdbje`, `lmdb`, or `rocksdb`.|memory|
|`storage.kafka.kafkacache.data.dir`|Root directory for backing cache storage.|/tmp|
