//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.kcache.janusgraph.diskstorage.kafka;

import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;

@PreInitializeConfigOptions
public interface KafkaConfigOptions {

    ConfigNamespace KAFKA_NS = new ConfigNamespace(
        GraphDatabaseConfiguration.STORAGE_NS,
        "kafka",
        "Kafka storage backend options");

    ConfigOption<String> BOOTSTRAP_SERVERS = new ConfigOption<>(
        KAFKA_NS,
        "bootstrap-servers",
        "A list of Kafka brokers to connect to.  Overrides the value in the KCache configuration file.",
        ConfigOption.Type.LOCAL,
        String.class);

    ConfigOption<String> TOPIC_PREFIX = new ConfigOption<>(
        KAFKA_NS,
        "topic-prefix",
        "The prefix to be used for topic names.  Defaults to the graph name.",
        ConfigOption.Type.LOCAL,
        String.class);
}
