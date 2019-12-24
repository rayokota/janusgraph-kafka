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

package io.kcache.janusgraph;

import io.kcache.janusgraph.diskstorage.kafka.KafkaConfigOptions;
import org.janusgraph.StorageSetup;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

public class KafkaStorageSetup extends StorageSetup {


    public static ModifiableConfiguration getKafkaConfiguration() {
        return getKafkaConfiguration("janusgraph-test-kafka");
    }

    public static ModifiableConfiguration getKafkaConfiguration(final String graphName) {
        return buildGraphConfiguration()
                .set(STORAGE_BACKEND,"io.kcache.janusgraph.diskstorage.kafka.KafkaStoreManager")
                .set(KafkaConfigOptions.BOOTSTRAP_SERVERS, "localhost:9092")
                .set(KafkaConfigOptions.TOPIC_PREFIX, graphName)
                .set(KafkaConfigOptions.ROCKSDB_ENABLE, false)
                .set(DROP_ON_CLEAR, false);
    }

    public static WriteConfiguration getKafkaGraphConfiguration() {
        return getKafkaConfiguration().getConfiguration();
    }
}
