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

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.common.AbstractStoreManager;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.kcache.janusgraph.diskstorage.kafka.KafkaConfigOptions.TOPIC_PREFIX;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;

public class KafkaStoreManager extends AbstractStoreManager implements OrderedKeyValueStoreManager {

    private static final Logger log = LoggerFactory.getLogger(KafkaStoreManager.class);

    private final Map<String, KafkaKeyValueStore> stores;

    protected final StoreFeatures features;

    public KafkaStoreManager(Configuration configuration) throws BackendException {
        super(configuration);
        stores = new ConcurrentHashMap<>();

        features = new StandardStoreFeatures.Builder()
                    .orderedScan(true)
                    .transactional(false)
                    .keyConsistent(GraphDatabaseConfiguration.buildGraphConfiguration())
                    .locking(true)
                    .keyOrdered(true)
                    .supportsInterruption(false)
                    .optimisticLocking(true)
                    .multiQuery(false)
                    .build();
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public StoreTransaction beginTransaction(final BaseTransactionConfig txCfg) throws BackendException {
        return new KafkaTx(txCfg);
    }

    @Override
    public KafkaKeyValueStore openDatabase(String name) throws BackendException {
        Preconditions.checkNotNull(name);
        if (stores.containsKey(name)) {
            return stores.get(name);
        }
        try {
            KafkaKeyValueStore store = new KafkaKeyValueStore(name, this);
            stores.put(name, store);
            return store;
        } catch (Exception e) {
            throw new PermanentBackendException("Could not open Kafka store", e);
        }
    }

    @Override
    public void mutateMany(Map<String, KVMutation> mutations, StoreTransaction txh) throws BackendException {
        for (Map.Entry<String,KVMutation> mutation : mutations.entrySet()) {
            KafkaKeyValueStore store = openDatabase(mutation.getKey());
            KVMutation mutationValue = mutation.getValue();

            if (!mutationValue.hasAdditions() && !mutationValue.hasDeletions()) {
                log.debug("Empty mutation set for {}, doing nothing", mutation.getKey());
            } else {
                log.debug("Mutating {}", mutation.getKey());
            }

            if (mutationValue.hasAdditions()) {
                for (KeyValueEntry entry : mutationValue.getAdditions()) {
                    store.insert(entry.getKey(),entry.getValue(),txh);
                    log.trace("Insertion on {}: {}", mutation.getKey(), entry);
                }
            }
            if (mutationValue.hasDeletions()) {
                for (StaticBuffer del : mutationValue.getDeletions()) {
                    store.delete(del,txh);
                    log.trace("Deletion on {}: {}", mutation.getKey(), del);
                }
            }
        }
    }

    @Override
    public void close() throws BackendException {
        for (KafkaKeyValueStore store : stores.values()) {
            store.shutDown();
        }
        stores.clear();
    }

    @Override
    public void clearStorage() throws BackendException {
        for (KafkaKeyValueStore store : stores.values()) {
            store.clear();
        }
    }

    @Override
    public boolean exists() throws BackendException {
        return !stores.isEmpty();
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    public String getTopicPrefix() {
        Configuration config = getStorageConfig();
        if (!config.has(TOPIC_PREFIX) && (config.has(GRAPH_NAME))) return config.get(GRAPH_NAME) + "_";
        return config.get(TOPIC_PREFIX);
    }
}
