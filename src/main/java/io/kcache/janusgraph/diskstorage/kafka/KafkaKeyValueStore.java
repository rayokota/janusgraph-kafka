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

import com.google.common.primitives.UnsignedBytes;
import io.kcache.Cache;
import io.kcache.KafkaCache;
import io.kcache.KafkaCacheConfig;
import io.kcache.KeyValue;
import io.kcache.KeyValueIterator;
import io.kcache.utils.Caches;
import io.kcache.utils.InMemoryCache;
import io.kcache.utils.rocksdb.RocksDBCache;
import org.apache.kafka.common.serialization.Serdes;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentSkipListMap;

public class KafkaKeyValueStore implements OrderedKeyValueStore {

    private static final Logger log = LoggerFactory.getLogger(KafkaKeyValueStore.class);

    public static final String KCACHE_PROPS_DEFAULT = "./conf/kcache.properties";

    private final String name;
    private final KafkaStoreManager manager;
    private boolean isOpen;
    private final Cache<byte[], byte[]> cache;

    public KafkaKeyValueStore(String name, KafkaStoreManager manager) {
        this.name = name;
        this.manager = manager;
        this.isOpen = true;
        String topicPrefix = manager.getTopicPrefix();
        String topic = topicPrefix + "_" + this.name;
        Map<?, ?> configs = getConfigs(topic);
        Configuration config = manager.getStorageConfig();
        boolean enableRocksDb = config.get(KafkaConfigOptions.ROCKSDB_ENABLE);
        String rootDir = config.get(KafkaConfigOptions.ROCKSDB_ROOT_DIR);
        Comparator<byte[]> cmp = UnsignedBytes.lexicographicalComparator();
        Cache<byte[], byte[]> cache = enableRocksDb
                ? new RocksDBCache<>(topic, "rocksdb", rootDir, Serdes.ByteArray(), Serdes.ByteArray(), cmp)
                : new InMemoryCache<>(new ConcurrentSkipListMap<>(cmp));
        Cache<byte[], byte[]> rows = new KafkaCache<>(
                new KafkaCacheConfig(configs), Serdes.ByteArray(), Serdes.ByteArray(), null, cache);
        this.cache = Caches.concurrentCache(rows);
        this.cache.init();
        log.debug("Initializing Kafka store {}, size={}", getName(), size());
    }

    private Map<?, ?> getConfigs(String topic) {
        String kcacheConfig = KCACHE_PROPS_DEFAULT;
        Configuration config = manager.getStorageConfig();
        if (config.has(GraphDatabaseConfiguration.STORAGE_CONF_FILE)) {
            kcacheConfig = config.get(GraphDatabaseConfiguration.STORAGE_CONF_FILE);
        }
        File file = new File(kcacheConfig);
        Properties configs = file.exists() ? KafkaCacheConfig.getPropsFromFile(kcacheConfig) : new Properties();
        if (config.has(KafkaConfigOptions.BOOTSTRAP_SERVERS)) {
            String bootstrapServers = config.get(KafkaConfigOptions.BOOTSTRAP_SERVERS);
            configs.put(KafkaCacheConfig.KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        }
        String groupId = "kstore-1";
        String clientId = topic + "-" + groupId;
        setConfig(configs, KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, topic);
        setConfig(configs, KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG, groupId);
        setConfig(configs, KafkaCacheConfig.KAFKACACHE_CLIENT_ID_CONFIG, clientId);
        return configs;
    }

    private void setConfig(Properties configs, String key, Object value) {
        if (!configs.containsKey(key)) {
            configs.put(key, value);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    public int size() {
        return cache.size();
    }

    @Override
    public void close() throws BackendException {
        // Don't close cache as tests will next call clear()
        log.debug("Closing Kafka store {}, size={}", getName(), size());
    }

    public void clear() {
        try (KeyValueIterator<byte[], byte[]> iter = cache.all()) {
            while (iter.hasNext()) {
                cache.remove(iter.next().key);
            }
            cache.flush();
        }
    }

    public synchronized void shutDown() throws BackendException {
        log.debug("Shutting down Kafka store {}, size={}", getName(), size());
        try {
            if (isOpen) {
                cache.close();
            }
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        }
        isOpen = false;
    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws BackendException {
        return fromBytes(cache.get(toBytes(key)));
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws BackendException {
        return cache.containsKey(toBytes(key));
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
    }

    @Override
    public RecordIterator<KeyValueEntry> getSlice(KVQuery query, StoreTransaction txh) throws BackendException {
        final List<KeyValueEntry> result = new ArrayList<>();
        final StaticBuffer keyStart = query.getStart();
        final StaticBuffer keyEnd = query.getEnd();
        final KeySelector selector = query.getKeySelector();
        int cmp = keyStart.compareTo(keyEnd);
        if (cmp < 0) {
            try (KeyValueIterator<byte[], byte[]> iter = cache.range(toBytes(keyStart), true, toBytes(keyEnd), false)) {
                while (iter.hasNext()) {
                    KeyValue<byte[], byte[]> keyValue = iter.next();
                    StaticBuffer key = fromBytes(keyValue.key);
                    if (selector.include(key)) {
                        StaticBuffer value = fromBytes(keyValue.value);
                        result.add(new KeyValueEntry(key, value));
                    }
                    if (selector.reachedLimit()) {
                        break;
                    }
                }
            }
        }
        return new KafkaRecordIterator(result);
    }

    private static class KafkaRecordIterator implements RecordIterator<KeyValueEntry> {
        private final Iterator<KeyValueEntry> entries;

        public KafkaRecordIterator(final List<KeyValueEntry> result) {
            this.entries = result.iterator();
        }

        @Override
        public boolean hasNext() {
            return entries.hasNext();
        }

        @Override
        public KeyValueEntry next() {
            return entries.next();
        }

        @Override
        public void close() {
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public Map<KVQuery,RecordIterator<KeyValueEntry>> getSlices(List<KVQuery> queries, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh, final Integer ttl) throws BackendException {
        cache.put(toBytes(key), toBytes(value));
        cache.flush();
    }

    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) throws BackendException {
        cache.remove(toBytes(key));
        cache.flush();
    }

    private static byte[] toBytes(StaticBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        return buffer.as(StaticBuffer.ARRAY_FACTORY);
    }

    private static StaticBuffer fromBytes(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return new StaticArrayBuffer(bytes);
    }
}
