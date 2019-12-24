package io.kcache.janusgraph.diskstorage.kafka;

import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;

public class KafkaTx extends AbstractStoreTransaction {

    public KafkaTx(BaseTransactionConfig config) {
        super(config);
    }
}
