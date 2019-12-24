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

import io.kcache.janusgraph.KafkaStorageSetup;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.KeyColumnValueStoreTest;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class KafkaVariableLengthKCVSTest extends KeyColumnValueStoreTest {

    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        KafkaStoreManager sm = new KafkaStoreManager(KafkaStorageSetup.getKafkaConfiguration());
        return new OrderedKeyValueStoreManagerAdapter(sm);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (null != this.manager) {
            this.manager.clearStorage();
        }
        super.tearDown();
    }

    @Test
    @Override
    public void testClearStorage() {

    }

    @Test
    @Override
    public void testConcurrentGetSlice() {

    }

    @Test @Override
    public void testConcurrentGetSliceAndMutate() {

    }
}
