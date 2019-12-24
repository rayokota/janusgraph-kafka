/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kcache.janusgraph.diskstorage.kafka.serialization;

import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StaticBufferSerializerTest {

    private final StaticBufferSerializer serializer = new StaticBufferSerializer();
    private final StaticBufferDeserializer deserializer = new StaticBufferDeserializer();

    @Test
    public void testKafkaAvroSerializer() {
        byte[] array = new byte[]{0, 1, 2, 3};
        StaticBuffer buf;
        byte[] bytes;

        buf = new StaticArrayBuffer(array);
        bytes = serializer.serialize("", buf);
        assertEquals(buf, deserializer.deserialize("", bytes));

        buf = new StaticArrayBuffer(array, 1, 3);
        bytes = serializer.serialize("", buf);
        assertEquals(buf, deserializer.deserialize("", bytes));

        buf = new StaticArrayBuffer(array, 0, 2);
        bytes = serializer.serialize("", buf);
        assertEquals(buf, deserializer.deserialize("", bytes));

        buf = new StaticArrayBuffer(array, 1, 2);
        bytes = serializer.serialize("", buf);
        assertEquals(buf, deserializer.deserialize("", bytes));
    }

    @Test
    public void testKafkaAvroSerializerWithEmptyByteArray() {
        byte[] array = new byte[0];
        StaticBuffer buf;
        byte[] bytes;

        buf = new StaticArrayBuffer(array);
        bytes = serializer.serialize("", buf);
        assertEquals(buf, deserializer.deserialize("", bytes));
    }
}
