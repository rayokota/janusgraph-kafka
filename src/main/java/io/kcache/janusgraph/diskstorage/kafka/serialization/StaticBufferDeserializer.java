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

import org.apache.kafka.common.serialization.Deserializer;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;

import java.util.Map;

public class StaticBufferDeserializer implements Deserializer<StaticBuffer> {

    public StaticBufferDeserializer() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    public StaticBuffer deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        return new StaticArrayBuffer(data);
    }

    @Override
    public void close() {
    }
}
