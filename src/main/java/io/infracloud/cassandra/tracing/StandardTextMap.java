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
package io.infracloud.cassandra.tracing;

import io.opentracing.propagation.TextMap;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static io.infracloud.cassandra.tracing.JaegerTracing.JAEGER_TRACE_KEY;


public class StandardTextMap implements TextMap {
    private static final String SECONDARY_KEY = "uber-trace-id";

    private final Map<String, String> map = new HashMap<>();
    private static final Charset charset = Charset.forName("UTF-8");

    private String operationName = "";

    public boolean hasOperationName() {
        return !operationName.equals("");
    }

    public String getOperationName() {
        return operationName;
    }

    public void injectToByteMap(Map<String, byte[]> my_map) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            my_map.put(entry.getKey(), entry.getValue().getBytes(charset));
        }
    }

    protected StandardTextMap() {
    }

    protected StandardTextMap(Map<String, ByteBuffer> custom_payload) {
        for (Map.Entry<String, ByteBuffer> entry : custom_payload.entrySet()) {
            String key = entry.getKey();
            String value = charset.decode(entry.getValue()).toString();
            // this safeguard is since Span adds the operation name in the header
            // which in turn confuses the codec
            if (key.equals(JAEGER_TRACE_KEY) || key.equals(SECONDARY_KEY)) {
                final String[] parts = value.split(" ");
                if (parts.length > 2) {
                    final StringBuilder sb = new StringBuilder();
                    for (int i=2; i<parts.length; i++) {
                        sb.append(parts[i]);
                        sb.append(" ");
                    }
                    sb.deleteCharAt(sb.length()-1);
                    operationName = sb.toString();
                }
                value = parts[0];
            }
            put(key, value);
        }
    }

    /**
     * An alternative constructor. Due to type erasure compatibility I cannot make
     * an overloaded constructor in Java
     */
    static public StandardTextMap from_bytes(Map<String, byte[]> custom_payload) {
        final Map<String, ByteBuffer> my_map = new HashMap<>();
        for (Map.Entry<String, byte[]> entry : custom_payload.entrySet()) {
            my_map.put(entry.getKey(), ByteBuffer.wrap(entry.getValue()));
        }
        return new StandardTextMap(my_map);
    }


    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        return map.entrySet().iterator();
    }

    /**
     * This is for debug use only
     * @return string representation of contents of the text map
     */
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("StandardTextMap<");
        for (Map.Entry<String, String> entry : map.entrySet()) {
            sb.append(entry.getKey());
            sb.append("=");
            sb.append(entry.getValue());
            sb.append(",");
        }
        sb.deleteCharAt(sb.length()-1);
        sb.append(">");
        return sb.toString();
    }

    @Override
    public void put(String s, String s1) {
        map.put(s, s1);
    }
}
