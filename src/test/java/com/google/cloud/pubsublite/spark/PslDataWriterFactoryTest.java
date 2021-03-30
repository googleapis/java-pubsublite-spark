/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.pubsublite.spark;


import com.google.cloud.pubsublite.internal.testing.UnitTestExamples;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class PslDataWriterFactoryTest {

    @Test
    public void testSerializable() throws Exception {
        PslDataWriterFactory obj = new PslDataWriterFactory(
                Constants.DEFAULT_SCHEMA,
                UnitTestExamples.exampleTopicPath(),
                (t) -> null);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(obj);
        oos.flush();
        byte[] data = bos.toByteArray();

        PslDataWriterFactory obj2;
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        ObjectInput in = new ObjectInputStream(bis);
        obj2 = (PslDataWriterFactory) in.readObject();
        obj2.createDataWriter(1, 1, 1);
    }
}