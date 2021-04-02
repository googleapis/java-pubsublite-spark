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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkStucts {

  public static ArrayType ATTRIBUTES_PER_KEY_DATATYPE =
      DataTypes.createArrayType(DataTypes.BinaryType);
  public static MapType ATTRIBUTES_DATATYPE =
      DataTypes.createMapType(DataTypes.StringType, ATTRIBUTES_PER_KEY_DATATYPE);
  public static Map<String, DataType> PUBLISH_FIELD_TYPES =
      ImmutableMap.of(
          "key", DataTypes.BinaryType,
          "data", DataTypes.BinaryType,
          "attributes", ATTRIBUTES_DATATYPE,
          "event_timestamp", DataTypes.TimestampType);
  public static StructType DEFAULT_SCHEMA =
      new StructType(
          new StructField[] {
            new StructField("subscription", DataTypes.StringType, false, Metadata.empty()),
            new StructField("partition", DataTypes.LongType, false, Metadata.empty()),
            new StructField("offset", DataTypes.LongType, false, Metadata.empty()),
            new StructField("key", PUBLISH_FIELD_TYPES.get("key"), false, Metadata.empty()),
            new StructField("data", PUBLISH_FIELD_TYPES.get("data"), false, Metadata.empty()),
            new StructField("publish_timestamp", DataTypes.TimestampType, false, Metadata.empty()),
            new StructField(
                "event_timestamp",
                PUBLISH_FIELD_TYPES.get("event_timestamp"),
                true,
                Metadata.empty()),
            new StructField(
                "attributes", PUBLISH_FIELD_TYPES.get("attributes"), true, Metadata.empty())
          });
}
