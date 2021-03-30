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

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkStructs {
  public static ArrayType ATTRIBUTES_PER_KEY_DATATYPE =
      DataTypes.createArrayType(DataTypes.BinaryType);
  public static MapType ATTRIBUTES_DATATYPE =
      DataTypes.createMapType(DataTypes.StringType, ATTRIBUTES_PER_KEY_DATATYPE);
  public static StructType DEFAULT_SCHEMA =
      new StructType(
          new StructField[] {
            new StructField("subscription", DataTypes.StringType, false, Metadata.empty()),
            new StructField("partition", DataTypes.LongType, false, Metadata.empty()),
            new StructField("offset", DataTypes.LongType, false, Metadata.empty()),
            new StructField("key", DataTypes.BinaryType, false, Metadata.empty()),
            new StructField("data", DataTypes.BinaryType, false, Metadata.empty()),
            new StructField("publish_timestamp", DataTypes.TimestampType, false, Metadata.empty()),
            new StructField("event_timestamp", DataTypes.TimestampType, true, Metadata.empty()),
            new StructField("attributes", ATTRIBUTES_DATATYPE, true, Metadata.empty())
          });
}
