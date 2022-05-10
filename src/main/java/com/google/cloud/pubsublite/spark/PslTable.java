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

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class PslTable implements SupportsRead, SupportsWrite {
  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap properties) {
    return new PslScanBuilder(PslReadDataSourceOptions.fromProperties(properties));
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo logicalWriteInfo) {
    PslSparkUtils.verifyWriteInputSchema(logicalWriteInfo.schema());
    return new PslWrite(logicalWriteInfo.schema(), PslWriteDataSourceOptions.fromProperties(logicalWriteInfo.options()));
  }

  @Override
  public String name() {
    return Constants.NAME;
  }

  @Override
  public StructType schema() {
    return SparkStructs.DEFAULT_SCHEMA;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return ImmutableSet.of(TableCapability.BATCH_WRITE, TableCapability.STREAMING_WRITE, TableCapability.MICRO_BATCH_READ, TableCapability.CONTINUOUS_READ);
  }
}
