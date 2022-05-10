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

import org.apache.spark.sql.connector.read.InputPartition;

public class PslContinuousInputPartition implements InputPartition {
  final SparkPartitionOffset startOffset;
  final PslReadDataSourceOptions options;

  public PslContinuousInputPartition(
      SparkPartitionOffset startOffset,
      PslReadDataSourceOptions options) {
    this.startOffset = startOffset;
    this.options = options;
  }
}
