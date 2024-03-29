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

import static org.junit.Assert.assertThrows;

import com.google.api.gax.rpc.ApiException;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.Test;

public class PslWriteDataSourceOptionsTest {

  @Test
  public void testInvalidTopicPath() {
    Map<String, String> options = ImmutableMap.of(Constants.TOPIC_CONFIG_KEY, "invalid/path");
    assertThrows(ApiException.class, () -> PslWriteDataSourceOptions.fromProperties(options));
  }
}
