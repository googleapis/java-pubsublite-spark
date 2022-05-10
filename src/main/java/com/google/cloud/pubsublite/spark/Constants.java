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

import com.google.cloud.pubsublite.internal.wire.PubsubContext;

public class Constants {
  public static final String NAME = "pubsublite";
  public static final PubsubContext.Framework FRAMEWORK = PubsubContext.Framework.of("SPARK");

  public static long DEFAULT_BYTES_OUTSTANDING = 50_000_000;
  public static long DEFAULT_MESSAGES_OUTSTANDING = Long.MAX_VALUE;
  public static long DEFAULT_MAX_MESSAGES_PER_BATCH = Long.MAX_VALUE;

  public static String MAX_MESSAGE_PER_BATCH_CONFIG_KEY =
      "pubsublite.flowcontrol.maxmessagesperbatch";
  public static String BYTES_OUTSTANDING_CONFIG_KEY =
      "pubsublite.flowcontrol.byteoutstandingperpartition";
  public static String MESSAGES_OUTSTANDING_CONFIG_KEY =
      "pubsublite.flowcontrol.messageoutstandingperparition";
  public static String TOPIC_CONFIG_KEY = "pubsublite.topic";
  public static String SUBSCRIPTION_CONFIG_KEY = "pubsublite.subscription";
  public static String CREDENTIALS_KEY_CONFIG_KEY = "gcp.credentials.key";
}
