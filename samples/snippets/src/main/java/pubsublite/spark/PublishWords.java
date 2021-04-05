/*
 * Copyright 2021 Google LLC
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

package pubsublite.spark;

import static pubsublite.spark.AdminUtils.createSubscriptionExample;
import static pubsublite.spark.AdminUtils.createTopicExample;
import static pubsublite.spark.AdminUtils.publisherExample;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PublishWords {

  private static final String REGION = "REGION";
  private static final String ZONE_ID = "ZONE_ID";
  private static final String SOURCE_TOPIC_ID = "SOURCE_TOPIC_ID";
  private static final String SOURCE_SUBSCRIPTION_ID = "SOURCE_SUBSCRIPTION_ID";
  private static final String DESTINATION_TOPIC_ID = "DESTINATION_TOPIC_ID";
  private static final String DESTINATION_SUBSCRIPTION_ID = "DESTINATION_SUBSCRIPTION_ID";
  private static final String PROJECT_NUMBER = "PROJECT_NUMBER";

  public static void main(String[] args) throws Exception {

    Map<String, String> env = System.getenv();
    Set<String> missingVars =
        Sets.difference(
            ImmutableSet.of(
                REGION,
                ZONE_ID,
                SOURCE_TOPIC_ID,
                SOURCE_SUBSCRIPTION_ID,
                DESTINATION_TOPIC_ID,
                DESTINATION_SUBSCRIPTION_ID,
                PROJECT_NUMBER),
            env.keySet());
    Preconditions.checkState(
        missingVars.isEmpty(), "Missing required environment variables: " + missingVars);

    final String cloudRegion = env.get(REGION);
    char zoneId = env.get(ZONE_ID).charAt(0);
    final String sourceTopicId = env.get(SOURCE_TOPIC_ID);
    final String sourceSubscriptionId = env.get(SOURCE_SUBSCRIPTION_ID);
    final String destinationTopicId = env.get(DESTINATION_TOPIC_ID);
    final String destinationSubscriptionId = env.get(DESTINATION_SUBSCRIPTION_ID);
    long projectNumber = Long.parseLong(env.get(PROJECT_NUMBER));
    int partitions = 1;

    String snippets =
        Resources.toString(Resources.getResource("text_snippets.txt"), Charset.defaultCharset());
    snippets =
        snippets
            .replaceAll("[:;,.!]", "")
            .replaceAll("\n", " ")
            .replaceAll("\\s+", " ")
            .toLowerCase();
    final List<String> words = Arrays.asList(snippets.split(" "));

    createTopicExample(cloudRegion, zoneId, projectNumber, sourceTopicId, partitions);
    createSubscriptionExample(cloudRegion, zoneId, projectNumber, sourceTopicId, sourceSubscriptionId);
    createTopicExample(cloudRegion, zoneId, projectNumber, destinationTopicId, partitions);
    createSubscriptionExample(
        cloudRegion, zoneId, projectNumber, destinationTopicId, destinationSubscriptionId);

    publisherExample(cloudRegion, zoneId, projectNumber, sourceTopicId, words);

    System.exit(0);
  }
}
