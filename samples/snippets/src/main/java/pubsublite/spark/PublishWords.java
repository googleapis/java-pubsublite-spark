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
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class PublishWords {

  private static final String REGION = "REGION";
  private static final String ZONE_ID = "ZONE_ID";
  private static final String TOPIC_ID = "TOPIC_ID";
  private static final String SUBSCRIPTION_ID = "SUBSCRIPTION_ID";
  private static final String PROJECT_ID = "PROJECT_ID";
  private static final String PARTITIONS = "PARTITIONS";

  public static void main(String[] args) throws Exception {

    Map<String, String> env = System.getenv();
    Preconditions.checkArgument(
        env.keySet()
            .containsAll(
                ImmutableList.of(
                    REGION, ZONE_ID, TOPIC_ID, SUBSCRIPTION_ID, PROJECT_ID, PARTITIONS)));

    String cloudRegion = env.get(REGION);
    char zoneId = env.get(ZONE_ID).charAt(0);
    String topicId = env.get(TOPIC_ID);
    String subscriptionId = env.get(SUBSCRIPTION_ID);
    String projectId = env.get(PROJECT_ID);
    int partitions = Integer.parseInt(env.get(PARTITIONS));

    String snippets =
        Resources.toString(Resources.getResource("text_snippets.txt"), Charset.defaultCharset());
    snippets =
        snippets
            .replaceAll("[:;,.!]", "")
            .replaceAll("\n", " ")
            .replaceAll("\\s+", " ")
            .toLowerCase();
    List<String> words = Arrays.asList(snippets.split(" "));

    createTopicExample(cloudRegion, zoneId, projectId, topicId, partitions);
    createSubscriptionExample(cloudRegion, zoneId, projectId, topicId, subscriptionId);

    publisherExample(cloudRegion, zoneId, projectId, topicId, words);
  }
}
