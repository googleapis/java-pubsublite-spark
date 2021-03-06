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

import static pubsublite.spark.AdminUtils.subscriberExample;

import java.util.Map;

public class ReadResults {

  private static final String REGION = "REGION";
  private static final String ZONE_ID = "ZONE_ID";
  private static final String DESTINATION_SUBSCRIPTION_ID = "DESTINATION_SUBSCRIPTION_ID";
  private static final String PROJECT_NUMBER = "PROJECT_NUMBER";

  public static void main(String[] args) {
    Map<String, String> env =
        CommonUtils.getAndValidateEnvVars(
            REGION, ZONE_ID, DESTINATION_SUBSCRIPTION_ID, PROJECT_NUMBER);

    String cloudRegion = env.get(REGION);
    char zoneId = env.get(ZONE_ID).charAt(0);
    String destinationSubscriptionId = env.get(DESTINATION_SUBSCRIPTION_ID);
    long projectNumber = Long.parseLong(env.get(PROJECT_NUMBER));

    System.out.println("Results from Pub/Sub Lite:");
    subscriberExample(cloudRegion, zoneId, projectNumber, destinationSubscriptionId)
        .forEach((m) -> System.out.println(m.getData().toStringUtf8().replace("_", ": ")));

    System.exit(0);
  }
}
