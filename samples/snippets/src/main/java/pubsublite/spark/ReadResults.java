package pubsublite.spark;

import static pubsublite.spark.AdminUtils.subscriberExample;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;

public class ReadResults {

  private static final String REGION = "REGION";
  private static final String ZONE_ID = "ZONE_ID";
  private static final String SUBSCRIPTION_ID_RESULT = "SUBSCRIPTION_ID_RESULT";
  private static final String PROJECT_NUMBER = "PROJECT_NUMBER";

  public static void main(String[] args) {

    Map<String, String> env = System.getenv();
    Set<String> missingVars =
        Sets.difference(
            ImmutableSet.of(REGION, ZONE_ID, SUBSCRIPTION_ID_RESULT, PROJECT_NUMBER), env.keySet());
    Preconditions.checkState(
        missingVars.isEmpty(), "Missing required environment variables: " + missingVars);

    String cloudRegion = env.get(REGION);
    char zoneId = env.get(ZONE_ID).charAt(0);
    String subscriptionIdResult = env.get(SUBSCRIPTION_ID_RESULT);
    long projectNumber = Long.parseLong(env.get(PROJECT_NUMBER));

    System.out.println("Word count results:");
    subscriberExample(cloudRegion, zoneId, projectNumber, subscriptionIdResult)
        .forEach((m) -> System.out.println(m.getData().toStringUtf8().replace("_", ": ")));

    System.exit(0);
  }
}
