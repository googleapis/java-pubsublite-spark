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

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;

public class CommonUtils {

  public static Map<String, String> getAndValidateEnvVars(String... keys) {
    Map<String, String> env = System.getenv();
    Set<String> missingVars = Sets.difference(Sets.newHashSet(keys), env.keySet());
    Preconditions.checkState(
        missingVars.isEmpty(), "Missing required environment variables: " + missingVars);
    return env;
  }
}
