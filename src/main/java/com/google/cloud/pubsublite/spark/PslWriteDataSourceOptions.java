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

import com.google.api.gax.rpc.ApiException;
import com.google.auto.value.AutoValue;
import com.google.cloud.pubsublite.TopicPath;
import javax.annotation.Nullable;
import org.apache.spark.sql.sources.v2.DataSourceOptions;

@AutoValue
public abstract class PslWriteDataSourceOptions {

  @Nullable
  public abstract String credentialsKey();

  public abstract TopicPath topicPath();

  public static Builder builder() {
    return new AutoValue_PslWriteDataSourceOptions.Builder().setCredentialsKey(null);
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract PslWriteDataSourceOptions.Builder setCredentialsKey(String credentialsKey);

    public abstract PslWriteDataSourceOptions.Builder setTopicPath(TopicPath topicPath);

    public abstract PslWriteDataSourceOptions build();
  }

  public static PslWriteDataSourceOptions fromSparkDataSourceOptions(DataSourceOptions options) {
    if (!options.get(Constants.TOPIC_CONFIG_KEY).isPresent()) {
      throw new IllegalArgumentException(Constants.TOPIC_CONFIG_KEY + " is required.");
    }

    Builder builder = builder();
    String topicPathVal = options.get(Constants.TOPIC_CONFIG_KEY).get();
    try {
      builder.setTopicPath(TopicPath.parse(topicPathVal));
    } catch (ApiException e) {
      throw new IllegalArgumentException("Unable to parse topic path " + topicPathVal, e);
    }
    options.get(Constants.CREDENTIALS_KEY_CONFIG_KEY).ifPresent(builder::setCredentialsKey);
    return builder.build();
  }

  public PslCredentialsProvider getCredentialProvider() {
    return new PslCredentialsProvider(this);
  }
}
