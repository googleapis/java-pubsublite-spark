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

package com.google.cloud.pubsublite.spark.internal;

import com.google.api.gax.core.CredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Base64;
import javax.annotation.Nullable;

public class PslCredentialsProvider implements CredentialsProvider {

  private final Credentials credentials;

  public PslCredentialsProvider(@Nullable String credentialsKey) {
    this.credentials =
        credentialsKey != null
            ? createCredentialsFromKey(credentialsKey)
            : createDefaultCredentials();
  }

  private static Credentials createCredentialsFromKey(String key) {
    try {
      return GoogleCredentials.fromStream(new ByteArrayInputStream(Base64.getDecoder().decode(key)))
          .createScoped("https://www.googleapis.com/auth/cloud-platform");
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create Credentials from key", e);
    }
  }

  public static Credentials createDefaultCredentials() {
    try {
      return GoogleCredentials.getApplicationDefault()
          .createScoped("https://www.googleapis.com/auth/cloud-platform");
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create default Credentials", e);
    }
  }

  @Override
  public Credentials getCredentials() {
    return credentials;
  }
}
