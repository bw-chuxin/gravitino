/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.server.authentication;

import java.nio.charset.StandardCharsets;
import java.security.Principal;
import org.apache.gravitino.Config;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.AuthConstants;

public class OpenApiAuthenticator implements Authenticator {
  private final Principal ANONYMOUS_PRINCIPAL = new UserPrincipal(AuthConstants.ANONYMOUS_USER);

  private String apiKey;

  @Override
  public void initialize(Config config) throws RuntimeException {
    this.apiKey = config.get(OpenApiConfig.API_KEY);
  }

  @Override
  public boolean supportsToken(byte[] tokenData) {
    return tokenData != null
        && new String(tokenData, StandardCharsets.UTF_8)
            .startsWith(AuthConstants.AUTHORIZATION_BEARER_HEADER);
  }

  @Override
  public boolean isDataFromToken() {
    return true;
  }

  @Override
  public Principal authenticateToken(byte[] tokenData) {
    String token = new String(tokenData, StandardCharsets.UTF_8);
    if ((AuthConstants.AUTHORIZATION_BEARER_HEADER + this.apiKey).equals(token)) {
      return ANONYMOUS_PRINCIPAL;
    } else {
      return null;
    }
  }
}
