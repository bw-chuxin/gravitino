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

import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;

public interface OpenApiConfig {
  String OPENAPI_CONFIG_PREFIX = "gravitino.authenticator.openapi.";

  ConfigEntry<String> API_KEY =
      new ConfigBuilder(OPENAPI_CONFIG_PREFIX + "api-key")
          .doc("The audience name when Gravitino uses OAuth as the authenticator")
          .version(ConfigConstants.VERSION_0_3_0)
          .stringConf();
}
