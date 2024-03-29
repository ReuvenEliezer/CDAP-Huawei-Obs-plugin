/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.plugin.huawei.obs.common;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.huawei.obs.source.ObsBatchSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;

/**
 * Obs connector config which contains the credential related information
 */
public class ObsConnectorConfig extends PluginConfig {
    private static final Logger logger = LogManager.getLogger(ObsConnectorConfig.class);

    public static final String ACCESS_CREDENTIALS = "Access Credentials";
    public static final String NAME_ACCESS_KEY = "accessKey";
    public static final String NAME_SECRET_KEY = "secretKey";
    public static final String NAME_END_POINT = "endPoint";
    public static final String NAME_AUTH_METHOD = "authenticationMethod";

    @Macro
    @Nullable
    @Description("Access Key of the Huawei Obs instance to connect to.")
    private String accessKey;

    @Macro
    @Nullable
    @Description("Secret Key of the Huawei Obs instance to connect to.")
    private String secretKey;

    @Macro
    @Nullable
    @Description("Authentication method to access Obs. Defaults to Access Credentials.")
    private String authenticationMethod;


    @Macro
    @Nullable
    @Description("End-Point to be used by the Obs Client.")
    private String endPoint;


    public ObsConnectorConfig() {
        authenticationMethod = ACCESS_CREDENTIALS;
    }

    public ObsConnectorConfig(@Nullable String accessKey, @Nullable String secretKey, @Nullable String authenticationMethod, @Nullable String endPoint) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.authenticationMethod = authenticationMethod;
        this.endPoint = endPoint;
    }

    @Nullable
    public String getAccessKey() {
        return accessKey;
    }

    @Nullable
    public String getSecretKey() {
        return secretKey;
    }

    @Nullable
    public String getAuthenticationMethod() {
        return authenticationMethod;
    }

    @Nullable
    public String getEndPoint() {
        return endPoint;
    }


    public boolean isAccessCredentials() {
        return ACCESS_CREDENTIALS.equalsIgnoreCase(authenticationMethod);
    }

    public void validate(FailureCollector collector) {
        if (containsMacro(NAME_AUTH_METHOD)) {
            return;
        }

        if (!ACCESS_CREDENTIALS.equals(authenticationMethod)) {
            return;
        }

        if (!containsMacro("secretKey") && (secretKey == null || secretKey.isEmpty())) {
            collector.addFailure("The Secret Key must be specified if authentication method is Access Credentials.", null)
                    .withConfigProperty(NAME_SECRET_KEY).withConfigProperty(NAME_AUTH_METHOD);
        }
        if (!containsMacro("accessKey") && (accessKey == null || accessKey.isEmpty())) {
            collector.addFailure("The Access Key must be specified if authentication method is Access Credentials.", null)
                    .withConfigProperty(NAME_ACCESS_KEY).withConfigProperty(NAME_AUTH_METHOD);
        }
        if (!containsMacro("endPoint") && (endPoint == null || endPoint.isEmpty())) {
            collector.addFailure("The End Point must be specified if authentication method is Access Credentials.", null)
                    .withConfigProperty(NAME_END_POINT).withConfigProperty(NAME_AUTH_METHOD);
        }
    }
}
