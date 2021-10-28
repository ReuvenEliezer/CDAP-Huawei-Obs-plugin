/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
 */

package io.cdap.plugin.huawei.obs.sink;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.format.plugin.AbstractFileSink;
import io.cdap.plugin.format.plugin.AbstractFileSinkConfig;
import io.cdap.plugin.huawei.obs.common.ObsConstants;
import io.cdap.plugin.huawei.obs.connector.ObsConnector;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * {@link ObsBatchSink} that stores the data of the latest run of an adapter in S3.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("Obs")
@Description("Batch sink to use Huawei Obs as a sink.")
public class ObsBatchSink extends AbstractFileSink<ObsBatchSink.ObsBatchSinkConfig> {
//    private static final String ENCRYPTION_VALUE = "AES256";
//    private static final String S3A_ENCRYPTION = "fs.s3a.server-side-encryption-algorithm"; //TODO fix
    private static final String ACCESS_CREDENTIALS = "Access Credentials";

    private final ObsBatchSinkConfig config;

    public ObsBatchSink(ObsBatchSinkConfig config) {
        super(config);
        this.config = config;
    }

    @Override
    protected Map<String, String> getFileSystemProperties(BatchSinkContext context) {
        Map<String, String> properties = new HashMap<>(config.getFilesystemProperties());

        if (ACCESS_CREDENTIALS.equalsIgnoreCase(config.authenticationMethod)) {
            if (config.path.startsWith("obs://")) {
                properties.put(ObsConstants.OBS_SECRET_KEY, config.secretKey);
                properties.put(ObsConstants.OBS_ACCESS_KEY, config.accessKey);
                properties.put(ObsConstants.OBS_END_POINT, config.endPoint);
//      properties.put("fs.obs.impl", "org.apache.hadoop.fs.obs.OBSFileSystem");

            }  //TODO fix
        }

//    if (config.shouldEnableEncryption()) {
//      if (config.path.startsWith("obs://")) {
//        properties.put(S3A_ENCRYPTION, ENCRYPTION_VALUE);
//      }  //TODO fix
//    }
        return properties;
    }

    @Override
    protected void recordLineage(LineageRecorder lineageRecorder, List<String> outputFields) {
        lineageRecorder.recordWrite("Write", "Wrote to Obs.", outputFields);
    }

    @VisibleForTesting
    ObsBatchSinkConfig getConfig() {
        return config;
    }

    /**
     * S3 Sink configuration.
     */
    @SuppressWarnings("unused")
    public static class ObsBatchSinkConfig extends AbstractFileSinkConfig {
        private static final String NAME_SECRET_KEY = "secretKey";
        private static final String NAME_END_POINT = "endPoint";
        private static final String NAME_ACCESS_KEY = "accessKey";
        private static final String NAME_PATH = "path";
        private static final String NAME_AUTH_METHOD = "authenticationMethod";
        private static final String NAME_FILE_SYSTEM_PROPERTIES = "fileSystemProperties";

        private static final Gson GSON = new Gson();
        private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() {
        }.getType();

        @Macro
        @Description("The Obs path where the data is stored. Example: 'obs://logs' for OBSFileSystem.")
        private String path; //TODO fix

        @Macro
        @Nullable
        @Description("Access Key of the Huawei Obs instance to connect to.")
        private String accessKey;

        @Macro
        @Nullable
        @Description("Secret KeyID of the Huawei Obs instance to connect to.")
        private String secretKey;

        @Macro
        @Nullable
        @Description("End-Point to be used by the Obs Client.")
        private String endPoint;


        @Macro
        @Nullable
        @Description("Authentication method to access Obs. " +
                "Defaults to Access Credentials. URI scheme should be obs://.")
        private String authenticationMethod;

        @Macro
        @Nullable
        @Description("Server side encryption. Defaults to True. " +
                "Sole supported algorithm is AES256.")
        private Boolean enableEncryption;

        @Macro
        @Nullable
        @Description("Any additional properties to use when reading from the filesystem. "
                + "This is an advanced feature that requires knowledge of the properties supported by the underlying filesystem.")
        private String fileSystemProperties;

        ObsBatchSinkConfig() {
            // Set default value for Nullable properties.
            this.enableEncryption = false;
            this.authenticationMethod = ACCESS_CREDENTIALS;
            this.fileSystemProperties = GSON.toJson(Collections.emptyMap());
        }

        public void validate() {
            // no-op
        }

        @Override
        public void validate(FailureCollector collector) {
            super.validate(collector);
            if (ACCESS_CREDENTIALS.equalsIgnoreCase(authenticationMethod)) {
                if (!containsMacro(NAME_SECRET_KEY) && (secretKey == null || secretKey.isEmpty())) {
                    collector.addFailure("The Secret Key must be specified if authentication method is Access Credentials.", null)
                            .withConfigProperty(NAME_SECRET_KEY).withConfigProperty(NAME_AUTH_METHOD);
                }
                if (!containsMacro(NAME_ACCESS_KEY) && (accessKey == null || accessKey.isEmpty())) {
                    collector.addFailure("The Access Key must be specified if authentication method is Access Credentials.", null)
                            .withConfigProperty(NAME_ACCESS_KEY).withConfigProperty(NAME_AUTH_METHOD);
                }
                if (!containsMacro(NAME_END_POINT) && (endPoint == null || endPoint.isEmpty())) {
                    collector.addFailure("The End Point must be specified if authentication method is Access Credentials.", null)
                            .withConfigProperty(NAME_END_POINT).withConfigProperty(NAME_AUTH_METHOD);
                }
            }

            if (!containsMacro(NAME_PATH) && !path.startsWith("obs://")) {
                collector.addFailure("Path must start with obs://.", null).withConfigProperty(NAME_PATH);
            }

            if (!containsMacro(NAME_FILE_SYSTEM_PROPERTIES)) {
                try {
                    getFilesystemProperties();
                } catch (Exception e) {
                    collector.addFailure("File system properties must be a valid json.", null)
                            .withConfigProperty(NAME_FILE_SYSTEM_PROPERTIES).withStacktrace(e.getStackTrace());
                }
            }
        }

        @Override
        public String getPath() {
            return path;
        }

        boolean shouldEnableEncryption() {
            return enableEncryption;
        }

        Map<String, String> getFilesystemProperties() {
            Map<String, String> properties = new HashMap<>();
            if (containsMacro(NAME_FILE_SYSTEM_PROPERTIES)) {
                return properties;
            }
            return GSON.fromJson(fileSystemProperties, MAP_STRING_STRING_TYPE);
        }
    }
}
