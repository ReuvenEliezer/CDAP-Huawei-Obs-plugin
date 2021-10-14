/*
 * Copyright © 2015 Cask Data, Inc.
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

package io.cdap.plugin.huawei.obs.source;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.format.input.PathTrackingInputFormat;
import io.cdap.plugin.format.plugin.AbstractFileSource;
import io.cdap.plugin.format.plugin.AbstractFileSourceConfig;
import io.cdap.plugin.huawei.obs.common.ObsConnectorConfig;
import io.cdap.plugin.huawei.obs.common.ObsConstants;
import io.cdap.plugin.huawei.obs.connector.ObsConnector;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * A {@link BatchSource} that reads from Huawei Obs.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(ObsBatchSource.NAME)
@Description("Batch source to use Huawei Obs as a source.")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = ObsConnector.NAME)})
public class ObsBatchSource extends AbstractFileSource<ObsBatchSource.S3BatchConfig> {
    public static final String NAME = "Obs";

    @SuppressWarnings("unused")
    private final S3BatchConfig config;

    public ObsBatchSource(S3BatchConfig config) {
        super(config);
        this.config = config;
    }

    public static final String S3A_ACCESS_KEY = "fs.s3a.access.key";
    public static final String S3A_SECRET_KEY = "fs.s3a.secret.key";

    @Override
    protected Map<String, String> getFileSystemProperties(BatchSourceContext context) {
        Map<String, String> properties = new HashMap<>(config.getFilesystemProperties());
        if (config.connection.isAccessCredentials()) {
            if (config.path.startsWith("https://")) {
                properties.put(ObsConstants.OBS_ACCESS_KEY, config.connection.getAccessKey());
                properties.put(ObsConstants.OBS_SECRET_KEY, config.connection.getSecretKey());
                properties.put(ObsConstants.OBS_END_POINT, config.connection.getEndPoint());
            } //TODO fix
        }
        if (config.shouldCopyHeader()) {
            properties.put(PathTrackingInputFormat.COPY_HEADER, "true");
        }
        if (config.getFileEncoding() != null && !config.getFileEncoding().equals(config.getDefaultFileEncoding())) {
            properties.put(PathTrackingInputFormat.SOURCE_FILE_ENCODING, config.getFileEncoding());
        }
        return properties;
    }

    @Override
    protected void recordLineage(LineageRecorder lineageRecorder, List<String> outputFields) {
        lineageRecorder.recordRead("Read", "Read from S3.", outputFields);
    }

    @Override
    protected boolean shouldGetSchema() {
        return !config.containsMacro(S3BatchConfig.NAME_PATH) && !config.containsMacro(S3BatchConfig.NAME_FORMAT) &&
                !config.containsMacro(S3BatchConfig.NAME_DELIMITER) && !config.containsMacro(ObsConnectorConfig.NAME_ACCESS_KEY)
                && !config.containsMacro(S3BatchConfig.NAME_FILE_SYSTEM_PROPERTIES) &&
                !config.containsMacro(ObsConnectorConfig.NAME_ACCESS_KEY)
                && !config.containsMacro(ObsConnectorConfig.NAME_ACCESS_KEY);
    }

    /**
     * Config class that contains properties needed for the S3 source.
     */
    @SuppressWarnings("unused")
    public static class S3BatchConfig extends AbstractFileSourceConfig {
        public static final String NAME_PATH = "path";
        private static final String NAME_FILE_SYSTEM_PROPERTIES = "fileSystemProperties";
        private static final String NAME_DELIMITER = "delimiter";

        private static final Gson GSON = new Gson();
        private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

        @Macro
        @Description("Path to file(s) to be read. If a directory is specified, terminate the path name with a '/'. " +
                "The path must start with https:// as follows: " +
                "https://Bucket name.Domain name/Object name Example: " +
                "https://bucketname.obs.cn-north-4.myhuaweicloud.com/objectname.")
        private String path;

        @Name(ConfigUtil.NAME_USE_CONNECTION)
        @Nullable
        @Description("Whether to use an existing connection.")
        private Boolean useConnection;

        @Name(ConfigUtil.NAME_CONNECTION)
        @Macro
        @Nullable
        @Description("The connection to use.")
        private ObsConnectorConfig connection;

        @Macro
        @Nullable
        @Description("Any additional properties to use when reading from the filesystem. " +
                "This is an advanced feature that requires knowledge of the properties supported by the underlying filesystem.")
        private String fileSystemProperties;

        public S3BatchConfig() {
            fileSystemProperties = GSON.toJson(Collections.emptyMap());
        }

        @Override
        public void validate() {
            // no-op
        }

        @Override
        public void validate(FailureCollector collector) {
            super.validate(collector);
            ConfigUtil.validateConnection(this, useConnection, connection, collector);
            if (!containsMacro(ConfigUtil.NAME_CONNECTION)) {
                if (connection == null) {
                    collector.addFailure("Connection credentials is not provided", "Please provide valid credentials");
                } else {
                    connection.validate(collector);
                }
            }
            if (!containsMacro("path") && !path.startsWith("https://")) {
                collector.addFailure("Path must start with https://", null).withConfigProperty(NAME_PATH);
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

        public ObsConnectorConfig getConnection() {
            return connection;
        }

        Map<String, String> getFilesystemProperties() {
            Map<String, String> properties = new HashMap<>();
            if (containsMacro("fileSystemProperties")) {
                return properties;
            }
            return GSON.fromJson(fileSystemProperties, MAP_STRING_STRING_TYPE);
        }
    }
}
