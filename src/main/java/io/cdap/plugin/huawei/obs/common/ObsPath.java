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

import io.cdap.plugin.huawei.obs.connector.ObsConnector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A path on Obs. Contains information about the bucket and file name (if applicable).
 * A path is of the form hdfs://bucket.domain/name.
 */
public class ObsPath {
    public static final String ROOT_DIR = "/";
    public static final String SCHEME = "obs://";
    private final String fullPath;
    private final String bucket;
    private final String name;

    private static final Logger logger = LogManager.getLogger(ObsPath.class);


    private ObsPath(String fullPath, String bucket, String name) {
        this.fullPath = fullPath;
        this.bucket = bucket;
        this.name = name;
    }

    public String getFullPath() {
        return fullPath;
    }

    public String getBucket() {
        return bucket;
    }

    /**
     * @return the object name. This will be an empty string if the path represents a bucket.
     */
    public String getName() {
        return name;
    }

    boolean isBucket() {
        return name.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ObsPath obsPath = (ObsPath) o;
        return Objects.equals(fullPath, obsPath.fullPath) &&
                Objects.equals(bucket, obsPath.bucket) &&
                Objects.equals(name, obsPath.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fullPath, bucket, name);
    }

    /**
     * Parse the given path string into a ObsPath. Paths are expected to be of the form
     * https://bucket.domain/dir0/dir1/file, or bucket/dir0/dir1/file.
     *
     * @param path the path string to parse
     * @return the S3Path for the given string.
     * @throws IllegalArgumentException if the path string is invalid
     */
    public static ObsPath from(String path) {
        if (path.isEmpty()) {
            throw new IllegalArgumentException("Obs path can not be empty. The path must be of form " +
                    "'https://<Bucket-name>.<Domain-name>/<Object-name>'.");
        }

        if (path.startsWith(ROOT_DIR)) {
            path = path.substring(1);
        } else if (path.startsWith(SCHEME)) {
            path = path.substring(SCHEME.length());
        }

        String bucket = path;
        int idx = path.indexOf(ROOT_DIR);
        // if the path within bucket is provided, then only get the bucket
        if (idx > 0) {
            bucket = path.substring(0, idx);
        }

        if (bucket.length() < 3 || bucket.length() > 63) {
            throw new IllegalArgumentException("Invalid bucket name, the bucket name length must be between 3 characters " +
                    "and 63 characters.");
        }

        if (!Pattern.matches("[a-z0-9-.]+", bucket)) {
            throw new IllegalArgumentException(
                    String.format("Invalid bucket name in path '%s'. Bucket name should only contain lower case alphanumeric, " +
                            "'-' and '.'. Please follow Obs bucket naming convention: " +
                            "https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html", path)); //TODO fix
        }

        String file = idx > 0 ? path.substring(idx).replaceAll("^/", "") : "";
        StringBuilder sb = new StringBuilder(SCHEME)
                .append(bucket)
                .append(ROOT_DIR)
                .append(file);
        logger.info("ObsPath from: {}, to: {}", path, sb.toString());
        return new ObsPath(sb.toString(), bucket, file);
    }
}
