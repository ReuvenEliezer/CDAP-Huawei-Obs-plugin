package io.cdap.plugin.huawei.obs.connector;


import com.obs.services.IObsClient;
import com.obs.services.ObsClient;
import com.obs.services.model.ListBucketsRequest;
import com.obs.services.model.ListObjectsRequest;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObsBucket;
import com.obs.services.model.ObsObject;
import io.cdap.cdap.api.annotation.Category;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseEntityPropertyValue;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.ReferenceNames;
import io.cdap.plugin.format.connector.AbstractFileConnector;
import io.cdap.plugin.format.connector.FileTypeDetector;
import io.cdap.plugin.format.plugin.AbstractFileSourceConfig;
import io.cdap.plugin.huawei.obs.common.ObsConnectorConfig;
import io.cdap.plugin.huawei.obs.common.ObsPath;
import io.cdap.plugin.huawei.obs.common.S3Constants;
import io.cdap.plugin.huawei.obs.source.ObsBatchSource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Obs connector
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(ObsConnector.NAME)
@Category("Huawei Web Services")
@Description("Connection to access data in Huawei Obs.")
public class ObsConnector extends AbstractFileConnector<ObsConnectorConfig> {
    public static final String NAME = "Obs";
    private static final String DELIMITER = "/";
    static final String BUCKET_TYPE = "bucket";
    static final String DIRECTORY_TYPE = "directory";
    static final String FILE_TYPE = "file";
    static final String LAST_MODIFIED_KEY = "Last Modified";
    static final String SIZE_KEY = "Size";
    static final String FILE_TYPE_KEY = "File Type";


    private IObsClient obsClient;
    private ObsConnectorConfig config;

    public ObsConnector(ObsConnectorConfig config) {
        super(config);
        this.obsClient = new ObsClient(config.getAccessKey(), config.getSecretKey(), config.getEndPoint());
        this.config = config;
    }

    @Override
    public void test(ConnectorContext connectorContext) throws ValidationException {
        FailureCollector failureCollector = connectorContext.getFailureCollector();
        config.validate(failureCollector);
        // if there is any problem here, that means the credentials are not given so no need to continue
        if (!failureCollector.getValidationFailures().isEmpty()) {
            return;
        }

        obsClient = getObsClient();
        obsClient.listBuckets(new ListBucketsRequest());
    }

    @Override
    public BrowseDetail browse(ConnectorContext connectorContext, BrowseRequest request) throws IOException {
        obsClient = getObsClient();
        String path = request.getPath();
        int limit = request.getLimit() == null || request.getLimit() <= 0 ? Integer.MAX_VALUE : request.getLimit();
        if (isRoot(path)) {
            return browseBuckets(limit);
        }
        return browseObjects(ObsPath.from(request.getPath()), limit);
    }

    private BrowseDetail browseObjects(ObsPath path, int limit) {
        IObsClient obsClient = getObsClient();
        ListObjectsRequest listObjectsRequest = getListObjectsRequest(path);
        List<BrowseEntity> entities = new ArrayList<>();
        BrowseDetail.Builder builder = BrowseDetail.builder();

        ObjectListing result;
        int count = 0;
        do {
            result = obsClient.listObjects(listObjectsRequest);
            // common prefixes are directories
            for (String dir : result.getCommonPrefixes()) {
                if (dir.equalsIgnoreCase("/")) {
                    continue;
                }
                if (count >= limit) {
                    break;
                }
                StringBuilder sb = new StringBuilder(result.getBucketName()).append(dir);
                entities.add(BrowseEntity.builder(new File(dir).getName(), sb.toString(),
                        DIRECTORY_TYPE).canBrowse(true).canSample(true).build());
                count++;
            }
            for (ObsObject summary : result.getObjects()) {
                if (count >= limit) {
                    break;
                }

                BrowseEntity build = generateFromSummary(summary);
                entities.add(build);
                count++;
            }
            listObjectsRequest.setMarker(result.getMarker());
        } while (count < limit && result.isTruncated());

        // if the result is empty, this path may already be a file so just try to list it without "/" in prefix
        if (count == 0 && entities.isEmpty()) {
            ListObjectsRequest fileRequest = new ListObjectsRequest();
            fileRequest.setBucketName(path.getBucket());
            fileRequest.setPrefix(path.getName());
            ObjectListing listing = obsClient.listObjects(fileRequest);
            List<ObsObject> objectSummaries = listing.getObjects();
            if (objectSummaries.isEmpty()) {
                return builder.build();
            }
            return builder.setTotalCount(1).addEntity(generateFromSummary(objectSummaries.get(0))).build();
        }
        return builder.setTotalCount(count).setEntities(entities).build();
    }

    private BrowseEntity generateFromSummary(ObsObject summary) {
        String name = summary.getObjectKey();
        // on aws the file name can be empty, it this way the key here will ends with "/"
        BrowseEntity.Builder entity = BrowseEntity.builder(name.endsWith(DELIMITER) ? "" : new File(name).getName(),
                String.format("%s/%s", summary.getBucketName(), name),
                FILE_TYPE);
        Map<String, BrowseEntityPropertyValue> properties = new HashMap<>();
        //TODO fix
//        properties.put(SIZE_KEY, BrowseEntityPropertyValue.builder(
//                String.valueOf(summary.getSize()), BrowseEntityPropertyValue.PropertyType.SIZE_BYTES).build());
        properties.put(LAST_MODIFIED_KEY, BrowseEntityPropertyValue.builder(
                String.valueOf(summary.getMetadata().getLastModified()), BrowseEntityPropertyValue.PropertyType.TIMESTAMP_MILLIS).build());
        String fileType = FileTypeDetector.detectFileType(name);
        properties.put(FILE_TYPE_KEY, BrowseEntityPropertyValue.builder(
                fileType, BrowseEntityPropertyValue.PropertyType.STRING).build());
        entity.canSample(FileTypeDetector.isSampleable(fileType));
        entity.setProperties(properties);
        return entity.build();
    }

    private ListObjectsRequest getListObjectsRequest(ObsPath path) {
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
        listObjectsRequest.setBucketName(path.getBucket());
        String name = path.getName();
        String prefix = name.isEmpty() ? null : name.endsWith(DELIMITER) ? name : name + DELIMITER;
        if (prefix != null) {
            listObjectsRequest.setPrefix(prefix);
        }
        listObjectsRequest.setDelimiter(DELIMITER);
        return listObjectsRequest;
    }

    private BrowseDetail browseBuckets(int limit) {
        IObsClient iObsClient = getObsClient();
        List<ObsBucket> buckets = iObsClient.listBuckets(new ListBucketsRequest());
        BrowseDetail.Builder builder = BrowseDetail.builder().setTotalCount(buckets.size());
        for (int i = 0; i < Math.min(buckets.size(), limit); i++) {
            String name = buckets.get(i).getBucketName();
            builder.addEntity(BrowseEntity.builder(name, name, BUCKET_TYPE).canBrowse(true).canSample(true).build());
        }
        return builder.build();
    }

    @Override
    protected String getFullPath(String path) {
        if (isRoot(path)) {
            return ObsPath.SCHEME;
        }
        return ObsPath.from(path).getFullPath();
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (obsClient != null) {
            obsClient.close();
        }
    }


    @Override
    protected Map<String, String> getFileSystemProperties(String path) {
        Map<String, String> properties = new HashMap<>();
        if (!config.isAccessCredentials()) {
            return properties;
        }

        properties.put(S3Constants.S3N_SECRET_KEY, config.getSecretKey());
        properties.put(S3Constants.S3N_ACCESS_KEY, config.getAccessKey());
        return properties;
    }

    @Override
    protected void setConnectorSpec(ConnectorSpecRequest request, ConnectorSpec.Builder builder) {
        super.setConnectorSpec(request, builder);
        Map<String, String> properties = new HashMap<>();
        properties.put(ConfigUtil.NAME_USE_CONNECTION, "true");
        properties.put(ConfigUtil.NAME_CONNECTION, request.getConnectionWithMacro());
        properties.put(ObsBatchSource.S3BatchConfig.NAME_PATH, getFullPath(request.getPath()));
        properties.put(AbstractFileSourceConfig.NAME_FORMAT, FileTypeDetector.detectFileFormat(
                FileTypeDetector.detectFileType(request.getPath())).name().toLowerCase());
        if (!isRoot(request.getPath())) {
            ObsPath obsPath = ObsPath.from(request.getPath());
            properties.put(Constants.Reference.REFERENCE_NAME,
                    ReferenceNames.cleanseReferenceName(obsPath.getBucket() + "." + obsPath.getName()));
        }
        builder.addRelatedPlugin(new PluginSpec(ObsBatchSource.NAME, BatchSource.PLUGIN_TYPE, properties));
    }


    private IObsClient getObsClient() {
        if (obsClient == null) {
            obsClient = new ObsClient(config.getAccessKey(), config.getSecretKey(), config.getEndPoint());
        }
        return obsClient;
    }

    private boolean isRoot(String path) {
        return path.isEmpty() || path.equals(ObsPath.ROOT_DIR);
    }

}
