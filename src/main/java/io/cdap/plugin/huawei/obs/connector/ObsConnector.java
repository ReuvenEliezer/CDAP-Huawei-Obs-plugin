package io.cdap.plugin.huawei.obs.connector;


import com.obs.services.IObsClient;
import com.obs.services.ObsClient;
import com.obs.services.model.ListBucketsRequest;
import com.obs.services.model.ObsBucket;
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
import io.cdap.plugin.format.connector.AbstractFileConnector;
import io.cdap.plugin.format.connector.FileTypeDetector;
import io.cdap.plugin.format.plugin.AbstractFileSourceConfig;
import io.cdap.plugin.huawei.obs.common.ObsConnectorConfig;
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
//    private static final Logger logger = LogManager.getLogger(ObsConnector.class);

    public static final String NAME = "Obs";
    private static final String BUCKET_TYPE = "bucket";


    private IObsClient obsClient;
    private  ObsConnectorConfig config;

    public ObsConnector(ObsConnectorConfig config) {
        super(config);
        this.obsClient = new ObsClient(config.getAccessKey(), config.getSecretKey(), config.getEndPoint());
        this.config = config;
//        logger.info("config: endPoint:{}", config.getEndPoint());
    }


    @Override
    public BrowseDetail browse(ConnectorContext connectorContext, BrowseRequest browseRequest) throws IOException {
//        logger.info("browse");
        List<ObsBucket> buckets = obsClient.listBuckets(new ListBucketsRequest());
        BrowseDetail.Builder builder = BrowseDetail.builder().setTotalCount(buckets.size());
        for (int i = 0; i < Math.min(buckets.size(), browseRequest.getLimit()); i++) {
            String name = buckets.get(i).getBucketName();
            builder.addEntity(BrowseEntity.builder(name, name, BUCKET_TYPE).canBrowse(true).canSample(true).build());
        }
        return builder.build();
    }

    @Override
    public void close() throws IOException {
        super.close();
        obsClient.close();
//        logger.info("close");
    }

    @Override
    public void test(ConnectorContext connectorContext) throws ValidationException {
        FailureCollector failureCollector = connectorContext.getFailureCollector();
        config.validate(failureCollector);
        // if there is any problem here, that means the credentials are not given so no need to continue
        if (!failureCollector.getValidationFailures().isEmpty()) {
            return;
        }

        obsClient.listBuckets(new ListBucketsRequest());
    }


    @Override
    protected Map<String, String> getFileSystemProperties(String path) {
        Map<String, String> properties = new HashMap<>();

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
        builder.addRelatedPlugin(new PluginSpec(ObsBatchSource.NAME, BatchSource.PLUGIN_TYPE, properties));
    }
}
