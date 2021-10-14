//package io.cdap.plugin.huawei.obs.connector;
//
//import com.obs.services.IObsClient;
//import com.obs.services.ObsClient;
//import com.obs.services.model.ObsObject;
//import io.cdap.plugin.huawei.obs.common.ObsConnectorConfig;
//import org.apache.commons.io.IOUtils;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//import org.junit.Test;
//
//import java.io.IOException;
//import java.nio.charset.StandardCharsets;
//
//public class ObsConnectorTest {
//
//    private static final Logger logger = LogManager.getLogger(ObsConnectorTest.class);
//
//    private static final String bucketName = "eliezer-test";
//    private static final String endPoint = "obs.ae-ad-1.g42cloud.com";
//    private static final String accessKey = "FZZTMMWMEDNU41Q9PRHF";
//    private static final String secretKey = "w3j6iX332106RIChc5A8Zn4xfkRRoct3ZNZbrfpO";
//obs://eliezer-test/calls.csv
//    @Test
//    public void doSomeThing() {
////        ObsConnectorConfig obsConnectorConfig = new ObsConnectorConfig(accessKey, secretKey, endPoint);
////        ObsConnector connector = new ObsConnector(obsConnectorConfig);
//        IObsClient obsClient = new ObsClient(accessKey, secretKey, endPoint);
//        String key = "calls.csv";
//        logger.info("try to get object from bucket: '{}', key: '{}'", bucketName, key);
//        ObsObject obsObject = obsClient.getObject(bucketName, key);
//
//        try {
//            String text = IOUtils.toString(obsObject.getObjectContent(), StandardCharsets.UTF_8);
//            logger.info(text);
//            obsClient.close();
//        } catch (IOException e) {
//            logger.error(e.getMessage());
//        }
//        logger.info("done");
//    }
//
//}
