package org.mgpk.backfill;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import org.mgpk.backfill.data.Record;
import org.mgpk.backfill.util.PropertiesUtil;

import java.util.ArrayList;
import java.util.logging.Logger;

/**
 * Main class.
 */
public class Main  {

    private static final Logger logger = Logger.getLogger("Main");

    private static final int CONCURRENT_NUM = 10;

    private static ESGateway gateway = new ESGateway();

    public static void main(String[] args) {
        gateway.init();

        ArrayList<String> logKeys = getLogFileKeysFromS3(args[0]);
        for (String key : logKeys) {
            LogParser parser = new LogParser();
            parser.init(PropertiesUtil.getBucketName(), key);
            parser.parse();
            process(parser);
        }
    }

    private static void process(LogParser parser) {
        int recordCount = parser.getRecordCount();
        int addedCount = 0;
        ArrayList<Record> targets = new ArrayList<>();
        while (addedCount < recordCount) {
            logger.info("addedCount: " + addedCount);
            String index = parser.getRecord(addedCount).getIndex();
            int maxCount = addedCount + CONCURRENT_NUM < recordCount
                    ? addedCount + CONCURRENT_NUM : recordCount;

            for (int i = addedCount; i < maxCount; i++) {
                Record record = parser.getRecord(i);
                if (index.equals(record.getIndex())) {
                    targets.add(record);
                } else {
                    gateway.executeRequest(record, record.getDecodedData());
                }
            }

            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    gateway.executeMultiRequest(targets);
                }
            };
            Thread thread = new Thread(runnable);
            thread.start();

            addedCount += CONCURRENT_NUM;
        }
    }

    private static ArrayList<String> getLogFileKeysFromS3(String prefix) {
        AmazonS3 client = AmazonS3ClientBuilder.defaultClient();
        ListObjectsRequest request = new ListObjectsRequest()
                .withBucketName(PropertiesUtil.getBucketName())
                .withPrefix(prefix);
        ObjectListing listing = client.listObjects(request);

        ArrayList<S3ObjectSummary> objSummary = (ArrayList<S3ObjectSummary>) listing.getObjectSummaries();
        ArrayList<String> keys = new ArrayList<>();
        for (S3ObjectSummary summary : objSummary) {
            keys.add(summary.getKey());
        }
        return keys;
    }
}
