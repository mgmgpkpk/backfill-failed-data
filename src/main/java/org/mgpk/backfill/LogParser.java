package org.mgpk.backfill;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.InputSerialization;
import com.amazonaws.services.s3.model.JSONInput;
import com.amazonaws.services.s3.model.JSONOutput;
import com.amazonaws.services.s3.model.OutputSerialization;
import com.amazonaws.services.s3.model.SelectObjectContentEventException;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.amazonaws.services.s3.model.SelectObjectContentResult;
import com.amazonaws.services.s3.model.SelectRecordsInputStream;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import org.mgpk.backfill.data.Record;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

/**
 * Parse log files on S3 bucket.
 */
public class LogParser {

    private AmazonS3 mS3Client;
    private String mBucket;
    private String mKey;
    private ArrayList<Record> mLogList = new ArrayList<>();

    void init(String bucket, String key) {
        mBucket = bucket;
        mKey = key;
        mS3Client = AmazonS3ClientBuilder.defaultClient();
    }

    void parse() {
        String result = getRecordsFromS3();
        String[] records = result.split("\n");

        Gson gson = new Gson();
        for (int i = 0; i < records.length; i++) {
            JsonObject obj = gson.fromJson(records[i], JsonObject.class);
            mLogList.add(new Record(obj));
        }
    }

    int getRecordCount() {
        return mLogList.size();
    }

    Record getRecord(int index) {
        return mLogList.get(index);
    }

    /**
     * Execute S3 select.
     */
    private String getRecordsFromS3() {
        SelectObjectContentRequest request = new SelectObjectContentRequest();
        request.setBucketName(mBucket);
        request.setKey(mKey);
        request.setExpression("SELECT s.esDocumentId, s.esTypeName, s.esIndexName, s.rawData from S3Object s");
        request.setExpressionType("SQL");

        JSONInput input = new JSONInput();
        input.setType("Lines");
        InputSerialization inputSerial = new InputSerialization().withJson(input);
        request.setInputSerialization(inputSerial);

        JSONOutput output = new JSONOutput();
        OutputSerialization outSerial = new OutputSerialization().withJson(output);
        request.setOutputSerialization(outSerial);

        SelectObjectContentResult result =mS3Client.selectObjectContent(request);
        SelectRecordsInputStream is = null;
        try {
            is = result.getPayload().getRecordsInputStream();
        } catch (SelectObjectContentEventException e) {
            e.printStackTrace();
        }
        return convertInputStreamToString(is);
    }

    public static String convertInputStreamToString(InputStream is) {
        InputStreamReader reader = new InputStreamReader(is);
        StringBuilder builder = new StringBuilder();
        char[] buf = new char[1024];
        int numRead;

        try {
            while (0 <= (numRead = reader.read(buf))) {
                builder.append(buf, 0, numRead);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return builder.toString();
    }
}
