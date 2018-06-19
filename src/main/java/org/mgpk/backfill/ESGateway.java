package org.mgpk.backfill;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.DefaultRequest;
import com.amazonaws.Request;
import com.amazonaws.SdkBaseException;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.http.AmazonHttpClient;
import com.amazonaws.http.HttpMethodName;
import com.amazonaws.http.HttpResponse;
import com.amazonaws.http.HttpResponseHandler;
import com.google.gson.JsonObject;

import org.mgpk.backfill.data.Record;
import org.mgpk.backfill.util.PropertiesUtil;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.logging.Logger;

/**
 * Execute REST API (bulk) towards Amazon ES.
 */
public class ESGateway {

    private static String ES_HOST;

    private final static Logger logger = Logger.getLogger("ESGateway");
    private AmazonHttpClient mClient;
    private AWSCredentials mCredentials;
    private final AWS4Signer mSigner = new AWS4Signer();

    void init() {
        mClient = new AmazonHttpClient(new ClientConfiguration());

        ES_HOST = PropertiesUtil.getEsHostName();

        mCredentials =  new BasicAWSCredentials(PropertiesUtil.getAccessKey(), PropertiesUtil.getSecretKey());
        mSigner.setRegionName("ap-northeast-1"); // for Tokyo region.
        mSigner.setServiceName("es");
    }

    void executeRequest(Record record, String logData) {
        if (mClient != null) {
            JsonObject obj = record.getJsonObject();
            String index = obj.get("esIndexName").getAsString();
            String type = obj.get("esTypeName").getAsString();
            String id = obj.get("esDocumentId").getAsString();

            String reqBody = buildBulkRequest(index, type, id, logData).toString();

            Request<?> request = new DefaultRequest<Void>("es");
            request.setContent(new ByteArrayInputStream(reqBody.getBytes()));
            request.setEndpoint(URI.create(ES_HOST + "/" + index + "/" + id + "/_bulk"));
            request.setHttpMethod(HttpMethodName.POST);

            execute(request);
        }
    }

    void executeMultiRequest(ArrayList<Record> records) {
        if (mClient != null) {

            StringBuilder builder = new StringBuilder();
            for (Record record : records) {
                builder.append(buildBulkRequest(
                        record.getIndex(), record.getType(), record.getId(), record.getDecodedData()));
            }
            String reqBody = builder.toString();

            Request<?> request = new DefaultRequest<Void>("es");
            request.setContent(new ByteArrayInputStream(reqBody.getBytes()));
            request.setEndpoint(URI.create(ES_HOST + "/_bulk"));
            request.setHttpMethod(HttpMethodName.POST);

            execute(request);
        }
    }

    private void execute(Request<?> request) {
        mSigner.sign(request, mCredentials);

        AmazonHttpClient.RequestExecutionBuilder builder = mClient.requestExecutionBuilder();
        builder.request(request);

        HttpResponseHandler<SdkBaseException> errorHandler = new HttpResponseHandler<SdkBaseException>() {
            @Override
            public SdkBaseException handle(HttpResponse response) throws Exception {
                logger.info("error response: " + response.getStatusText());
                logger.info("content: " + LogParser.convertInputStreamToString(response.getContent()));
                return null;
            }

            @Override
            public boolean needsConnectionLeftOpen() {
                return false;
            }
        };

        builder.errorResponseHandler(errorHandler);
        builder.execute();
    }


    private StringBuilder buildBulkRequest(String index, String type, String id, String logData) {
        StringBuilder builder = new StringBuilder();

        String actionMetaData
                = String.format("{ \"index\" : { \"_index\" : \"%s\", \"_type\" : \"%s\" , \"_id\" : \"%s\"} }%n",
                index, type, id);

        builder.append(actionMetaData);
        builder.append(logData);
        builder.append("\n");

        return builder;
    }
}
