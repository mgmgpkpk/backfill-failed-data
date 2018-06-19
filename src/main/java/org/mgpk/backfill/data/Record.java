package org.mgpk.backfill.data;


import com.google.gson.JsonObject;

import java.util.Base64;
import java.util.logging.Logger;


/**
 * Log data class.
 */
public class Record {

    private static final Logger logger = Logger.getLogger("Record");
    private JsonObject mObj;

    public Record (JsonObject obj) {
        mObj = obj;
    }

    public String getDecodedData() {
        String base64 = mObj.get("rawData").getAsString();
        String origin = new String(Base64.getDecoder()
                .decode(base64));
//        logger.info("origin: " + origin);
        return origin;
    }

    public JsonObject getJsonObject() {
        return mObj;
    }

    public String getIndex() {
        return mObj.get("esIndexName").getAsString();
    }

    public String getId() {
        return mObj.get("esDocumentId").getAsString();
    }

    public String getType() {
        return mObj.get("esTypeName").getAsString();
    }
}
