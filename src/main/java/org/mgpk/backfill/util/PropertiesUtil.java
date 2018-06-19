package org.mgpk.backfill.util;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Utility class for properties.
 */
public class PropertiesUtil {

    private static final Properties properties = new Properties();
    private static boolean isLoaded = false;

    public static String getBucketName() {
        if (!isLoaded) initialize();
        return properties.getProperty("bucket");
    }

    public static String getEsHostName() {
        if (!isLoaded) initialize();
        return properties.getProperty("es-endpoint");
    }

    public static String getAccessKey() {
        if (!isLoaded) initialize();
        return properties.getProperty("accessKey");
    }

    public static String getSecretKey() {
        if (!isLoaded) initialize();
        return properties.getProperty("secretKey");
    }

    private static void initialize() {
        try {
            isLoaded = true;
            properties.load(Files.newBufferedReader(Paths.get("aws.properties"), StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
            isLoaded = false;
        }
    }
}
