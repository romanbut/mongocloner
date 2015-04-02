package com.publicreputation.util;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoURI;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadFactory;

public class CommonUtils {
    private static final String THREAD_NAMING_SUFFIX = "-%d";

    private CommonUtils() {
    }


    public static Set<String> getCollectionNamesFrom(final String mongoURI) {
        return getCollectionNamesFrom(new MongoClientURI(mongoURI));
    }

    public static Set<String> getCollectionNamesFrom(final MongoClientURI uri) {
        Set<String> strings = new HashSet<>();
        MongoClient client = null;
        try {
            client = new MongoClient(uri);
            DB db = client.getDB(uri.getDatabase());
            strings.addAll(db.getCollectionNames());
            strings.removeIf(name -> StringUtils.startsWith(name, "system"));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (client != null) {
                client.close();
            }
        }
        return strings;
    }

    public static ThreadFactory createThreadFactory(final String namingPrefix) {
        return new BasicThreadFactory.Builder().daemon(true).namingPattern(namingPrefix + THREAD_NAMING_SUFFIX).build();
    }
}
