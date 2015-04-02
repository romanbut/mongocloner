package com.publicreputation;

import com.mongodb.DBCollection;
import com.publicreputation.mongo.CollectionProcessor;
import com.publicreputation.util.CommonUtils;
import com.publicreputation.util.MongoConnectionWrapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final String[] propFiles = {"mongo.properties", "queries.properties"};
    private static final String DEFAULT_QUERY = "{}";
    private static ExecutorService executor;
    public static Properties PROPERTIES;

    public static void main(String[] args) {
        initProperties();
        initExecutors();


        String sourceURI = PROPERTIES.getProperty("source.mongo.uri");
        String targetURI = PROPERTIES.getProperty("target.mongo.uri");

        try (MongoConnectionWrapper srcClient = new MongoConnectionWrapper(sourceURI);
             MongoConnectionWrapper targetClient = new MongoConnectionWrapper(targetURI)) {

            Collection<String> collections = getCollectionsFrom(args, sourceURI);

            for (String collection : collections) {
                DBCollection source = srcClient.getDB().getCollection(collection);
                DBCollection target = targetClient.getDB().getCollection(collection);

                executor.submit(new CollectionProcessor(source, target, getQueryFor(collection, PROPERTIES)));
            }
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.DAYS);

        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }

    }

    private static Collection<String> getCollectionsFrom(final String[] args, final String sourceDBUri) {
        return (args.length == 1 && "*".equals(args[0].trim())) ? CommonUtils.getCollectionNamesFrom(sourceDBUri) : Arrays.asList(args);
    }

    private static void initProperties() {
        PROPERTIES = new Properties();
        for (String propFile : propFiles) {
            try (InputStream stream = Main.class.getClassLoader().getResourceAsStream(propFile)) {
                PROPERTIES.load(stream);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void initExecutors() {
        executor = Executors.newFixedThreadPool(Integer.parseInt(PROPERTIES.getProperty("concurrent.cloner.count")), CommonUtils.createThreadFactory("MainPool"));
    }

    private static String getQueryFor(final String collection, final Properties source) {
        return source.getProperty("source." + collection + ".query", DEFAULT_QUERY);
    }

}
