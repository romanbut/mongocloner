package com.publicreputation;

import com.mongodb.*;
import com.mongodb.util.JSON;
import org.omg.CORBA.INTERNAL;
import org.omg.PortableInterceptor.INACTIVE;
import sun.dc.pr.PRError;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

public class CollectionCloner {
    private static final int WRITERS_COUNT = 3;

    private final String collectionName;
    private final MongoClientURI sourceURI;
    private final MongoClientURI targetURI;
    private final String sourceFilterQuery;

    private MongoClient sourceClient;
    private MongoClient targetClient;
    private final ExecutorService executorService;


    public CollectionCloner(String collectionName, String sourceConnectionURI, String targetConnectionURI, String sourceFilterQuery) {
        this.collectionName = collectionName;
        this.sourceFilterQuery = sourceFilterQuery;
        sourceURI = new MongoClientURI(sourceConnectionURI);
        targetURI = new MongoClientURI(targetConnectionURI);
        executorService = Executors.newFixedThreadPool(WRITERS_COUNT + 1);
        init();
    }

    private void init() {
        try {
            sourceClient = new MongoClient(sourceURI);
            targetClient = new MongoClient(targetURI);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public void start() {
        BasicDBObject query = (BasicDBObject) JSON.parse(sourceFilterQuery);
        DBCollection sourceCollection = sourceClient.getDB(sourceURI.getDatabase()).getCollection(collectionName);
        DBCollection targetCollection = targetClient.getDB(targetURI.getDatabase()).getCollection(collectionName);
        ExtendedQueue<DBObject> pool = new ExtendedQueue<>();
        List<Callable<Void>> tasks = new ArrayList<>();
        tasks.add(new CursorReader(sourceCollection, pool, query));
        for (int idx = 0; idx < WRITERS_COUNT; idx++) {
            tasks.add(new CollectionWriter(targetCollection, pool));
        }
        try {
            executorService.invokeAll(tasks);
            executorService.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
