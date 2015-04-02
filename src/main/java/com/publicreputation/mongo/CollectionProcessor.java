package com.publicreputation.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import com.publicreputation.util.ExtendedQueue;
import com.publicreputation.util.LoggingThread;
import com.publicreputation.util.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CollectionProcessor implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final String sourceFilterQuery;
    private final DBCollection sourceCollection;
    private final DBCollection targetCollection;

    private int writersCount = 1;


    public CollectionProcessor(final DBCollection source, final DBCollection target, String sourceFilterQuery) {
        this.sourceFilterQuery = sourceFilterQuery;
        this.sourceCollection = source;
        this.targetCollection = target;

    }

    @Override
    public void run() {

        String collectionName = sourceCollection.getName();

        logger.info("Processing of `{}` started", collectionName);

        //POOL SIZE = WRITERS AMOUNT + READERS AMOUNT + LOGGING THREAD
        ExecutorService executorService = Executors.newFixedThreadPool(writersCount + 2, CommonUtils.createThreadFactory(collectionName));

        BasicDBObject query = (BasicDBObject) JSON.parse(sourceFilterQuery);

        ExtendedQueue<DBObject> pool = new ExtendedQueue<>();

        executorService.submit(new LoggingThread<>(pool, collectionName));

        executorService.submit(new CollectionReader(sourceCollection, pool, query));

        for (int idx = 0; idx < writersCount; idx++) {
            executorService.submit(new CollectionWriter(targetCollection, pool));
        }

        try {
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.DAYS);
            logger.info("Collection `{}` processing finished!", collectionName);
        } catch (InterruptedException e) {
            logger.warn("Failed to finish tasks for collection `{}`; Cause: {}", collectionName, e.getMessage());
        }

    }

    public void setWritersCount(int writersCount) {
        this.writersCount = writersCount;
    }
}
