package com.publicreputation.mongo;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.publicreputation.util.ExtendedQueue;

public class CollectionWriter extends MongoItemProcessor<DBObject> {

    protected CollectionWriter(DBCollection collection, ExtendedQueue<DBObject> pool) {
        super(collection, pool);
    }

    @Override
    public void run() {
        while (pool.processing()) {
            try {
                DBObject poll = pool.poll();
                if (poll == null) {
                    Thread.sleep(500);
                    continue;
                }
                collection.save(poll);
                pool.incrementWriteCount();
            } catch (Exception e) {
                logger.warn("Failed to perform write", e);
            }
        }
    }
}
