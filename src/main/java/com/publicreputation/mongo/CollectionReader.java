package com.publicreputation.mongo;

import com.mongodb.*;
import com.publicreputation.util.ExtendedQueue;
import com.publicreputation.Main;

public class CollectionReader extends MongoItemProcessor<DBObject> {
    private final BasicDBObject query;
    private final int batchSize;

    public CollectionReader(DBCollection collection, ExtendedQueue<DBObject> pool, BasicDBObject query) {
        super(collection, pool);
        this.query = query;
        batchSize = Integer.parseInt(Main.PROPERTIES.getProperty("concurrent.cloner.reader.batchsize", "0"));
    }

    @Override
    public void run() {
        logger.info("Reader started");
        pool.setTotalCount(collection.count(query));

        DBCursor dbObjects = collection.find(query).batchSize(batchSize);

        for (DBObject dbObject : dbObjects) {
            pool.add(dbObject);
            pool.incrementReadCount();
        }
        dbObjects.close();
        pool.shutdown();
        logger.info("Total records found: {}", pool.getTotal());
    }
}
