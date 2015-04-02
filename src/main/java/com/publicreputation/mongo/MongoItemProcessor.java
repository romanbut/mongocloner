package com.publicreputation.mongo;

import com.mongodb.DBCollection;
import com.publicreputation.util.ExtendedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MongoItemProcessor<T> implements Runnable {
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    protected final DBCollection collection;
    protected final ExtendedQueue<T> pool;

    protected MongoItemProcessor(DBCollection collection, ExtendedQueue<T> pool) {
        this.collection = collection;
        this.pool = pool;
    }
}
