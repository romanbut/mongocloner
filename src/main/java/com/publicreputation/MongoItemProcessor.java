package com.publicreputation;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import java.util.concurrent.Callable;

public abstract class MongoItemProcessor implements Callable<Void> {
    protected final DBCollection collection;
    protected final ExtendedQueue<DBObject> pool;
    protected int processed = 0;

    protected MongoItemProcessor(DBCollection collection, ExtendedQueue<DBObject> pool) {
        this.collection = collection;
        this.pool = pool;
    }

    protected static String getThreadName() {
        return Thread.currentThread().getName();
    }
}
