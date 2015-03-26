package com.publicreputation;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import java.util.concurrent.TimeUnit;

public class CollectionWriter extends MongoItemProcessor {
    protected CollectionWriter(DBCollection collection, ExtendedQueue<DBObject> pool) {
        super(collection, pool);
    }

    @Override
    public Void call() throws Exception {
        Thread.currentThread().setName("WRITER " + getThreadName());
        System.out.println(getThreadName()+ " started");
        while (pool.processing()) {
            try {
                DBObject poll = pool.poll();
                if (poll == null) {
                    System.out.println(getThreadName() + ": nothing to load, sleeping...");
                    Thread.sleep(1000);
                    continue;
                }
                collection.save(poll);
                processed++;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println(getThreadName() + ": processed " + processed);

        return null;
    }
}
