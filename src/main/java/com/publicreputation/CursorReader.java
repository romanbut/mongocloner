package com.publicreputation;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.bson.BSON;

public class CursorReader extends MongoItemProcessor {
    private final DBObject query;

    public CursorReader(DBCollection collection, ExtendedQueue<DBObject> pool, DBObject query) {
        super(collection, pool);
        this.query = query;
    }

    @Override
    public Void call() throws Exception {
        Thread.currentThread().setName("READER: " + getThreadName());
        System.out.println(getThreadName() + " started");
        DBCursor dbObjects = collection.find(query);
        while (dbObjects.hasNext()) {
            pool.add(dbObjects.next());
            processed++;
        }
        pool.shutdown();
        dbObjects.close();
        System.out.println("Total records found: " + processed);
        return null;

    }
}
