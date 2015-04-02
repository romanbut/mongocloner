package com.publicreputation.util;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import java.io.Closeable;
import java.io.IOException;
import java.net.UnknownHostException;

public class MongoConnectionWrapper implements Closeable {
    private final String uri;
    private MongoClientURI mongoClientURI;
    private MongoClient mongoClient;

    public MongoConnectionWrapper(String uri) throws UnknownHostException {
        this.uri = uri;
        init();
    }

    private void init() throws UnknownHostException {
        mongoClientURI = new MongoClientURI(uri);
        mongoClient = new MongoClient(mongoClientURI);
    }

    public DB getDB() {
        return mongoClient.getDB(mongoClientURI.getDatabase());
    }

    @Override
    public void close() throws IOException {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
