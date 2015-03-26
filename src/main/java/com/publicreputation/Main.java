package com.publicreputation;

public class Main {
    private static final String SRC_URI = "mongodb://rokk3rQa:trudat@c162.candidate.46.mongolayer.com:10162/public_reputation_qa";
    public static final String TARGET_URI = "mongodb://localhost:27017/public_reputation_qa";
    public static final String QUERY = "{}";
    public static final String[] COLLECTIONS = {"job","guidcount","mousetrap"};

    public static void main(String[] args) {
        for (String collection : COLLECTIONS) {
            new CollectionCloner(collection, SRC_URI, TARGET_URI, QUERY).start();
        }
    }

}
