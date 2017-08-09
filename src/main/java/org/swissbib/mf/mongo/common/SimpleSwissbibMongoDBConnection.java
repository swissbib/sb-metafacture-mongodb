package org.swissbib.mf.mongo.common;

import com.mongodb.*;

import org.culturegraph.mf.mongodb.common.MongoDBConnection;

import java.util.Arrays;

public class SimpleSwissbibMongoDBConnection implements MongoDBConnection {

    private final MongoClient mongoClient;
    private final DBCollection dbCollection;


    public SimpleSwissbibMongoDBConnection(String host,
                                           String port,
                                           String user,
                                           String password,
                                           String database,
                                           String collection) {

        ServerAddress server = new ServerAddress(host, Integer.valueOf(port));

        if (user != null && password != null){
            MongoCredential credential = MongoCredential.createMongoCRCredential(
                    user,"admin",password.toCharArray());
            this.mongoClient  = new MongoClient(server, Arrays.asList(credential));

        } else {
            this.mongoClient  = new MongoClient(server);
        }

        //todo : change to new API
        DB db = this.mongoClient.getDB(database);
        this.dbCollection = db.getCollection(collection);

    }

    @Override
    public DBCursor find(DBObject dbObject) {
        return dbCollection.find(dbObject);
    }

    @Override
    public void save(DBObject dbObject) {
        dbCollection.save(dbObject);
    }

    @Override
    public void close() {
        mongoClient.close();
    }
}
