package org.mosence.tinkerpop.gremlin.mongodb.api.impl;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbGraphComputerAPI;

public class MongodbGraphComputerApiImpl implements MongodbGraphComputerAPI {

    private final String nodeCollection;
    private final String edgeCollection;
    private final MongoClient mongoClient;
    private final String database;

    public MongodbGraphComputerApiImpl(MongoClient mongoClient, String database, String nodeCollection, String edgeCollection) {
        this.database = database;
        this.mongoClient = mongoClient;
        this.nodeCollection = nodeCollection;
        this.edgeCollection = edgeCollection;
    }

    @Override
    public Iterable<Document> nodeMapReduce(String mapFunction,String reduceFunction){
        return nodeCollection().mapReduce(mapFunction,reduceFunction);
    }

    @Override
    public Iterable<Document> edgeMapReduce(String mapFunction,String reduceFunction){
        return edgeCollection().mapReduce(mapFunction,reduceFunction);
    }

    private MongoDatabase graphDatabase(){
        return mongoClient.getDatabase(database);
    }
    private MongoCollection<Document> nodeCollection(){
        return graphDatabase().getCollection(nodeCollection);
    }
    private MongoCollection<Document> edgeCollection(){
        return graphDatabase().getCollection(edgeCollection);
    }
}
