package org.mosence.tinkerpop.gremlin.mongodb.api.impl;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbGraphAPI;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbNode;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbRelationship;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbTx;
import org.mosence.tinkerpop.gremlin.mongodb.api.impl.exception.NotFoundException;
import org.mosence.tinkerpop.gremlin.mongodb.api.impl.property.MongodbEdgeProperty;
import org.mosence.tinkerpop.gremlin.mongodb.api.impl.property.MongodbNodeProperty;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.client.model.Filters.*;

/**
 * mongodb 图api实现
 * @author MoSence
 */
public class MongodbGraphApiImpl implements MongodbGraphAPI {

    private final String nodeCollection;
    private final String edgeCollection;
    private final MongoClient mongoClient;
    private final String database;
    private volatile MongodbTxImpl mongodbTx;

    MongodbGraphApiImpl(MongoClient mongoClient, String database, String nodeCollection, String edgeCollection) {
        this.mongoClient = mongoClient;
        this.database = database;
        this.nodeCollection = nodeCollection;
        this.edgeCollection = edgeCollection;
        this.mongodbTx = new MongodbTxImpl(mongoClient);
    }

    @Override
    public MongodbNode createNode(String... labels) {
        Document node = new Document();
        String type = labels.length == 0 ? "": labels[0];
        node.put(MongodbNodeProperty._id.name(), ObjectId.get());
        node.put(MongodbNodeProperty.type.name(),type);
        node.put(MongodbNodeProperty.label.name(), Stream.of(labels).collect(Collectors.toList()));
        return MongodbNodeImpl.newInstance(node,nodeCollection(),edgeCollection());
    }

    @Override
    public MongodbNode getNodeById(String id) throws NotFoundException{

        Document result = nodeCollection().find(
                or(
                        eq(MongodbNodeProperty._id.name(),new ObjectId(id)),
                        eq(MongodbNodeProperty._id.name(),id)
                )).first();
        return Optional.ofNullable(result).map(node->MongodbNodeImpl.valueOf(node,nodeCollection(),edgeCollection())).orElseThrow(NotFoundException::new);
    }

    @Override
    public MongodbRelationship getRelationshipById(String id) throws NotFoundException{
        Document result = edgeCollection().find(
                or(
                        eq(MongodbEdgeProperty._id.name(),new ObjectId(id)),
                        eq(MongodbEdgeProperty._id.name(),id)
                )).first();
        return Optional.ofNullable(result).map(edge->MongodbRelationshipImpl.valueOf(edge,nodeCollection(),edgeCollection())).orElseThrow(NotFoundException::new);
    }

    @Override
    public void shutdown() {
        mongoClient.close();
    }

    @Override
    public Iterable<MongodbNode> allNodes() {
        return nodeCollection().find().map(node->MongodbNodeImpl.valueOf(node,nodeCollection(),edgeCollection()));
    }

    @Override
    public Iterable<MongodbRelationship> allRelationships() {
        return edgeCollection().find().map(edge->MongodbRelationshipImpl.valueOf(edge,nodeCollection(),edgeCollection()));
    }

    @Override
    public Iterable<MongodbNode> findNodes(String label) {
        return nodeCollection().find(or(new Document(MongodbNodeProperty.label.name(),label),eq(MongodbNodeProperty.type.name(),label))).map(node->MongodbNodeImpl.valueOf(node,nodeCollection(),edgeCollection()));
    }

    @Override
    public Iterable<MongodbNode> findNodes(String label, String property, Object value) {
        return nodeCollection().find(and(or(new Document(MongodbNodeProperty.label.name(),label),eq(MongodbNodeProperty.type.name(),label)),eq(property,value))).map(node->MongodbNodeImpl.valueOf(node,nodeCollection(),edgeCollection()));
    }

    @Override
    public MongodbTx tx() {
        if(Objects.isNull(mongodbTx)){
            mongodbTx = new MongodbTxImpl(mongoClient);
        }
        if(mongodbTx.isClose()){
            mongodbTx = new MongodbTxImpl(mongoClient);
        }
        return mongodbTx;
    }

    @Override
    public Iterator<Map<String, Object>> execute(String query, Map<String, Object> params) {
        //TODO query method
        return null;
    }

    @Override
    public boolean hasSchemaIndex(String label, String property) {
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterable<String> getKeys() {
        return (Iterable<String>) Collections.emptyIterator();
    }

    @Override
    public Object getProperty(String key) {
        return null;
    }

    @Override
    public boolean hasProperty(String key) {
        return false;
    }

    @Override
    public Object removeProperty(String key) {
        return null;
    }

    @Override
    public void setProperty(String key, Object value) {
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
