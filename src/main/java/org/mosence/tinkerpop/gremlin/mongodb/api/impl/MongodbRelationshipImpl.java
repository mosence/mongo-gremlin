package org.mosence.tinkerpop.gremlin.mongodb.api.impl;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbNode;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbRelationship;
import org.mosence.tinkerpop.gremlin.mongodb.api.impl.property.MongodbEdgeProperty;
import org.mosence.tinkerpop.gremlin.mongodb.api.impl.property.MongodbNodeProperty;

import java.util.Set;

import static com.mongodb.client.model.Filters.eq;
/**
 * 边实现
 * @author MoSence
 */
public class MongodbRelationshipImpl extends BaseMongodbEntityImpl implements MongodbRelationship {

    private MongodbRelationshipImpl(Document edge, MongoCollection<Document> nodeCollection, MongoCollection<Document> edgeCollection) {
        super(edge,nodeCollection,edgeCollection);
    }

    public static MongodbRelationshipImpl newInstance(Document node, MongoCollection<Document> nodeCollection, MongoCollection<Document> edgeCollection) {
        MongodbRelationshipImpl newEdge = new MongodbRelationshipImpl(node,nodeCollection,edgeCollection);
        newEdge.needCreate();
        newEdge.persist();
        return newEdge;
    }

    public static MongodbRelationshipImpl valueOf(Document node, MongoCollection<Document> nodeCollection, MongoCollection<Document> edgeCollection) {
        MongodbRelationshipImpl newEdge = new MongodbRelationshipImpl(node,nodeCollection,edgeCollection);
        return newEdge;
    }

    @Override
    public String type() {
        return entity.getString(MongodbEdgeProperty.type.name());
    }

    @Override
    public String startId() {
        Object id = entity.get(MongodbEdgeProperty.source.name());
        return id instanceof ObjectId?((ObjectId)id).toHexString():id.toString();
    }

    @Override
    public MongodbNode start() {
        ObjectId startId = new ObjectId(startId());
        return nodeCollection.find(eq(MongodbNodeProperty._id.name(),startId)).map(node->MongodbNodeImpl.valueOf(node,nodeCollection,edgeCollection)).first();
    }

    @Override
    public String endId() {
        Object id = entity.get(MongodbEdgeProperty.target.name());
        return id instanceof ObjectId?((ObjectId)id).toHexString():id.toString();
    }

    @Override
    public MongodbNode end() {
        ObjectId endId = new ObjectId(endId());
        return nodeCollection.find(eq(MongodbNodeProperty._id.name(),endId)).map(node->MongodbNodeImpl.valueOf(node,nodeCollection,edgeCollection)).first();
    }

    @Override
    public MongodbNode other(MongodbNode node) {
        String startId = entity.getString(MongodbEdgeProperty.source.name());
        String endId = entity.getString(MongodbEdgeProperty.target.name());
        if(startId.equals(node.getId())){
            return end();
        }
        if(endId.equals(node.getId())){
            return start();
        }
        throw new RuntimeException("this node is not Relationship's endpoint");
    }
    @Override
    public String toString() {
        return "["+startId()+"] -> ["+endId()+"]";
    }

    @Override
    Set<String> ignoreKeys() {
        return IGNORE_KEYS;
    }

    @Override
    public MongodbRelationshipImpl clone() throws CloneNotSupportedException {
        return (MongodbRelationshipImpl)super.clone();
    }
}
