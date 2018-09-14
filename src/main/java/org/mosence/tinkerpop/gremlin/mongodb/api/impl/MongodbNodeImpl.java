package org.mosence.tinkerpop.gremlin.mongodb.api.impl;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbDirection;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbNode;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbRelationship;
import org.mosence.tinkerpop.gremlin.mongodb.api.impl.property.MongodbEdgeProperty;
import org.mosence.tinkerpop.gremlin.mongodb.api.impl.property.MongodbNodeProperty;

import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.client.model.Filters.*;

/**
 * mongodb 点实现
 *
 * @author MoSence
 */
@SuppressWarnings("unchecked")
public class MongodbNodeImpl extends BaseMongodbEntityImpl implements MongodbNode {

    private MongodbNodeImpl(Document node, MongoCollection<Document> nodeCollection, MongoCollection<Document> edgeCollection) {
        super(node,nodeCollection,edgeCollection);
    }

    public static MongodbNodeImpl newInstance(Document node, MongoCollection<Document> nodeCollection, MongoCollection<Document> edgeCollection) {
        MongodbNodeImpl newNode = new MongodbNodeImpl(node,nodeCollection,edgeCollection);
        newNode.needCreate();
        MongodbTxImpl.ram(newNode);
        newNode.persist();
        return newNode;
    }

    public static MongodbNodeImpl valueOf(Document node, MongoCollection<Document> nodeCollection, MongoCollection<Document> edgeCollection) {
        MongodbNodeImpl newNode = new MongodbNodeImpl(node,nodeCollection,edgeCollection);
        MongodbTxImpl.ram(newNode);
        return newNode;
    }

    @Override
    public Set<String> labels() {
        Object labels = Optional.ofNullable(entity.get(MongodbNodeProperty.label.name())).orElse(Collections.EMPTY_SET);
        if (labels instanceof Array) {
            return Stream.of((Object[]) labels).map(Objects::toString).collect(Collectors.toSet());
        }
        if (labels instanceof Collection) {
            return ((Collection<Object>) labels).stream().map(Object::toString).collect(Collectors.toSet());
        }
        if (labels instanceof String) {
            return Stream.of(Objects.toString(labels)).collect(Collectors.toSet());
        }
        return Collections.EMPTY_SET;
    }

    @Override
    public String type() {
        return entity.getString(MongodbNodeProperty.type.name());
    }

    @Override
    public String name() {
        return Objects.toString(entity.getString(MongodbNodeProperty.name.name()),"<NoName>");
    }

    @Override
    public boolean hasLabel(String label) {
        return labels().contains(label);
    }

    @Override
    public void addLabel(String label) {
        entity.put(MongodbNodeProperty.label.name(), labels().add(label));
        needUpdate();
        persist();
    }

    @Override
    public void removeLabel(String label) {
        entity.put(MongodbNodeProperty.label.name(), labels().remove(label));
        needUpdate();
        persist();
    }

    @Override
    public int degree(MongodbDirection direction, String type) {
        int result = 0;
        switch (direction) {
            case OUTGOING:
                result = Long.valueOf(edgeCollection.countDocuments(eq(MongodbEdgeProperty.source.name(), entity.get(MongodbNodeProperty._id.name())))).intValue();
                break;
            case INCOMING:
                result = Long.valueOf(edgeCollection.countDocuments(eq(MongodbEdgeProperty.target.name(), entity.get(MongodbNodeProperty._id.name())))).intValue();
                break;
            case BOTH:
                result = Long.valueOf(edgeCollection.countDocuments(or(eq(MongodbEdgeProperty.source.name(), entity.get(MongodbNodeProperty._id.name())), eq(MongodbEdgeProperty.target.name(), entity.get(MongodbNodeProperty._id.name()))))).intValue();
                break;
            default:
                break;
        }
        return result;
    }

    @Override
    public Iterable<MongodbRelationship> relationships(MongodbDirection direction, String... types) {
        Bson filter;
        switch (direction) {
            case OUTGOING:
                filter = eq(MongodbEdgeProperty.source.name(), getId());
                break;
            case INCOMING:
                filter = eq(MongodbEdgeProperty.target.name(), getId());
                break;
            case BOTH:
            default:
                filter = or(eq(MongodbEdgeProperty.source.name(), getId()), eq(MongodbEdgeProperty.target.name(), getId()));
                break;
        }
        if (types.length > 0) {
            filter = and(in(MongodbNodeProperty.type.name(), types),filter);
        }
        return edgeCollection.find(filter).map(edge -> MongodbRelationshipImpl.valueOf(edge, nodeCollection, edgeCollection));
    }

    @Override
    public MongodbRelationship connectTo(MongodbNode node, String type) {
        if(this.isDelete() || node.isDelete()){
            throw new RuntimeException("Can not create relationship with deleted node!");
        }
        Document edge = new Document();
        edge.put(MongodbEdgeProperty._id.name(), ObjectId.get());
        edge.put(MongodbEdgeProperty.type.name(),type);
        edge.put(MongodbEdgeProperty.source.name(),this.getId());
        edge.put(MongodbEdgeProperty.target.name(),node.getId());
        return MongodbRelationshipImpl.newInstance(edge,nodeCollection,edgeCollection);
    }
    @Override
    public String toString() {
        return "["+getId()+"::<"+type()+">::"+name()+"]";
    }

    @Override
    Set<String> ignoreKeys() {
        return IGNORE_KEYS;
    }

    @Override
    public MongodbNodeImpl clone() throws CloneNotSupportedException {
        return (MongodbNodeImpl)super.clone();
    }
}
