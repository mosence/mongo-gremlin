package org.mosence.tinkerpop.gremlin.mongodb.api.impl;

import com.mongodb.client.MongoCollection;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbEntity;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbNode;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbRelationship;
import org.mosence.tinkerpop.gremlin.mongodb.api.impl.property.MongodbEdgeProperty;
import org.mosence.tinkerpop.gremlin.mongodb.api.impl.property.MongodbNodeProperty;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.or;

/**
 * 抽象 entity
 *
 * @author MoSence
 */
public abstract class BaseMongodbEntityImpl implements MongodbEntity, Cloneable {

    private static final String PROPERTY_REPLACE_REGEX = "\\.";
    private static final String PROPERTY_REPLACE_CHAR = "_";

    protected Document entity;
    protected MongoCollection<Document> nodeCollection;
    protected MongoCollection<Document> edgeCollection;
    private boolean isDelete = false;
    private boolean isCreate = false;
    private boolean isUpdate = false;

    protected void resetStatus() {
        isDelete = false;
        isCreate = false;
        isUpdate = false;
    }

    protected void needCreate() {
        this.isCreate = true;
    }

    protected void needUpdate() {
        this.isUpdate = true;
    }

    protected BaseMongodbEntityImpl(Document entity, MongoCollection<Document> nodeCollection, MongoCollection<Document> edgeCollection) {
        if (Objects.isNull(entity)) {
            throw new NullPointerException();
        }
        this.entity = entity;
        this.nodeCollection = nodeCollection;
        this.edgeCollection = edgeCollection;
    }

    @Override
    public String getId() {
        if (this instanceof MongodbNode) {
            Object id = entity.get(MongodbNodeProperty._id.name());
            return id instanceof ObjectId ? ((ObjectId) id).toHexString() : id.toString();
        }
        if (this instanceof MongodbRelationship) {
            Object id = entity.get(MongodbNodeProperty._id.name());
            return id instanceof ObjectId ? ((ObjectId) id).toHexString() : id.toString();
        }
        throw new RuntimeException("This entity have not an id!");
    }

    @Override
    public Iterable<String> getKeys() {
        return entity.keySet().stream()
                .map(key -> key.replaceAll(PROPERTY_REPLACE_REGEX, PROPERTY_REPLACE_CHAR))
                .filter(key -> !ignoreKeys().contains(key)).collect(Collectors.toSet());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object getProperty(String name) {
        name = formatProperty(name);
        Object property = entity.get(name);
        if(property instanceof Collection){
            TraverserSet<Object> set = new TraverserSet<>();
            set.addAll((Collection)property);
            return set;
        }
        return property;
    }

    @Override
    public Object getProperty(String name, Object defaultValue) {
        name = formatProperty(name);
        return entity.getOrDefault(name, defaultValue);
    }

    @Override
    public void setProperty(String name, Object value) {
        name = formatProperty(name);
        entity.put(name, value);
        needUpdate();
        persist();
    }

    private String formatProperty(String name) {
        return name.replaceAll(PROPERTY_REPLACE_REGEX, PROPERTY_REPLACE_CHAR);
    }

    @Override
    public Object removeProperty(String name) {
        entity.remove(name);
        needUpdate();
        persist();
        return entity;
    }

    @Override
    public boolean hasProperty(String name) {
        name = formatProperty(name);
        return entity.containsKey(name);
    }

    @Override
    public void delete() {
        this.isDelete = true;
        persist();
    }

    @Override
    public boolean isDelete() {
        return isDelete;
    }

    public boolean isCreate() {
        return isCreate;
    }

    public boolean isUpdate() {
        return isUpdate;
    }

    protected boolean originExists() {
        if (this instanceof MongodbNode) {
            return Objects.nonNull(nodeCollection.find(
                    or(
                            eq(MongodbNodeProperty._id.name(), new ObjectId(getId())),
                            eq(MongodbNodeProperty._id.name(), getId())
                    )).first());
        }
        if (this instanceof MongodbRelationship) {
            return Objects.nonNull(edgeCollection.find(
                    or(
                            eq(MongodbNodeProperty._id.name(), new ObjectId(getId())),
                            eq(MongodbNodeProperty._id.name(), getId())
                    )).first());
        }
        return false;
    }

    protected void persist() {
        if (isCreate) {
            if (!isDelete) {
                if (this instanceof MongodbNode) {
                    nodeCollection.insertOne(entity);
                }
                if (this instanceof MongodbRelationship) {
                    edgeCollection.insertOne(entity);
                }
            }
            isCreate = false;
        } else if (isDelete) {
            if (this instanceof MongodbNode) {
                nodeCollection.deleteOne(eq(MongodbNodeProperty._id.name(), entity.get(MongodbNodeProperty._id.name())));
            }
            if (this instanceof MongodbRelationship) {
                edgeCollection.deleteOne(eq(MongodbEdgeProperty._id.name(), entity.get(MongodbEdgeProperty._id.name())));
            }
        } else if (isUpdate) {
            if (this instanceof MongodbNode) {
                nodeCollection.findOneAndReplace(eq(MongodbNodeProperty._id.name(), entity.get(MongodbNodeProperty._id.name())), entity);
            }
            if (this instanceof MongodbRelationship) {
                edgeCollection.findOneAndReplace(eq(MongodbEdgeProperty._id.name(), entity.get(MongodbEdgeProperty._id.name())), entity);
            }
            isUpdate = false;
        }
    }

    @Override
    public String toJson() {
        return entity.toJson();
    }

    @Override
    public String toString() {
        return toJson();
    }

    /**
     * 需要忽略的key值
     *
     * @return 需要忽略的key值
     */
    abstract Set<String> ignoreKeys();

    @Override
    public BaseMongodbEntityImpl clone() throws CloneNotSupportedException {
        return (BaseMongodbEntityImpl) super.clone();
    }
}
