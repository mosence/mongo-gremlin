package org.mosence.tinkerpop.gremlin.mongodb.structure;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbNode;

import java.util.*;

/**
 * @author MoSence
 */
@SuppressWarnings("unchecked")
public final class MongodbVertexProperty<V> implements VertexProperty<V> {

    protected final MongodbVertex vertex;
    protected final String key;
    protected final V value;
    protected MongodbNode vertexPropertyNode;

    public MongodbVertexProperty(final MongodbVertex vertex, final String key, final V value) {
        this.vertex = vertex;
        this.key = key;
        this.value = value;
        this.vertexPropertyNode = null;
    }

    public MongodbVertexProperty(final MongodbVertex vertex, final String key, final V value, final MongodbNode vertexPropertyNode) {
        this.vertex = vertex;
        this.key = key;
        this.value = value;
        this.vertexPropertyNode = vertexPropertyNode;
    }

    public MongodbVertexProperty(final MongodbVertex vertex, final MongodbNode vertexPropertyNode) {
        this.vertex = vertex;
        this.key = (String) vertexPropertyNode.getProperty(T.key.getAccessor());
        this.value = (V) vertexPropertyNode.getProperty(T.value.getAccessor());
        this.vertexPropertyNode = vertexPropertyNode;
    }

    @Override
    public Vertex element() {
        return this.vertex;
    }

    @Override
    public Object id() {
        // TODO: Mongodb needs a better ID system for VertexProperties
        return (long) (this.key.hashCode() + this.value.hashCode() + this.vertex.id().hashCode());
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public V value() throws NoSuchElementException {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return null != this.value;
    }

    @Override
    public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
        this.vertex.graph.tx().readWrite();
        return this.vertex.graph.trait.getProperties(this, propertyKeys);
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        this.vertex.graph.tx().readWrite();
        ElementHelper.validateProperty(key, value);
        return this.vertex.graph.trait.setProperty(this, key, value);
    }

    @Override
    public void remove() {
        this.vertex.graph.tx().readWrite();
        this.vertex.graph.trait.removeVertexProperty(this);
        this.vertexPropertyNode= null;
    }

    @Override
    public Set<String> keys() {
        if(null == this.vertexPropertyNode) {
            return Collections.emptySet();
        }
        final Set<String> keys = new HashSet<>();
        for (final String key : this.vertexPropertyNode.getKeys()) {
            if (!Graph.Hidden.isHidden(key) && !key.equals(this.key)) {
                keys.add(key);
            }
        }
        return Collections.unmodifiableSet(keys);
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode((Element) this);
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }
}