package org.mosence.tinkerpop.gremlin.mongodb.structure;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedElement;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbEntity;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author MoSence
 */
public abstract class BaseMongodbElement implements Element, WrappedElement<MongodbEntity> {
    protected final MongodbGraph graph;
    protected final MongodbEntity baseElement;

    public BaseMongodbElement(final MongodbEntity baseElement, final MongodbGraph graph) {
        this.baseElement = baseElement;
        this.graph = graph;
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public Object id() {
        this.graph.tx().readWrite();
        return this.baseElement.getId();
    }

    @Override
    public Set<String> keys() {
        this.graph.tx().readWrite();
        final Set<String> keys = new HashSet<>();
        for (final String key : this.baseElement.getKeys()) {
            if (!Graph.Hidden.isHidden(key) && !ignoreKeys().contains(key)) {
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
        return ElementHelper.hashCode(this);
    }

    @Override
    public MongodbEntity getBaseElement() {
        return this.baseElement;
    }

    /**
     * 需要忽略的key值
     * @return 需要忽略的key值
     */
    abstract Set<String> ignoreKeys();
}
