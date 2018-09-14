package org.mosence.tinkerpop.gremlin.mongodb.structure;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbEntity;

/**
 * @author MoSence
 */
@SuppressWarnings("unchecked")
public final class MongodbProperty<V> implements Property<V> {

    protected final Element element;
    protected final String key;
    protected final MongodbGraph graph;
    protected V value;
    protected boolean removed = false;

    public MongodbProperty(final Element element, final String key, final V value) {
        this.element = element;
        this.key = key;
        this.value = value;
        this.graph = element instanceof MongodbVertexProperty ?
                ((MongodbVertex) (((MongodbVertexProperty) element).element())).graph :
                ((BaseMongodbElement) element).graph;
    }

    @Override
    public Element element() {
        return this.element;
    }

    @Override
    public void remove() {
        if (this.removed) {
            return;
        }
        this.removed = true;
        this.graph.tx().readWrite();
        final MongodbEntity entity = this.element instanceof MongodbVertexProperty ?
                ((MongodbVertexProperty) this.element).vertexPropertyNode :
                ((BaseMongodbElement) this.element).getBaseElement();
        if (entity.hasProperty(this.key)) {
            entity.removeProperty(this.key);
        }
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public V value() {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return null != this.value;
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }
}