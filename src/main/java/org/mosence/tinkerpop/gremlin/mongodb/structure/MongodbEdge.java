package org.mosence.tinkerpop.gremlin.mongodb.structure;

import com.mongodb.MongoException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedEdge;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbRelationship;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author MoSence
 */
@SuppressWarnings("unchecked")
public final class MongodbEdge extends BaseMongodbElement implements Edge, WrappedEdge<MongodbRelationship> {

    private static final String PROPERTY_REPLACE_REGEX = "\\.";
    private static final String PROPERTY_REPLACE_CHAR = "_";
    public MongodbEdge(final MongodbRelationship relationship, final MongodbGraph graph) {
        super(relationship, graph);
    }

    @Override
    public Vertex outVertex() {
        return new MongodbVertex(this.getBaseEdge().start(), this.graph);
    }

    @Override
    public Vertex inVertex() {
        return new MongodbVertex(this.getBaseEdge().end(), this.graph);
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction) {
        this.graph.tx().readWrite();
        try{
            switch (direction) {
                case OUT:
                    return IteratorUtils.of(new MongodbVertex(this.getBaseEdge().start(), this.graph));
                case IN:
                    return IteratorUtils.of(new MongodbVertex(this.getBaseEdge().end(), this.graph));
                default:
                    return IteratorUtils.of(new MongodbVertex(this.getBaseEdge().start(), this.graph), new MongodbVertex(this.getBaseEdge().end(), this.graph));
            }
        }catch (MongoException ex){
            ((MongodbGraph.MongodbTransaction)graph().tx()).error(ex);
            throw new TraversalInterruptedException();
        }
    }

    @Override
    public void remove() {
        this.graph.tx().readWrite();
        try {
            this.baseElement.delete();
        } catch (IllegalStateException ignored) {
            // NotFoundException happens if the edge is committed
            // IllegalStateException happens if the edge is still chilling in the tx
        } catch (RuntimeException e) {
            if (!MongodbHelper.isNotFound(e)) {
                throw e;
            }
            // NotFoundException happens if the edge is committed
            // IllegalStateException happens if the edge is still chilling in the tx
        }
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    public String label() {
        this.graph.tx().readWrite();
        return this.getBaseEdge().type();
    }

    @Override
    public MongodbRelationship getBaseEdge() {
        return (MongodbRelationship) this.baseElement;
    }

    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        this.graph.tx().readWrite();
        Iterable<String> keys = this.baseElement.getKeys();
        List<String> keyList = Stream.of(propertyKeys).map(k->k.replaceAll(PROPERTY_REPLACE_REGEX,PROPERTY_REPLACE_CHAR)).collect(Collectors.toList());
        String[] keyArray = keyList.toArray(new String[0]);
        Iterator<String> filter = IteratorUtils.filter(keys.iterator(),
                key -> ElementHelper.keyExists(key, keyArray));
        return IteratorUtils.map(filter,
                key -> new MongodbProperty<>(this, key, (V) this.baseElement.getProperty(key)));
    }

    @Override
    public <V> Property<V> property(final String key) {
        this.graph.tx().readWrite();
        if (this.baseElement.hasProperty(key)) {
            return new MongodbProperty<>(this, key, (V) this.baseElement.getProperty(key));
        } else {
            return Property.empty();
        }
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        ElementHelper.validateProperty(key, value);
        this.graph.tx().readWrite();
        try {
            this.baseElement.setProperty(key, value);
            return new MongodbProperty<>(this, key, value);
        } catch (final IllegalArgumentException e) {
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value, e);
        }
    }

    @Override
    Set<String> ignoreKeys() {
        return MongodbRelationship.IGNORE_KEYS;
    }
}
