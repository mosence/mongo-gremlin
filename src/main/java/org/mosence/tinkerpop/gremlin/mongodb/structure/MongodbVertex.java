package org.mosence.tinkerpop.gremlin.mongodb.structure;

import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbNode;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbRelationship;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.tinkerpop.gremlin.structure.Direction.*;

/**
 * @author MoSence
 */
@SuppressWarnings("unchecked")
public final class MongodbVertex extends BaseMongodbElement implements Vertex, WrappedVertex<MongodbNode> {

    public static final String LABEL_DELIMINATOR = "::";

    public MongodbVertex(final MongodbNode node, final MongodbGraph graph) {
        super(node, graph);
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        if (null == inVertex) {
            throw Graph.Exceptions.argumentCanNotBeNull("inVertex");
        }
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (ElementHelper.getIdValue(keyValues).isPresent()) {
            throw Edge.Exceptions.userSuppliedIdsNotSupported();
        }

        final MongodbNode node = (MongodbNode) this.baseElement;
        final MongodbEdge edge = new MongodbEdge(node.connectTo(((MongodbVertex) inVertex).getBaseVertex(), label), this.graph);
        ElementHelper.attachProperties(edge, keyValues);
        return edge;
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        return this.property(VertexProperty.Cardinality.single, key, value);
    }

    @Override
    public void remove() {
        this.graph.getTrait().removeVertex(this);
    }

    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        ElementHelper.validateProperty(key, value);
        if (ElementHelper.getIdValue(keyValues).isPresent()) {
            throw Vertex.Exceptions.userSuppliedIdsNotSupported();
        }
        return this.graph.getTrait().setVertexProperty(this, cardinality, key, value, keyValues);
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        return this.graph.getTrait().getVertexProperty(this, key);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        return this.graph.getTrait().getVertexProperties(this, propertyKeys);
    }

    @Override
    public MongodbNode getBaseVertex() {
        return (MongodbNode) this.baseElement;
    }

    @Override
    public String label() {
        return String.join(LABEL_DELIMINATOR, this.labels());
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        return new Iterator<Vertex>() {
            final Iterator<MongodbRelationship> relationshipIterator = IteratorUtils.filter(0 == edgeLabels.length ?
                    BOTH == direction ?
                            IteratorUtils.concat(getBaseVertex().relationships(MongodbHelper.mapDirection(OUT)).iterator(),
                                    getBaseVertex().relationships(MongodbHelper.mapDirection(IN)).iterator()) :
                            getBaseVertex().relationships(MongodbHelper.mapDirection(direction)).iterator() :
                    BOTH == direction ?
                            IteratorUtils.concat(getBaseVertex().relationships(MongodbHelper.mapDirection(OUT), (edgeLabels)).iterator(),
                                    getBaseVertex().relationships(MongodbHelper.mapDirection(IN), (edgeLabels)).iterator()) :
                            getBaseVertex().relationships(MongodbHelper.mapDirection(direction), (edgeLabels)).iterator(), graph.getTrait().getRelationshipPredicate());

            @Override
            public boolean hasNext() {
                return this.relationshipIterator.hasNext();
            }

            @Override
            public MongodbVertex next() {
                try {
                    return new MongodbVertex(this.relationshipIterator.next().other(getBaseVertex()), graph);
                } catch (Exception ex) {
                    throw new TraversalInterruptedException();
                }
            }
        };
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        return new Iterator<Edge>() {
            final Iterator<MongodbRelationship> relationshipIterator = IteratorUtils.filter(0 == edgeLabels.length ?
                    BOTH == direction ?
                            IteratorUtils.concat(getBaseVertex().relationships(MongodbHelper.mapDirection(OUT)).iterator(),
                                    getBaseVertex().relationships(MongodbHelper.mapDirection(IN)).iterator()) :
                            getBaseVertex().relationships(MongodbHelper.mapDirection(direction)).iterator() :
                    BOTH == direction ?
                            IteratorUtils.concat(getBaseVertex().relationships(MongodbHelper.mapDirection(OUT), (edgeLabels)).iterator(),
                                    getBaseVertex().relationships(MongodbHelper.mapDirection(IN), (edgeLabels)).iterator()) :
                            getBaseVertex().relationships(MongodbHelper.mapDirection(direction), (edgeLabels)).iterator(), graph.getTrait().getRelationshipPredicate());

            @Override
            public boolean hasNext() {
                return this.relationshipIterator.hasNext();
            }

            @Override
            public MongodbEdge next() {
                try {
                    return new MongodbEdge(this.relationshipIterator.next(), graph);
                } catch (Exception ex) {
                    throw new TraversalInterruptedException();
                }
            }
        };
    }

    public Set<String> labels() {
        final Set<String> labels = new TreeSet<>(this.getBaseVertex().labels());
        return Collections.unmodifiableSet(labels);
    }

    public void addLabel(final String label) {
        this.getBaseVertex().addLabel(label);
    }

    public void removeLabel(final String label) {
        this.getBaseVertex().removeLabel(label);
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    Set<String> ignoreKeys() {
        return MongodbNode.IGNORE_KEYS;
    }
}
