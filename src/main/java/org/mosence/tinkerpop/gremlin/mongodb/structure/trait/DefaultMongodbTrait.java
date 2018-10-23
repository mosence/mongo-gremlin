package org.mosence.tinkerpop.gremlin.mongodb.structure.trait;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbDirection;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbNode;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbRelationship;
import org.mosence.tinkerpop.gremlin.mongodb.process.traversal.LabelP;
import org.mosence.tinkerpop.gremlin.mongodb.structure.MongodbGraph;
import org.mosence.tinkerpop.gremlin.mongodb.structure.MongodbHelper;
import org.mosence.tinkerpop.gremlin.mongodb.structure.MongodbVertex;
import org.mosence.tinkerpop.gremlin.mongodb.structure.MongodbVertexProperty;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author MoSence
 */
@SuppressWarnings("unchecked")
public final class DefaultMongodbTrait implements MongodbTrait {

    private static final String PROPERTY_REPLACE_REGEX = "\\.";
    private static final String PROPERTY_REPLACE_CHAR = "_";
    private static final DefaultMongodbTrait INSTANCE = new DefaultMongodbTrait();

    private final static Predicate TRUE_PREDICATE = x -> true;

    public static DefaultMongodbTrait instance() {
        return INSTANCE;
    }

    private DefaultMongodbTrait() {

    }

    @Override
    public Predicate<MongodbNode> getNodePredicate() {
        return TRUE_PREDICATE;
    }

    @Override
    public Predicate<MongodbRelationship> getRelationshipPredicate() {
        return TRUE_PREDICATE;
    }

    @Override
    public void removeVertex(final MongodbVertex vertex) {
        try {
            final MongodbNode node = vertex.getBaseVertex();
            for (final MongodbRelationship relationship : node.relationships(MongodbDirection.BOTH)) {
                relationship.delete();
            }
            node.delete();
        } catch (final IllegalStateException ignored) {
            // this one happens if the vertex is still chilling in the tx
        } catch (final RuntimeException ex) {
            if (!MongodbHelper.isNotFound(ex)) {
                throw ex;
            }
            // this one happens if the vertex is committed
        }
    }

    @Override
    public <V> VertexProperty<V> getVertexProperty(final MongodbVertex vertex, final String key) {
        return vertex.getBaseVertex().hasProperty(key) ? new MongodbVertexProperty<>(vertex, key, (V) vertex.getBaseVertex().getProperty(key)) : VertexProperty.<V>empty();
    }

    @Override
    public <V> Iterator<VertexProperty<V>> getVertexProperties(final MongodbVertex vertex, final String... keys) {
        List<String> keyList = Stream.of(keys).map(k -> k.replaceAll(PROPERTY_REPLACE_REGEX, PROPERTY_REPLACE_CHAR)).collect(Collectors.toList());
        String[] keyArray = keyList.toArray(new String[0]);
        return (Iterator) IteratorUtils.stream(vertex.getBaseVertex().getKeys())
                .filter(key -> ElementHelper.keyExists(key, keyArray))
                .map(key -> new MongodbVertexProperty<>(vertex, key, (V) vertex.getBaseVertex().getProperty(key))).iterator();
    }

    @Override
    public <V> VertexProperty<V> setVertexProperty(final MongodbVertex vertex, final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        if (cardinality != VertexProperty.Cardinality.single) {
            throw VertexProperty.Exceptions.multiPropertiesNotSupported();
        }
        if (keyValues.length > 0) {
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        }
        try {
            vertex.getBaseVertex().setProperty(key, value);
            return new MongodbVertexProperty<>(vertex, key, value);
        } catch (final IllegalArgumentException iae) {
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value, iae);
        }
    }

    @Override
    public VertexProperty.Cardinality getCardinality(final String key) {
        return VertexProperty.Cardinality.single;
    }

    @Override
    public boolean supportsMultiProperties() {
        return false;
    }

    @Override
    public boolean supportsMetaProperties() {
        return false;
    }

    @Override
    public void removeVertexProperty(final MongodbVertexProperty vertexProperty) {
        final MongodbNode node = ((MongodbVertex) vertexProperty.element()).getBaseVertex();
        if (node.hasProperty(vertexProperty.key())) {
            node.removeProperty(vertexProperty.key());
        }
    }

    @Override
    public <V> Property<V> setProperty(final MongodbVertexProperty vertexProperty, final String key, final V value) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public <V> Property<V> getProperty(final MongodbVertexProperty vertexProperty, final String key) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public <V> Iterator<Property<V>> getProperties(final MongodbVertexProperty vertexProperty, final String... keys) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public Iterator<Vertex> lookupVertices(final MongodbGraph graph, final List<HasContainer> hasContainers, final Object... ids) {
        List<HasContainer> containers = preTansHasContainer(hasContainers);
        if (ids.length > 0) {
            return IteratorUtils.filter(graph.vertices(ids), vertex -> HasContainer.testAll(vertex, containers));
        }
        Optional<String> label = containers.stream()
                .filter(hasContainer -> hasContainer.getKey().equals(T.label.getAccessor()))
                .filter(hasContainer -> Compare.eq == hasContainer.getBiPredicate())
                .map(hasContainer -> (String) hasContainer.getValue())
                .findAny();
        if (!label.isPresent()) {
            label = containers.stream()
                    .filter(hasContainer -> hasContainer.getKey().equals(T.label.getAccessor()))
                    .filter(hasContainer -> hasContainer.getPredicate() instanceof LabelP)
                    .map(hasContainer -> (String) hasContainer.getValue())
                    .findAny();
        }

        if (label.isPresent()) {
            // find a vertex by label and key/value
            for (final HasContainer hasContainer : containers) {
                if (Compare.eq == hasContainer.getBiPredicate() && !hasContainer.getKey().equals(T.label.getAccessor())) {
                    if (graph.getBaseGraph().hasSchemaIndex(label.get(), hasContainer.getKey())) {
                        return IteratorUtils.stream(graph.getBaseGraph().findNodes(label.get(), hasContainer.getKey(), hasContainer.getValue()))
                                .map(node -> (Vertex) new MongodbVertex(node, graph))
                                .filter(vertex -> HasContainer.testAll(vertex, containers)).iterator();
                    }
                }
            }
            // find a vertex by label
            return IteratorUtils.stream(graph.getBaseGraph().findNodes(label.get()))
                    .map(node -> (Vertex) new MongodbVertex(node, graph))
                    .filter(vertex -> HasContainer.testAll(vertex, containers)).iterator();
        } else {
            // linear scan
            return IteratorUtils.filter(graph.vertices(), vertex -> HasContainer.testAll(vertex, containers));
        }
    }

    private List<HasContainer> preTansHasContainer(List<HasContainer> hasContainers) {
        return hasContainers.stream().map(hasContainer -> new HasContainer(hasContainer.getKey().replaceAll(PROPERTY_REPLACE_REGEX, PROPERTY_REPLACE_CHAR), hasContainer.getPredicate())).collect(Collectors.toList());
    }
}