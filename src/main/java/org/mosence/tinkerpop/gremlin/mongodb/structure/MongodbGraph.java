package org.mosence.tinkerpop.gremlin.mongodb.structure;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.TransactionException;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.mosence.tinkerpop.gremlin.mongodb.api.*;
import org.mosence.tinkerpop.gremlin.mongodb.process.traversal.strategy.optimization.MongodbGraphStepStrategy;
import org.mosence.tinkerpop.gremlin.mongodb.structure.trait.DefaultMongodbTrait;
import org.mosence.tinkerpop.gremlin.mongodb.structure.trait.MongodbTrait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * @author MoSence
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn("org.mosence.tinkerpop.gremlin.mongodb.MongodbSuite")
@SuppressWarnings("unchecked")
public final class MongodbGraph implements Graph, WrappedGraph<MongodbGraphAPI> {
    public static final Logger LOGGER = LoggerFactory.getLogger(MongodbGraph.class);

    static {
        TraversalStrategies.GlobalCache.registerStrategies(MongodbGraph.class,
                TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone()
                        .addStrategies(MongodbGraphStepStrategy.instance())
                        /*.addStrategies(NoOpBarrierStepStrategy.instance())*/);
    }

    private static final Configuration EMPTY_CONFIGURATION = new BaseConfiguration() {{
        this.setProperty(Graph.GRAPH, MongodbGraph.class.getName());
    }};

    protected Features features = new MongodbGraphFeatures();

    protected MongodbGraphAPI baseGraph;
    protected BaseConfiguration configuration = new BaseConfiguration();

    public static final String CONFIG_URI = "gremlin.mongodb.uri";
    public static final String CONFIG_EDGE_COLLECTION = "gremlin.mongo.edge.collection";
    public static final String CONFIG_NODE_COLLECTION = "gremlin.mongo.node.collection";
    public static final String CONFIG_DATABASE = "gremlin.mongo.graph.database";

    private final MongodbTransaction mongodbTransaction = new MongodbTransaction();
    private MongodbGraphVariables mongodbGraphVariables;

    protected MongodbTrait trait;

    private void initialize(final MongodbGraphAPI baseGraph, final Configuration configuration) {
        this.configuration.copy(configuration);
        this.baseGraph = baseGraph;
        this.mongodbGraphVariables = new MongodbGraphVariables(this);
        this.tx().readWrite();
        this.trait = DefaultMongodbTrait.instance();
        this.tx().commit();
    }

    protected MongodbGraph(final MongodbGraphAPI baseGraph, final Configuration configuration) {
        this.initialize(baseGraph, configuration);
    }

    protected MongodbGraph(final Configuration configuration) {
        this.configuration.copy(configuration);
        final String directory = this.configuration.getString(CONFIG_URI);
        final Map mongodbSpecificConfig = ConfigurationConverter.getMap(this.configuration);
        this.baseGraph = MongodbFactory.Builder.open(directory, mongodbSpecificConfig);
        this.initialize(this.baseGraph, configuration);
    }

    /**
     * Open a new {@link MongodbGraph} instance.
     *
     * @param configuration the configuration for the instance
     * @return a newly opened {@link org.apache.tinkerpop.gremlin.structure.Graph}
     */
    public static MongodbGraph open(final Configuration configuration) {
        if (null == configuration) {
            throw Exceptions.argumentCanNotBeNull("configuration");
        }
        if (!configuration.containsKey(CONFIG_URI)) {
            throw new IllegalArgumentException(String.format("Mongodb configuration requires that the %s be set", CONFIG_URI));
        }
        return new MongodbGraph(configuration);
    }

    /**
     * Construct a MongodbGraph instance by specifying the directory to create the database in..
     */
    public static MongodbGraph open(final String uri) {
        final Configuration config = new BaseConfiguration();
        config.setProperty(CONFIG_URI, uri);
        return open(config);
    }

    /**
     * Construct a MongodbGraph instance using an existing Mongodb raw instance.
     */
    public static MongodbGraph open(final MongodbGraphAPI baseGraph) {
        return new MongodbGraph(baseGraph, EMPTY_CONFIGURATION);
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (ElementHelper.getIdValue(keyValues).isPresent()) {
            throw Vertex.Exceptions.userSuppliedIdsNotSupported();
        }
        this.tx().readWrite();
        final MongodbVertex vertex = new MongodbVertex(this.baseGraph.createNode(ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL).split(MongodbVertex.LABEL_DELIMINATOR)), this);
        ElementHelper.attachProperties(vertex, keyValues);
        return vertex;
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        this.tx().readWrite();
        try{
            final Predicate<MongodbNode> nodePredicate = this.trait.getNodePredicate();
            if (0 == vertexIds.length) {
                return IteratorUtils.stream(this.getBaseGraph().allNodes())
                        .filter(nodePredicate)
                        .map(node -> (Vertex) new MongodbVertex(node, this)).iterator();
            } else {
                ElementHelper.validateMixedElementIds(Vertex.class, vertexIds);
                return Stream.of(vertexIds)
                        .map(id -> {
                            if (id instanceof Number) {
                                return String.valueOf(id);
                            } else if (id instanceof String) {
                                return id.toString();
                            } else if (id instanceof Vertex) {
                                return ((Vertex) id).id().toString();
                            } else {
                                throw new IllegalArgumentException("Unknown vertex id type: " + id);
                            }
                        })
                        .flatMap(id -> {
                            try {
                                return Stream.of(this.baseGraph.getNodeById(id));
                            } catch (final RuntimeException e) {
                                if (MongodbHelper.isNotFound(e)) {
                                    return Stream.empty();
                                }
                                throw e;
                            }
                        })
                        .filter(nodePredicate)
                        .map(node -> (Vertex) new MongodbVertex(node, this)).iterator();
            }
        }catch (Exception ex){
            ((MongodbTransaction)this.tx()).error(ex);
            throw new TraversalInterruptedException();
        }
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        this.tx().readWrite();
        try{
            final Predicate<MongodbRelationship> relationshipPredicate = this.trait.getRelationshipPredicate();
            if (0 == edgeIds.length) {
                return IteratorUtils.stream(this.getBaseGraph().allRelationships())
                        .filter(relationshipPredicate)
                        .map(relationship -> (Edge) new MongodbEdge(relationship, this)).iterator();
            } else {
                ElementHelper.validateMixedElementIds(Edge.class, edgeIds);
                return Stream.of(edgeIds)
                        .map(id -> {
                            if (id instanceof Number) {
                                return String.valueOf(id);
                            } else if (id instanceof String) {
                                return id.toString();
                            } else if (id instanceof Edge) {
                                return ((Edge) id).id().toString();
                            } else {
                                throw new IllegalArgumentException("Unknown edge id type: " + id);
                            }
                        })
                        .flatMap(id -> {
                            try {
                                return Stream.of(this.baseGraph.getRelationshipById(id));
                            } catch (final RuntimeException e) {
                                if (MongodbHelper.isNotFound(e)) {
                                    return Stream.empty();
                                }
                                throw e;
                            }
                        })
                        .filter(relationshipPredicate)
                        .map(relationship -> (Edge) new MongodbEdge(relationship, this)).iterator();
            }
        }catch (Exception ex){
            ((MongodbTransaction)this.tx()).error(ex);
            throw new TraversalInterruptedException();
        }
    }

    public MongodbTrait getTrait() {
        return this.trait;
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public Transaction tx() {
        return this.mongodbTransaction;
    }

    @Override
    public Variables variables() {
        return this.mongodbGraphVariables;
    }

    @Override
    public Configuration configuration() {
        return this.configuration;
    }

    /**
     * This implementation of {@code close} will also close the current transaction on the thread, but it
     * is up to the caller to deal with dangling transactions in other threads prior to calling this method.
     */
    @Override
    public void close() throws Exception {
        this.tx().close();
        if (this.baseGraph != null) {
            this.baseGraph.shutdown();
        }
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, baseGraph.toString());
    }

    @Override
    public Features features() {
        return features;
    }

    @Override
    public MongodbGraphAPI getBaseGraph() {
        return this.baseGraph;
    }

    /**
     * Execute the Cypher query and get the result set as a {@link GraphTraversal}.
     *
     * @param query the Cypher query to execute
     * @return a fluent Gremlin traversal
     */
    public <S, E> GraphTraversal<S, E> cypher(final String query) {
        return cypher(query, Collections.emptyMap());
    }

    /**
     * Execute the Cypher query with provided parameters and get the result set as a {@link GraphTraversal}.
     *
     * @param query      the Cypher query to execute
     * @param parameters the parameters of the Cypher query
     * @return a fluent Gremlin traversal
     */
    public <S, E> GraphTraversal<S, E> cypher(final String query, final Map<String, Object> parameters) {
        this.tx().readWrite();
        final GraphTraversal.Admin<S, E> traversal = new DefaultGraphTraversal<>(this);
        //TODO traversal.addStep(new CypherStartStep(traversal, query, new MongodbCypherIterator<>((Iterator) this.baseGraph.execute(query, parameters), this)));
        return traversal;
    }

    class MongodbTransaction extends AbstractThreadLocalTransaction {

        protected final ThreadLocal<MongodbTx> threadLocalTx = ThreadLocal.withInitial(() -> null);

        public MongodbTransaction() {
            super(MongodbGraph.this);
        }

        public void error(Throwable throwable){
            threadLocalTx.get().error(throwable);
        }

        @Override
        public void doOpen() {
            threadLocalTx.set(getBaseGraph().tx());
        }

        @Override
        public void doCommit() throws TransactionException {
            try (MongodbTx tx = threadLocalTx.get()) {
                tx.success();
            } catch (Exception ex) {
                throw new TransactionException(ex);
            } finally {
                threadLocalTx.remove();
            }
        }

        @Override
        public void doRollback() throws TransactionException {
            try (MongodbTx tx = threadLocalTx.get()) {
                tx.failure();
            } catch (Exception e) {
                throw new TransactionException(e);
            } finally {
                threadLocalTx.remove();
            }
        }

        @Override
        public boolean isOpen() {
            return (threadLocalTx.get() != null);
        }
    }

    public class MongodbGraphFeatures implements Features {
        protected GraphFeatures graphFeatures = new MongodbGraphGraphFeatures();
        protected VertexFeatures vertexFeatures = new MongodbVertexFeatures();
        protected EdgeFeatures edgeFeatures = new MongodbEdgeFeatures();

        @Override
        public GraphFeatures graph() {
            return graphFeatures;
        }

        @Override
        public VertexFeatures vertex() {
            return vertexFeatures;
        }

        @Override
        public EdgeFeatures edge() {
            return edgeFeatures;
        }

        @Override
        public String toString() {
            return StringFactory.featureString(this);
        }

        public class MongodbGraphGraphFeatures implements GraphFeatures {

            private VariableFeatures variableFeatures = new MongodbGraphVariables.MongodbVariableFeatures();

            MongodbGraphGraphFeatures() {
            }

            @Override
            public boolean supportsConcurrentAccess() {
                return false;
            }

            @Override
            public boolean supportsComputer() {
                return true;
            }

            @Override
            public VariableFeatures variables() {
                return variableFeatures;
            }

            @Override
            public boolean supportsThreadedTransactions() {
                return false;
            }
        }

        public class MongodbVertexFeatures extends MongodbElementFeatures implements VertexFeatures {

            private final VertexPropertyFeatures vertexPropertyFeatures = new MongodbVertexPropertyFeatures();

            protected MongodbVertexFeatures() {
            }

            @Override
            public VertexPropertyFeatures properties() {
                return vertexPropertyFeatures;
            }

            @Override
            public boolean supportsMetaProperties() {
                return trait.supportsMetaProperties();
            }

            @Override
            public boolean supportsMultiProperties() {
                return trait.supportsMultiProperties();
            }

            @Override
            public boolean supportsUserSuppliedIds() {
                return false;
            }

            @Override
            public VertexProperty.Cardinality getCardinality(final String key) {
                return trait.getCardinality(key);
            }
        }

        public class MongodbEdgeFeatures extends MongodbElementFeatures implements EdgeFeatures {

            private final EdgePropertyFeatures edgePropertyFeatures = new MongodbEdgePropertyFeatures();

            MongodbEdgeFeatures() {
            }

            @Override
            public EdgePropertyFeatures properties() {
                return edgePropertyFeatures;
            }
        }

        public class MongodbElementFeatures implements ElementFeatures {

            MongodbElementFeatures() {
            }

            @Override
            public boolean supportsUserSuppliedIds() {
                return false;
            }

            @Override
            public boolean supportsStringIds() {
                return false;
            }

            @Override
            public boolean supportsUuidIds() {
                return false;
            }

            @Override
            public boolean supportsAnyIds() {
                return false;
            }

            @Override
            public boolean supportsCustomIds() {
                return false;
            }
        }

        public class MongodbVertexPropertyFeatures implements VertexPropertyFeatures {

            MongodbVertexPropertyFeatures() {
            }

            @Override
            public boolean supportsMapValues() {
                return false;
            }

            @Override
            public boolean supportsMixedListValues() {
                return false;
            }

            @Override
            public boolean supportsSerializableValues() {
                return false;
            }

            @Override
            public boolean supportsUniformListValues() {
                return false;
            }

            @Override
            public boolean supportsUserSuppliedIds() {
                return false;
            }

            @Override
            public boolean supportsAnyIds() {
                return false;
            }
        }

        public class MongodbEdgePropertyFeatures implements EdgePropertyFeatures {

            MongodbEdgePropertyFeatures() {
            }

            @Override
            public boolean supportsMapValues() {
                return false;
            }

            @Override
            public boolean supportsMixedListValues() {
                return false;
            }

            @Override
            public boolean supportsSerializableValues() {
                return false;
            }

            @Override
            public boolean supportsUniformListValues() {
                return false;
            }
        }
    }
}
