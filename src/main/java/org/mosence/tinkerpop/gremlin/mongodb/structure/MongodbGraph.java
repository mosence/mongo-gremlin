package org.mosence.tinkerpop.gremlin.mongodb.structure;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbFactory;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbGraphAPI;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbNode;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbRelationship;
import org.mosence.tinkerpop.gremlin.mongodb.process.computer.MongodbGraphComputer;
import org.mosence.tinkerpop.gremlin.mongodb.process.traversal.strategy.optimization.MongodbGraphStepStrategy;
import org.mosence.tinkerpop.gremlin.mongodb.structure.feature.MongodbGremlinFeatures;
import org.mosence.tinkerpop.gremlin.mongodb.structure.trait.DefaultMongodbTrait;
import org.mosence.tinkerpop.gremlin.mongodb.structure.trait.MongodbTrait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_COMPUTER)
@Graph.OptIn("org.mosence.tinkerpop.gremlin.mongodb.MongodbSuite")
@Graph.OptOut(test="org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest",method = "modern_V_out_out_profile",reason = "MongodbGraph do not support profile !")
@Graph.OptOut(test="org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest",method = "modern_V_out_out_profileXmetricsX",reason = "MongodbGraph do not support profile !")
@Graph.OptOut(test="org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest",method = "grateful_V_out_out_profile",reason = "MongodbGraph do not support profile !")
@Graph.OptOut(test="org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest",method = "grateful_V_out_out_profileXmetricsX",reason = "MongodbGraph do not support profile !")
@Graph.OptOut(test="org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest",method = "g_V_sideEffectXThread_sleepX10XX_sideEffectXThread_sleepX5XX_profile",reason = "MongodbGraph do not support profile !")
@Graph.OptOut(test="org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest",method = "g_V_sideEffectXThread_sleepX10XX_sideEffectXThread_sleepX5XX_profileXmetricsX",reason = "MongodbGraph do not support profile !")
@Graph.OptOut(test="org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest",method = "g_V_repeat_both_profile",reason = "MongodbGraph do not support profile !")
@Graph.OptOut(test="org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest",method = "g_V_repeat_both_profileXmetricsX",reason = "MongodbGraph do not support profile !")
@Graph.OptOut(test="org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest",method = "g_V_whereXinXcreatedX_count_isX1XX_name_profile",reason = "MongodbGraph do not support profile !")
@Graph.OptOut(test="org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest",method = "g_V_whereXinXcreatedX_count_isX1XX_name_profileXmetricsX",reason = "MongodbGraph do not support profile !")
@Graph.OptOut(test="org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest",method = "testProfileStrategyCallback",reason = "MongodbGraph do not support profile !")
@Graph.OptOut(test="org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest",method = "testProfileStrategyCallbackSideEffect",reason = "MongodbGraph do not support profile !")
@Graph.OptOut(test="org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest",method = "g_V_matchXa_created_b__b_in_count_isXeqX1XXX_selectXa_bX_profile",reason = "MongodbGraph do not support profile !")
@Graph.OptOut(test="org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest",method = "g_V_matchXa_created_b__b_in_count_isXeqX1XXX_selectXa_bX_profileXmetricsX",reason = "MongodbGraph do not support profile !")
@Graph.OptOut(test="org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest",method = "g_V_hasLabelXpersonX_pageRank_byXrankX_byXbothEX_rank_profile",reason = "MongodbGraph do not support profile !")
@Graph.OptOut(test="org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest",method = "g_V_groupXmX_profile",reason = "MongodbGraph do not support profile !")
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

    private Features features = new MongodbGremlinFeatures();

    private MongodbGraphAPI baseGraph;
    private BaseConfiguration configuration = new BaseConfiguration();

    public static final String CONFIG_URI = "gremlin.mongodb.uri";
    public static final String CONFIG_EDGE_COLLECTION = "gremlin.mongo.edge.collection";
    public static final String CONFIG_NODE_COLLECTION = "gremlin.mongo.node.collection";
    public static final String CONFIG_DATABASE = "gremlin.mongo.graph.database";

    private MongodbGraphVariables mongodbGraphVariables;

    private MongodbTrait trait;

    private void initialize(final MongodbGraphAPI baseGraph, final Configuration configuration) {
        this.configuration.copy(configuration);
        this.baseGraph = baseGraph;
        this.mongodbGraphVariables = new MongodbGraphVariables(this);
        this.trait = DefaultMongodbTrait.instance();
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
        final MongodbVertex vertex = new MongodbVertex(this.baseGraph.createNode(ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL).split(MongodbVertex.LABEL_DELIMINATOR)), this);
        ElementHelper.attachProperties(vertex, keyValues);
        return vertex;
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
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
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
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
    }

    public MongodbTrait getTrait() {
        return this.trait;
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) {
        if (!graphComputerClass.equals(MongodbGraphComputer.class)){
            throw Graph.Exceptions.graphDoesNotSupportProvidedGraphComputer(graphComputerClass);
        }
        return (C) new MongodbGraphComputer(this);
        //throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() {
        return new MongodbGraphComputer(this);
        //throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public Transaction tx() {
        throw Graph.Exceptions.transactionsNotSupported();
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
}
