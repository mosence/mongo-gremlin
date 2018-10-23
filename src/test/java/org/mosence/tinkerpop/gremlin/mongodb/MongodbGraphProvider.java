package org.mosence.tinkerpop.gremlin.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.mosence.tinkerpop.gremlin.mongodb.process.computer.*;
import org.mosence.tinkerpop.gremlin.mongodb.structure.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author MoSence
 */
public class MongodbGraphProvider extends AbstractGraphProvider {
    private static final Set<Class> IMPLEMENTATIONS = new HashSet<Class>() {{
        add(MongodbEdge.class);
        add(BaseMongodbElement.class);
        add(MongodbGraph.class);
        add(MongodbGraphVariables.class);
        add(MongodbProperty.class);
        add(MongodbVertex.class);
        add(MongodbVertexProperty.class);
        add(MongodbGraphComputer.class);
        add(MongodbGraphMapEmitter.class);
        add(MongodbGraphMemory.class);
        add(MongodbGraphMessageBoard.class);
        add(MongodbGraphMessenger.class);
        add(MongodbGraphReduceEmitter.class);
    }};

    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {
        if (null != graph) {
            graph.close();
        }
        if (null != configuration && configuration.containsKey(MongodbGraph.CONFIG_URI)
                && configuration.containsKey(MongodbGraph.CONFIG_EDGE_COLLECTION)
                && configuration.containsKey(MongodbGraph.CONFIG_NODE_COLLECTION)
                && configuration.containsKey(MongodbGraph.CONFIG_DATABASE)) {
            MongoClient mongoClient = MongoClients.create(configuration.getString(MongodbGraph.CONFIG_URI));
            mongoClient.getDatabase(configuration.getString(MongodbGraph.CONFIG_DATABASE)).drop();
            mongoClient.close();
        }
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATIONS;
    }

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        return new HashMap<String, Object>() {{
            put(Graph.GRAPH, MongodbGraph.class.getName());
            put(MongodbGraph.CONFIG_URI, "mongodb://localhost:27017/gremlin");
            put(MongodbGraph.CONFIG_EDGE_COLLECTION, "edge");
            put(MongodbGraph.CONFIG_NODE_COLLECTION, "node");
            put(MongodbGraph.CONFIG_DATABASE, "gremlin");
        }};
    }
}