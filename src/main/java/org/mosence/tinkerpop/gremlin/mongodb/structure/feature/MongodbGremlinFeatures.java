package org.mosence.tinkerpop.gremlin.mongodb.structure.feature;

import org.apache.tinkerpop.gremlin.structure.Graph.Features;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * @author MoSence
 */
public class MongodbGremlinFeatures implements Features {

    private GraphFeatures graphFeatures = new MongodbGraphFeatures();
    private VertexFeatures vertexFeatures = new MongodbVertexFeatures();
    private EdgeFeatures edgeFeatures = new MongodbEdgeFeatures();

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
}
