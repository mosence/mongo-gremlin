package org.mosence.tinkerpop.gremlin.mongodb.structure.feature;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.EdgeFeatures;

/**
 * @author MoSence
 */
public class MongodbEdgeFeatures extends MongodbElementFeatures implements EdgeFeatures {
    MongodbEdgeFeatures(){}
    private MongodbEdgePropertyFeatures edgePropertyFeatures = new MongodbEdgePropertyFeatures();

    @Override
    public Graph.Features.EdgePropertyFeatures properties() {
        return edgePropertyFeatures;
    }
}
