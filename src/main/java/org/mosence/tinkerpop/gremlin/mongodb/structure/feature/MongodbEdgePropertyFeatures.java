package org.mosence.tinkerpop.gremlin.mongodb.structure.feature;

import org.apache.tinkerpop.gremlin.structure.Graph.Features.EdgePropertyFeatures;

/**
 * @author MoSence
 */
public class MongodbEdgePropertyFeatures implements EdgePropertyFeatures {
    MongodbEdgePropertyFeatures(){}

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
