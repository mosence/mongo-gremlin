package org.mosence.tinkerpop.gremlin.mongodb.structure.feature;

import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures;

/**
 * @author MoSence
 */
public class MongodbVertexPropertyFeatures implements VertexPropertyFeatures {

    MongodbVertexPropertyFeatures(){}

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
