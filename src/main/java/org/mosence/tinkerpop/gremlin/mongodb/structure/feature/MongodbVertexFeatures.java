package org.mosence.tinkerpop.gremlin.mongodb.structure.feature;

import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;

/**
 * @author MoSence
 */
public class MongodbVertexFeatures extends MongodbElementFeatures implements VertexFeatures {

    private final VertexPropertyFeatures vertexPropertyFeatures = new MongodbVertexPropertyFeatures();

    MongodbVertexFeatures() {
    }

    @Override
    public VertexPropertyFeatures properties() {
        return vertexPropertyFeatures;
    }

    @Override
    public boolean supportsMetaProperties() {
        return false;
    }

    @Override
    public boolean supportsMultiProperties() {
        return false;
    }

    @Override
    public boolean supportsUserSuppliedIds() {
        return false;
    }

    @Override
    public Cardinality getCardinality(final String key) {
        return Cardinality.single;
    }
}
