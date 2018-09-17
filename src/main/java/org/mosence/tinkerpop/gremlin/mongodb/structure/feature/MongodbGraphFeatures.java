package org.mosence.tinkerpop.gremlin.mongodb.structure.feature;

import org.apache.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VariableFeatures;

/**
 * @author MoSence
 */
public class MongodbGraphFeatures implements GraphFeatures {

    MongodbGraphFeatures(){}

    private VariableFeatures variableFeatures = new MongodbVariableFeatures();

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

    @Override
    public boolean supportsTransactions() {
        return false;
    }
}
