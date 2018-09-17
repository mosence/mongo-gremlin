package org.mosence.tinkerpop.gremlin.mongodb.structure.feature;

import org.apache.tinkerpop.gremlin.structure.Graph.Features.ElementFeatures;

/**
 * @author MoSence
 */
public class MongodbElementFeatures implements ElementFeatures {
    MongodbElementFeatures() {
    }

    @Override
    public boolean supportsUserSuppliedIds() {
        return false;
    }

    @Override
    public boolean supportsNumericIds() {
        return false;
    }

    @Override
    public boolean supportsStringIds() {
        return true;
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
