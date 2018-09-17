package org.mosence.tinkerpop.gremlin.mongodb.structure.feature;

import org.apache.tinkerpop.gremlin.structure.Graph.Features.VariableFeatures;

/**
 * @author MoSence
 */
public class MongodbVariableFeatures implements VariableFeatures{
    MongodbVariableFeatures(){}
    @Override
    public boolean supportsBooleanValues() {
        return true;
    }

    @Override
    public boolean supportsDoubleValues() {
        return true;
    }

    @Override
    public boolean supportsFloatValues() {
        return true;
    }

    @Override
    public boolean supportsIntegerValues() {
        return true;
    }

    @Override
    public boolean supportsLongValues() {
        return true;
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
    public boolean supportsByteValues() {
        return false;
    }

    @Override
    public boolean supportsBooleanArrayValues() {
        return true;
    }

    @Override
    public boolean supportsByteArrayValues() {
        return false;
    }

    @Override
    public boolean supportsDoubleArrayValues() {
        return true;
    }

    @Override
    public boolean supportsFloatArrayValues() {
        return true;
    }

    @Override
    public boolean supportsIntegerArrayValues() {
        return true;
    }

    @Override
    public boolean supportsLongArrayValues() {
        return true;
    }

    @Override
    public boolean supportsStringArrayValues() {
        return true;
    }

    @Override
    public boolean supportsSerializableValues() {
        return false;
    }

    @Override
    public boolean supportsStringValues() {
        return true;
    }

    @Override
    public boolean supportsUniformListValues() {
        return false;
    }
}
