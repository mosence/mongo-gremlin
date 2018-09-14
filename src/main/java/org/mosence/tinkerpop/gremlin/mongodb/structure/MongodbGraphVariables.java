package org.mosence.tinkerpop.gremlin.mongodb.structure;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphVariableHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbGraphAPI;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * @author MoSence
 */
@SuppressWarnings("unchecked")
public final class MongodbGraphVariables  implements Graph.Variables {

    private final MongodbGraph graph;
    private final MongodbGraphAPI baseGraph;

    protected MongodbGraphVariables(final MongodbGraph graph) {
        this.graph = graph;
        baseGraph = graph.getBaseGraph();
    }

    @Override
    public Set<String> keys() {
        this.graph.tx().readWrite();
        final Set<String> keys = new HashSet<>();
        for (final String key : this.baseGraph.getKeys()) {
            if (!Graph.Hidden.isHidden(key)) {
                keys.add(key);
            }
        }
        return keys;
    }

    @Override
    public <R> Optional<R> get(final String key) {
        this.graph.tx().readWrite();
        return this.baseGraph.hasProperty(key) ?
                Optional.of((R) this.baseGraph.getProperty(key)) :
                Optional.<R>empty();
    }

    @Override
    public void set(final String key, final Object value) {
        GraphVariableHelper.validateVariable(key, value);
        this.graph.tx().readWrite();
        try {
            this.baseGraph.setProperty(key, value);
        } catch (final IllegalArgumentException e) {
            throw Graph.Variables.Exceptions.dataTypeOfVariableValueNotSupported(value, e);
        }
    }

    @Override
    public void remove(final String key) {
        this.graph.tx().readWrite();
        if (this.baseGraph.hasProperty(key)) {
            this.baseGraph.removeProperty(key);
        }
    }

    @Override
    public String toString() {
        return StringFactory.graphVariablesString(this);
    }

    public static class MongodbVariableFeatures implements Graph.Features.VariableFeatures {
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
}
