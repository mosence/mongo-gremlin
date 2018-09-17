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
        return this.baseGraph.hasProperty(key) ?
                Optional.of((R) this.baseGraph.getProperty(key)) :
                Optional.<R>empty();
    }

    @Override
    public void set(final String key, final Object value) {
        GraphVariableHelper.validateVariable(key, value);
        try {
            this.baseGraph.setProperty(key, value);
        } catch (final IllegalArgumentException e) {
            throw Graph.Variables.Exceptions.dataTypeOfVariableValueNotSupported(value, e);
        }
    }

    @Override
    public void remove(final String key) {
        if (this.baseGraph.hasProperty(key)) {
            this.baseGraph.removeProperty(key);
        }
    }

    @Override
    public String toString() {
        return StringFactory.graphVariablesString(this);
    }

}
