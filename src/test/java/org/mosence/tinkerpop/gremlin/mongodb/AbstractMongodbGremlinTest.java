package org.mosence.tinkerpop.gremlin.mongodb;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbGraphAPI;
import org.mosence.tinkerpop.gremlin.mongodb.structure.MongodbGraph;

import static org.junit.Assert.assertEquals;

public abstract class AbstractMongodbGremlinTest extends AbstractGremlinTest {
    protected MongodbGraph getGraph() {
        return (MongodbGraph) this.graph;
    }

    protected MongodbGraphAPI getBaseGraph() {
        return ((MongodbGraph) this.graph).getBaseGraph();
    }

    protected void validateCounts(int gV, int gE, int gN, int gR) {
        assertEquals(gV, IteratorUtils.count(graph.vertices()));
        assertEquals(gE, IteratorUtils.count(graph.edges()));
        assertEquals(gN, IteratorUtils.count(this.getBaseGraph().allNodes()));
        assertEquals(gR, IteratorUtils.count(this.getBaseGraph().allRelationships()));
    }
}
