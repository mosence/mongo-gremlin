package org.mosence.tinkerpop.gremlin.mongodb.process.computer;

import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.concurrent.Future;

public class MongodbGraphComputer implements GraphComputer {
    @Override
    public GraphComputer result(ResultGraph resultGraph) {
        return null;
    }

    @Override
    public GraphComputer persist(Persist persist) {
        return null;
    }

    @Override
    public GraphComputer program(VertexProgram vertexProgram) {
        return null;
    }

    @Override
    public GraphComputer mapReduce(MapReduce mapReduce) {
        return null;
    }

    @Override
    public GraphComputer workers(int workers) {
        return null;
    }

    @Override
    public GraphComputer vertices(Traversal<Vertex, Vertex> vertexFilter) throws IllegalArgumentException {
        return null;
    }

    @Override
    public GraphComputer edges(Traversal<Vertex, Edge> edgeFilter) throws IllegalArgumentException {
        return null;
    }

    @Override
    public GraphComputer configure(String key, Object value) {
        return null;
    }

    @Override
    public Future<ComputerResult> submit() {
        return null;
    }

    @Override
    public Features features() {
        return null;
    }
}
