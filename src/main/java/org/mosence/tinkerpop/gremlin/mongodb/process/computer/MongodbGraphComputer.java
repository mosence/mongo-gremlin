package org.mosence.tinkerpop.gremlin.mongodb.process.computer;

import org.apache.tinkerpop.gremlin.process.computer.*;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;
import org.apache.tinkerpop.gremlin.process.computer.util.DefaultComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbGraphAPI;
import org.mosence.tinkerpop.gremlin.mongodb.structure.MongodbGraph;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * @author MoSence
 */
public class MongodbGraphComputer implements GraphComputer {

    private MongodbGraphAPI mongodbGraphAPI;
    private final MongodbGraph mongodbGraph;
    private ResultGraph resultGraph;
    private Persist persist;
    private VertexProgram<?> vertexProgram;
    private Set<MapReduce> mapReducers = new HashSet<>();
    private int workers = Runtime.getRuntime().availableProcessors();
    private GraphFilter graphFilter = new GraphFilter();
    private MongodbGraphMemory memory = null;
    private MongodbGraphMessageBoard messageBoard = new MongodbGraphMessageBoard<>();
    private boolean executed = false;

    public MongodbGraphComputer(MongodbGraph mongodbGraph) {
        this.mongodbGraph = mongodbGraph;
    }

    @Override
    public String toString() {
        return StringFactory.graphComputerString(this);
    }

    @Override
    public GraphComputer result(ResultGraph resultGraph) {
        this.resultGraph = resultGraph;
        return this;
    }

    @Override
    public GraphComputer persist(Persist persist) {
        this.persist = persist;
        return this;
    }

    @Override
    public GraphComputer program(VertexProgram vertexProgram) {
        this.vertexProgram = vertexProgram;
        return this;
    }

    @Override
    public GraphComputer mapReduce(MapReduce mapReduce) {
        this.mapReducers.add(mapReduce);
        return this;
    }

    @Override
    public GraphComputer workers(int workers) {
        this.workers = workers;
        return this;
    }

    @Override
    public GraphComputer vertices(Traversal<Vertex, Vertex> vertexFilter) throws IllegalArgumentException {
        this.graphFilter.setVertexFilter(vertexFilter);
        return this;
    }

    @Override
    public GraphComputer edges(Traversal<Vertex, Edge> edgeFilter) throws IllegalArgumentException {
        this.graphFilter.setEdgeFilter(edgeFilter);
        return this;
    }

    @Override
    public Future<ComputerResult> submit() {
        this.resultGraph = GraphComputerHelper.getResultGraphState(Optional.ofNullable(this.vertexProgram), Optional.ofNullable(this.resultGraph));
        this.persist = GraphComputerHelper.getPersistState(Optional.ofNullable(this.vertexProgram), Optional.ofNullable(this.persist));

        //1.Ensure the GraphComputer has not already been executed.
        if (this.executed) {
            throw Exceptions.computerHasAlreadyBeenSubmittedAVertexProgram();
        } else {
            this.executed = true;
        }
        //2.Ensure that at least there is a VertexProgram or 1 MapReduce job.
        if (null == this.vertexProgram && this.mapReducers.isEmpty()) {
            throw Exceptions.computerHasNoVertexProgramNorMapReducers();
        }
        //3.If there is a VertexProgram, validate that it can execute on the GraphComputer given the respectively defined features.
        if (null != this.vertexProgram) {
            GraphComputerHelper.validateProgramOnComputer(this, this.vertexProgram);
            this.mapReducers.addAll(this.vertexProgram.getMapReducers());
        }
        //4.Create the Memory to be used for the computation.
        this.memory = new MongodbGraphMemory(vertexProgram, mapReducers);
        //5.Execute the VertexProgram.setup() method once and only once.
        this.vertexProgram.setup(this.memory);
        // 6.Execute the VertexProgram.execute() method for each vertex.
        do {
            this.mongodbGraph.vertices().forEachRemaining(vertex ->
                    this.vertexProgram.execute(ComputerGraph.vertexProgram(vertex, vertexProgram)
                            , new MongodbGraphMessenger<>(vertex, this.messageBoard, vertexProgram.getMessageCombiner())
                            , this.memory));
            //7.Execute the VertexProgram.terminate() method once and if true, repeat VertexProgram.execute().
        } while (this.vertexProgram.terminate(this.memory));
        //8.When VertexProgram.terminate() returns true, move to MapReduce job execution.
        for (final MapReduce mapReduce : mapReducers) {
            final MongodbGraphMapEmitter<?, ?> mapEmitter = new MongodbGraphMapEmitter(mapReduce.doStage(MapReduce.Stage.REDUCE));
            final MongodbGraphReduceEmitter<?, ?> reduceEmitter = new MongodbGraphReduceEmitter();
            //9.MapReduce jobs are not required to be executed in any specified order.
            //10.For each Vertex, execute MapReduce.map(). Then (if defined) execute MapReduce.combine() and MapReduce.reduce().
            mapEmitter.reduceMap.entrySet().forEach(entry -> {
                this.mongodbGraph.vertices().forEachRemaining(vertex -> {
                    mapReduce.map(vertex, mapEmitter);
                });
                mapReduce.reduce(entry.getKey(), entry.getValue().iterator(), reduceEmitter);
            });
            mapEmitter.complete(mapReduce);
            //11.Update Memory with runtime information.
            mapReduce.addResultToMemory(this.memory, reduceEmitter.reduceQueue.iterator());
        }
        //12.Construct a new ComputerResult containing the compute Graph and Memory.
        DefaultComputerResult result = new DefaultComputerResult(this.mongodbGraph, this.memory.asImmutable());
        return CompletableFuture.completedFuture(result);
    }
}
