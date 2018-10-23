package org.mosence.tinkerpop.gremlin.mongodb.process.computer;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.tinkerpop.gremlin.process.computer.*;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;
import org.apache.tinkerpop.gremlin.process.computer.util.DefaultComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.mosence.tinkerpop.gremlin.mongodb.process.traversal.strategy.optimization.MongodbGraphStepStrategy;
import org.mosence.tinkerpop.gremlin.mongodb.process.util.ComputerSubmissionHelper;
import org.mosence.tinkerpop.gremlin.mongodb.structure.MongodbGraph;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;

/**
 * @author MoSence
 */
public class MongodbGraphComputer implements GraphComputer {

    static {
        TraversalStrategies.GlobalCache.registerStrategies(MongodbGraphComputer.class,
                TraversalStrategies.GlobalCache.getStrategies(GraphComputer.class).clone()
                        .addStrategies(MongodbGraphStepStrategy.instance()));
    }

    private final ThreadFactory threadFactoryBoss = new BasicThreadFactory.Builder().namingPattern(MongodbGraphComputer.class.getSimpleName() + "-boss").build();
    /**
     * An {@code ExecutorService} that schedules up background work. Since a {@link GraphComputer} is only used once
     * for a {@link VertexProgram} a single threaded executor is sufficient.
     */
    private final ExecutorService computerService = Executors.newSingleThreadExecutor(threadFactoryBoss);

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
        validateStatePriorToExecution();
        return ComputerSubmissionHelper.runWithBackgroundThread(this::submitWithExecutor, "MongodbSubmitter");
    }
    private Future<ComputerResult> submitWithExecutor(Executor exec) {
        this.memory = new MongodbGraphMemory(vertexProgram, mapReducers);
        // create the completable future
        final Future<ComputerResult> result = computerService.submit(() -> {
            //4.Create the Memory to be used for the computation.
            //5.Execute the VertexProgram.setup() method once and only once.
            if(null!=this.vertexProgram){
                this.vertexProgram.setup(this.memory);
                // 6.Execute the VertexProgram.execute() method for each vertex.
                while(true){
                    this.memory.completeSubRound();
                    this.mongodbGraph.vertices().forEachRemaining(vertex ->
                            this.vertexProgram.execute(ComputerGraph.vertexProgram(vertex, vertexProgram)
                                    , new MongodbGraphMessenger<>(vertex, this.messageBoard, vertexProgram.getMessageCombiner())
                                    , this.memory));
                    //7.Execute the VertexProgram.terminate() method once and if true, repeat VertexProgram.execute().
                    this.messageBoard.completeIteration();
                    this.memory.completeSubRound();
                    if(this.vertexProgram.terminate(this.memory)){
                        memory.incrIteration();
                        break;
                    }else{
                        memory.incrIteration();
                    }
                }
            }
            //8.When VertexProgram.terminate() returns true, move to MapReduce job execution.
            for (final MapReduce mapReduce : mapReducers) {
                final MongodbGraphMapEmitter<?, ?> mapEmitter = new MongodbGraphMapEmitter<>(mapReduce.doStage(MapReduce.Stage.REDUCE));
                final MongodbGraphReduceEmitter<?, ?> reduceEmitter = new MongodbGraphReduceEmitter<>();
                //9.MapReduce jobs are not required to be executed in any specified order.
                //10.For each Vertex, execute MapReduce.map(). Then (if defined) execute MapReduce.combine() and MapReduce.reduce().
                mapEmitter.reduceMap.forEach((key, value) -> {
                    this.mongodbGraph.vertices().forEachRemaining(vertex -> mapReduce.map(vertex, mapEmitter));
                    mapReduce.reduce(key, value.iterator(), reduceEmitter);
                });
                mapEmitter.complete(mapReduce);
                //11.Update Memory with runtime information.
                mapReduce.addResultToMemory(this.memory, reduceEmitter.reduceQueue.iterator());
            }
            this.memory.complete();
            //12.Construct a new ComputerResult containing the compute Graph and Memory.
            Graph lastGraph = this.mongodbGraph;
            if(Persist.NOTHING.equals(this.persist)){
                if(!ResultGraph.ORIGINAL.equals(this.resultGraph)){
                    lastGraph = EmptyGraph.instance();
                }
            } else if(Persist.VERTEX_PROPERTIES.equals(this.persist)){
                if(!ResultGraph.ORIGINAL.equals(this.resultGraph)){
                    // TODO
                }
            } else if(Persist.EDGES.equals(this.persist)){
                if(!ResultGraph.ORIGINAL.equals(this.resultGraph)){
                    // TODO
                }
            }
            return new DefaultComputerResult(lastGraph, this.memory.asImmutable());
        });
        computerService.shutdown();
        return result;
    }

    private void validateStatePriorToExecution() {
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
        // if the user didn't set desired persistence/result graph, then get from vertex program or else, no persistence
        this.resultGraph = GraphComputerHelper.getResultGraphState(Optional.ofNullable(this.vertexProgram), Optional.ofNullable(this.resultGraph));
        this.persist = GraphComputerHelper.getPersistState(Optional.ofNullable(this.vertexProgram), Optional.ofNullable(this.persist));
        // determine persistence and result graph options
        if (!this.features().supportsResultGraphPersistCombination(this.resultGraph, this.persist)) {
            throw Exceptions.resultGraphPersistCombinationNotSupported(this.resultGraph, this.persist);
        }
        // if too many workers are requested, throw appropriate exception
        if (this.workers > this.features().getMaxWorkers()) {
            throw Exceptions.computerRequiresMoreWorkersThanSupported(this.workers, this.features().getMaxWorkers());
        }
    }
}
