package org.mosence.tinkerpop.gremlin.mongodb;

import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
import org.apache.tinkerpop.gremlin.process.traversal.CoreTraversalTest;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.ComplexTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.*;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.*;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IncidentToAdjacentStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategyProcessTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

public class MongodbSuite extends AbstractGremlinSuite {
    private static final Class<?>[] allTests = new Class<?>[]{
            // branch
            BranchTest.Traversals.class,
            ChooseTest.Traversals.class,
            OptionalTest.Traversals.class,
            LocalTest.Traversals.class,
            RepeatTest.Traversals.class,
            UnionTest.Traversals.class,

            // filter
            AndTest.Traversals.class,
            CoinTest.Traversals.class,
            CyclicPathTest.Traversals.class,
            DedupTest.Traversals.class,
            DropTest.Traversals.class,
            FilterTest.Traversals.class,
            HasTest.Traversals.class,
            IsTest.Traversals.class,
            OrTest.Traversals.class,
            RangeTest.Traversals.class,
            SampleTest.Traversals.class,
            SimplePathTest.Traversals.class,
            TailTest.Traversals.class,
            WhereTest.Traversals.class,

            // map
            AddEdgeTest.Traversals.class,
            AddVertexTest.Traversals.class,
            CoalesceTest.Traversals.class,
            ConstantTest.Traversals.class,
            CountTest.Traversals.class,
            FlatMapTest.Traversals.class,
            FoldTest.Traversals.class,
            GraphTest.Traversals.class,
            LoopsTest.Traversals.class,
            MapTest.Traversals.class,
            MatchTest.CountMatchTraversals.class,
            MatchTest.GreedyMatchTraversals.class,
            MathTest.Traversals.class,
            MaxTest.Traversals.class,
            MeanTest.Traversals.class,
            MinTest.Traversals.class,
            SumTest.Traversals.class,
            OrderTest.Traversals.class,
            PathTest.Traversals.class,
            //ProfileTest.Traversals.class,
            ProjectTest.Traversals.class,
            PropertiesTest.Traversals.class,
            SelectTest.Traversals.class,
            VertexTest.Traversals.class,
            UnfoldTest.Traversals.class,
            ValueMapTest.Traversals.class,

            // sideEffect
            AggregateTest.Traversals.class,
            ExplainTest.Traversals.class,
            GroupTest.Traversals.class,
            GroupCountTest.Traversals.class,
            InjectTest.Traversals.class,
            SackTest.Traversals.class,
            SideEffectCapTest.Traversals.class,
            SideEffectTest.Traversals.class,
            StoreTest.Traversals.class,
            SubgraphTest.Traversals.class,
            TreeTest.Traversals.class,

            // compliance
            ComplexTest.Traversals.class,
            CoreTraversalTest.class,
            TraversalInterruptionTest.class,

            // creations
            TranslationStrategyProcessTest.class,

            // decorations
            ElementIdStrategyProcessTest.class,
            EventStrategyProcessTest.class,
            ReadOnlyStrategyProcessTest.class,
            PartitionStrategyProcessTest.class,
            SubgraphStrategyProcessTest.class,

            // optimizations
            IncidentToAdjacentStrategyProcessTest.class
    };

    private static final Class<?>[] testsToEnforce = new Class<?>[]{
            // branch
            BranchTest.class,
            ChooseTest.class,
            OptionalTest.class,
            LocalTest.class,
            RepeatTest.class,
            UnionTest.class,

            // filter
            AndTest.class,
            CoinTest.class,
            CyclicPathTest.class,
            DedupTest.class,
            DropTest.class,
            FilterTest.class,
            HasTest.class,
            IsTest.class,
            OrTest.class,
            RangeTest.class,
            SampleTest.class,
            SimplePathTest.class,
            TailTest.class,
            WhereTest.class,

            // map
            AddEdgeTest.class,
            AddVertexTest.class,
            CoalesceTest.class,
            ConstantTest.class,
            CountTest.class,
            FlatMapTest.class,
            FoldTest.class,
            LoopsTest.class,
            MapTest.class,
            MatchTest.class,
            MathTest.class,
            MaxTest.class,
            MeanTest.class,
            MinTest.class,
            SumTest.class,
            OrderTest.class,
            PathTest.class,
            PropertiesTest.class,
            ProfileTest.class,
            ProjectTest.class,
            SelectTest.class,
            VertexTest.class,
            UnfoldTest.class,
            ValueMapTest.class,

            // sideEffect
            AggregateTest.class,
            GroupTest.class,
            GroupCountTest.class,
            InjectTest.class,
            SackTest.class,
            SideEffectCapTest.class,
            SideEffectTest.class,
            StoreTest.class,
            SubgraphTest.class,
            TreeTest.class,
    };

    /**
     * @param klass test
     * @param builder test
     * @throws InitializationError test
     */
    public MongodbSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, allTests, testsToEnforce, false, TraversalEngine.Type.STANDARD);
    }

    /**
     * @param klass test
     * @param builder test
     * @param testsToExecute test
     * @throws InitializationError test
     */
    public MongodbSuite(final Class<?> klass, final RunnerBuilder builder, final Class<?>[] testsToExecute) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce, true, TraversalEngine.Type.STANDARD);
    }
}
