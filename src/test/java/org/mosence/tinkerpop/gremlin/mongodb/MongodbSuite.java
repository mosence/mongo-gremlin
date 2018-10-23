package org.mosence.tinkerpop.gremlin.mongodb;

import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

public class MongodbSuite extends AbstractGremlinSuite {
    private static final Class<?>[] allTests = new Class<?>[]{
    };

    private static final Class<?>[] testsToEnforce = new Class<?>[]{
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
