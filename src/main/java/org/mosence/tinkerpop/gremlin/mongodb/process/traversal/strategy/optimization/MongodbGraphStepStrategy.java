package org.mosence.tinkerpop.gremlin.mongodb.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.mosence.tinkerpop.gremlin.mongodb.process.traversal.step.side.MongodbGraphStep;

/**
 * @author MoSence
 */
@SuppressWarnings("unchecked")
public final class MongodbGraphStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final MongodbGraphStepStrategy INSTANCE = new MongodbGraphStepStrategy();

    private MongodbGraphStepStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        for (final GraphStep originalGraphStep : TraversalHelper.getStepsOfClass(GraphStep.class, traversal)) {
            final MongodbGraphStep<?, ?> mongodbGraphStep = new MongodbGraphStep<>(originalGraphStep);
            TraversalHelper.replaceStep(originalGraphStep, mongodbGraphStep, traversal);
            Step<?, ?> currentStep = mongodbGraphStep.getNextStep();
            while (currentStep instanceof HasStep || currentStep instanceof NoOpBarrierStep) {
                if (currentStep instanceof HasStep) {
                    for (final HasContainer hasContainer : ((HasContainerHolder) currentStep).getHasContainers()) {
                        if (!GraphStep.processHasContainerIds(mongodbGraphStep, hasContainer)) {
                            mongodbGraphStep.addHasContainer(hasContainer);
                        }
                    }
                    TraversalHelper.copyLabels(currentStep, currentStep.getPreviousStep(), false);
                    traversal.removeStep(currentStep);
                }
                currentStep = currentStep.getNextStep();
            }
        }
    }

    public static MongodbGraphStepStrategy instance() {
        return INSTANCE;
    }

}