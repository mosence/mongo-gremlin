package org.mosence.tinkerpop.gremlin.mongodb.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

/**
 * @author MoSence
 */
public class NoOpBarrierStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy  {

    private static final NoOpBarrierStepStrategy INSTANCE = new NoOpBarrierStepStrategy();

    private NoOpBarrierStepStrategy() {
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        for (final NoOpBarrierStep noOpBarrierStep : TraversalHelper.getStepsOfClass(NoOpBarrierStep.class, traversal)) {
            if(!(noOpBarrierStep.getPreviousStep() instanceof Mutating) &&
                    !(noOpBarrierStep.getPreviousStep() instanceof VertexStep)){
                traversal.removeStep(noOpBarrierStep);
            }
        }
    }

    public static NoOpBarrierStepStrategy instance() {
        return INSTANCE;
    }
}
