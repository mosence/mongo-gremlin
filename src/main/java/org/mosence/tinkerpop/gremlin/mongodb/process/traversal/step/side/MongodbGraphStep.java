package org.mosence.tinkerpop.gremlin.mongodb.process.traversal.step.side;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.mosence.tinkerpop.gremlin.mongodb.structure.MongodbGraph;

import java.util.*;

/**
 * @author MoSence
 */
@SuppressWarnings("unchecked")
public final class MongodbGraphStep<S, E extends Element> extends GraphStep<S, E> implements HasContainerHolder {

    private final List<HasContainer> hasContainers = new ArrayList<>();

    public MongodbGraphStep(final GraphStep<S, E> originalGraphStep) {
        super(originalGraphStep.getTraversal(), originalGraphStep.getReturnClass(), originalGraphStep.isStartStep(), originalGraphStep.getIds());
        originalGraphStep.getLabels().forEach(this::addLabel);
        this.setIteratorSupplier(() -> (Iterator<E>) (Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));
    }

    private Iterator<? extends Edge> edges() {
        if (null == this.ids) {
            return Collections.emptyIterator();
        }
        return IteratorUtils.filter(this.getTraversal().getGraph().get().edges(this.ids), edge -> HasContainer.testAll(edge, this.hasContainers));
    }

    private Iterator<? extends Vertex> vertices() {
        if (null == this.ids) {
            return Collections.emptyIterator();
        }
        final MongodbGraph graph = (MongodbGraph) this.getTraversal().getGraph().get();
        return graph.getTrait().lookupVertices(graph, this.hasContainers, this.ids);
    }

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty()) {
            return super.toString();
        } else {
            return 0 == this.ids.length ?
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), this.hasContainers) :
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), Arrays.toString(this.ids), this.hasContainers);
        }
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(this.hasContainers);
    }

    @Override
    public void addHasContainer(final HasContainer hasContainer) {
        if (hasContainer.getPredicate() instanceof AndP) {
            for (final P<?> predicate : ((AndP<?>) hasContainer.getPredicate()).getPredicates()) {
                this.addHasContainer(new HasContainer(hasContainer.getKey(), predicate));
            }
        } else {
            this.hasContainers.add(hasContainer);
        }
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.hasContainers.hashCode();
    }
}
