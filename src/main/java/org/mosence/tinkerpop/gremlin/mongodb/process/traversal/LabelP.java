package org.mosence.tinkerpop.gremlin.mongodb.process.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.mosence.tinkerpop.gremlin.mongodb.structure.MongodbVertex;

import java.io.Serializable;
import java.util.function.BiPredicate;

/**
 * @author MoSence
 */
public final class LabelP extends P<String> {

    private LabelP(final String label) {
        super(LabelBiPredicate.instance(), label);
    }

    public static P<String> of(final String label) {
        return new LabelP(label);
    }

    public static final class LabelBiPredicate implements BiPredicate<String, String>, Serializable {

        private static final LabelBiPredicate INSTANCE = new LabelBiPredicate();

        private LabelBiPredicate() {
        }

        @Override
        public boolean test(final String labels, final String checkLabel) {
            return labels.equals(checkLabel) || labels.contains(MongodbVertex.LABEL_DELIMINATOR + checkLabel) || labels.contains(checkLabel + MongodbVertex.LABEL_DELIMINATOR);
        }

        public static LabelBiPredicate instance() {
            return INSTANCE;
        }
    }

}
