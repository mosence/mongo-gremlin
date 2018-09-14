package org.mosence.tinkerpop.gremlin.mongodb.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbNode;

/**
 * @author MoSence
 */
public final class MongodbHelper {

    private static final String NOT_FOUND_EXCEPTION = "NotFoundException";

    private MongodbHelper() {
    }

    public static org.mosence.tinkerpop.gremlin.mongodb.api.MongodbDirection mapDirection(final Direction direction) {
        if (direction.equals(Direction.OUT)) {
            return org.mosence.tinkerpop.gremlin.mongodb.api.MongodbDirection.OUTGOING;
        } else if (direction.equals(Direction.IN)) {
            return org.mosence.tinkerpop.gremlin.mongodb.api.MongodbDirection.INCOMING;
        } else {
            return org.mosence.tinkerpop.gremlin.mongodb.api.MongodbDirection.BOTH;
        }
    }

    public static boolean isDeleted(final MongodbNode node) {
        try {
            node.getKeys();
            return false;
        } catch (final RuntimeException e) {
            if (isNotFound(e)) {
                return true;
            } else {
                throw e;
            }
        }
    }

    public static boolean isNotFound(final RuntimeException ex) {
        return ex.getClass().getSimpleName().equals(NOT_FOUND_EXCEPTION);
    }

    public static MongodbNode getVertexPropertyNode(final MongodbVertexProperty vertexProperty) {
        return vertexProperty.vertexPropertyNode;
    }

    public static void setVertexPropertyNode(final MongodbVertexProperty vertexProperty, final MongodbNode node) {
        vertexProperty.vertexPropertyNode = node;
    }
}
