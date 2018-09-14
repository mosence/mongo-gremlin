package org.mosence.tinkerpop.gremlin.mongodb.jsr233;

import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;
import org.mosence.tinkerpop.gremlin.mongodb.process.traversal.LabelP;
import org.mosence.tinkerpop.gremlin.mongodb.structure.*;

/**
 * @author MoSence
 */
public final class MongodbGremlinPlugin extends AbstractGremlinPlugin {

    private static final String NAME = "tinkerpop.mongodb";

    private static final ImportCustomizer IMPORTS;

    static {
        try {
            IMPORTS = DefaultImportCustomizer.build()
                    .addClassImports(MongodbEdge.class,
                            BaseMongodbElement.class,
                            MongodbGraph.class,
                            MongodbGraphVariables.class,
                            MongodbHelper.class,
                            MongodbProperty.class,
                            MongodbVertex.class,
                            MongodbVertexProperty.class,
                            LabelP.class)
                    .addMethodImports(LabelP.class.getMethod("of", String.class))
                    .create();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private static final MongodbGremlinPlugin INSTANCE = new MongodbGremlinPlugin();

    public MongodbGremlinPlugin() {
        super(NAME, IMPORTS);
    }

    public static MongodbGremlinPlugin instance() {
        return INSTANCE;
    }

    @Override
    public boolean requireRestart() {
        return true;
    }
}
