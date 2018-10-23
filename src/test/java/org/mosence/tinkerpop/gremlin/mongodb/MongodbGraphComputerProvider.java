package org.mosence.tinkerpop.gremlin.mongodb;

import org.apache.tinkerpop.gremlin.GraphProvider;
import org.mosence.tinkerpop.gremlin.mongodb.process.computer.MongodbGraphComputer;

/**
 * @author MoSence
 */
@GraphProvider.Descriptor(computer = MongodbGraphComputer.class)
public class MongodbGraphComputerProvider extends MongodbGraphProvider{
}
