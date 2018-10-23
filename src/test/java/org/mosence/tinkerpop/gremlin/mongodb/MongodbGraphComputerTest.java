package org.mosence.tinkerpop.gremlin.mongodb;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessComputerSuite;
import org.junit.runner.RunWith;
import org.mosence.tinkerpop.gremlin.mongodb.structure.MongodbGraph;

@RunWith(ProcessComputerSuite.class)
@GraphProviderClass(provider = MongodbGraphComputerProvider.class, graph = MongodbGraph.class)
public class MongodbGraphComputerTest {
}
