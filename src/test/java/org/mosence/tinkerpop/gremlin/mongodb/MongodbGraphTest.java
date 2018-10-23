package org.mosence.tinkerpop.gremlin.mongodb;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;
import org.mosence.tinkerpop.gremlin.mongodb.structure.MongodbGraph;

@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = MongodbGraphProvider.class, graph = MongodbGraph.class)
public class MongodbGraphTest{
}