package org.mosence.tinkerpop.gremlin.mongodb.api.impl;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.apache.commons.lang.StringUtils;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbFactory;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbGraphAPI;

import java.util.Map;

/**
 * Mongo连接工厂实现
 * @author MoSence
 */
public class MongodbFactoryImpl implements MongodbFactory {

    private static final String NODE_COLLECTION_KEY = "gremlin.mongo.node.collection";
    private static final String EDGE_COLLECTION_KEY = "gremlin.mongo.edge.collection";
    private static final String GRAPH_DATABASE = "gremlin.mongo.graph.database";

    @Override
    public MongodbGraphAPI newGraphDatabase(String uri, Map<String, String> config) {
        try {
            String nodeCollection = config.get(NODE_COLLECTION_KEY);
            String edgeCollection = config.get(EDGE_COLLECTION_KEY);
            String database = config.get(GRAPH_DATABASE);
            if(StringUtils.isBlank(nodeCollection) || StringUtils.isBlank(edgeCollection)){
                throw new RuntimeException("Error create mongo graph, need config 'gremlin.mongo.node.collection' and 'gremlin.mongo.edge.collection'");
            }
            return new MongodbGraphApiImpl(createMongoClient(uri),database, nodeCollection, edgeCollection);
        }catch (Exception e){
            throw new RuntimeException("Error handling mongoClient "+ uri, e);
        }
    }

    private MongoClient createMongoClient(String uri){
        return MongoClients.create(uri);
    }


}
