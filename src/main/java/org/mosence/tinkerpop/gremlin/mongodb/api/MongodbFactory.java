package org.mosence.tinkerpop.gremlin.mongodb.api;

import java.util.Map;

/**
 * mongodb graph 连接工厂
 * @author MoSence
 */
public interface MongodbFactory{

    /**
     * 创建一个基于mongodb的图
     * mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]
     * @param uri mongodb远程连接地址
     * @param config 配置
     * @return MongodbGraphAPI
     */
    MongodbGraphAPI newGraphDatabase(String uri, Map<String, String> config);

    String DEFAULT_IMPL_CLASS = "org.mosence.tinkerpop.gremlin.mongodb.api.impl.MongodbFactoryImpl";

    public static class Builder {
        public static MongodbGraphAPI open(String uri, Map<String, String> config) {
            try {
                MongodbFactory factory = (MongodbFactory) Class.forName(DEFAULT_IMPL_CLASS).newInstance();
                return factory.newGraphDatabase(uri, config);
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                throw new RuntimeException("Error instantiating Mongodb Database for "+uri,e);
            }
        }
    }
}
