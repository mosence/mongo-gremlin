package org.mosence.tinkerpop.gremlin.mongodb.api;

import org.bson.Document;

/**
 * @author MoSence
 */
public interface MongodbGraphComputerAPI {
    /**
     * 点表为基础的 map-reduce
     * @param mapFunction map javascript 脚本
     * @param reduceFunction reduce javascript 脚本
     * @return 结果Document
     */
    Iterable<Document> nodeMapReduce(String mapFunction, String reduceFunction);

    /**
     * 边表为基础的 map-reduce
     * @param mapFunction map javascript 脚本
     * @param reduceFunction reduce javascript 脚本
     * @return 结果Document
     */
    Iterable<Document> edgeMapReduce(String mapFunction, String reduceFunction);
}
