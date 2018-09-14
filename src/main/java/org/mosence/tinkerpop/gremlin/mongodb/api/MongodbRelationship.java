package org.mosence.tinkerpop.gremlin.mongodb.api;

import org.mosence.tinkerpop.gremlin.mongodb.api.impl.property.MongodbEdgeProperty;

import java.util.HashSet;
import java.util.Set;

/**
 * 边
 * @author MoSence
 */
public interface MongodbRelationship  extends MongodbEntity{

    Set<String> IGNORE_KEYS = new HashSet<String>(){
        {
            add(MongodbEdgeProperty._id.name());
            add(MongodbEdgeProperty.type.name());
            add(MongodbEdgeProperty.source.name());
            add(MongodbEdgeProperty.target.name());
        }
    };

    /**
     * 关系类型
     * @return 关系类型
     */
    String type();

    /**
     * 获取关系开始点ID
     * @return 关系开始点ID
     */
    String startId();

    /**
     * 获取关系开始点
     * @return 关系开始点
     */
    MongodbNode start();

    /**
     * 获取关系结束点ID
     * @return 关系结束点ID
     */
    String endId();
    /**
     * 获取关系结束点
     * @return 关系结束点
     */
    MongodbNode end();

    /**
     * 获取关系的另外一个方向点
     * @param node 开始/结束点
     * @return 结束/开始点
     */
    MongodbNode other(MongodbNode node);
}
