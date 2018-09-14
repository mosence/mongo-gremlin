package org.mosence.tinkerpop.gremlin.mongodb.api;

import org.mosence.tinkerpop.gremlin.mongodb.api.impl.property.MongodbNodeProperty;

import java.util.HashSet;
import java.util.Set;

/**
 * mongodb 点
 * @author MoSence
 */
public interface MongodbNode extends MongodbEntity{
    Set<String> IGNORE_KEYS = new HashSet<String>(){
        {
            add(MongodbNodeProperty._id.name());
            add(MongodbNodeProperty.type.name());
            add(MongodbNodeProperty.label.name());
        }
    };
    /**
     * 获取标签
     * @return 标签集合
     */
    Set<String> labels();

    /**
     * 获取主要类型
     * @return 主要类型
     */
    String type();

    /**
     * 获取主要类型
     * @return 主要类型
     */
    String name();

    /**
     * 判断是否存在标签
     * @param label 标签
     * @return 是否存在标签
     */
    boolean hasLabel(String label);

    /**
     * 添加标签
     * @param label 标签
     */
    void addLabel(String label);

    /**
     * 移除标签
     * @param label 标签
     */
    void removeLabel(String label);

    /**
     * 获取度
     * @param direction 关系方向
     * @param type 关系类型
     * @return 出入度值
     */
    int degree(MongodbDirection direction, String type);

    /**
     * 获取边
     * @param direction 方向
     * @param types 关系类型
     * @return 边
     */
    Iterable<MongodbRelationship> relationships(MongodbDirection direction, String...types);

    /**
     * 获取连接到另外一个点的边
     * @param node 另一个点
     * @param type 关系类型
     * @return 边
     */
    MongodbRelationship connectTo(MongodbNode node, String type);
}
