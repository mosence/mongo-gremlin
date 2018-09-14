package org.mosence.tinkerpop.gremlin.mongodb.api;

import org.mosence.tinkerpop.gremlin.mongodb.api.impl.exception.NotFoundException;

import java.util.Iterator;
import java.util.Map;

/**
 * Mongodb 图Api
 * @author MoSence
 */
public interface MongodbGraphAPI {

    /**
     * 新增点
     * @param labels 点标签
     * @return 点
     */
    MongodbNode createNode(String... labels);

    /**
     * 查找点
     * @param id 点ID
     * @return 点
     * @throws NotFoundException 抛出找不到点错误
     */
    MongodbNode getNodeById(String id) throws NotFoundException;

    /**
     * 查找边
     * @param id 边ID
     * @return 边
     * @throws NotFoundException 抛出找不到边错误
     */
    MongodbRelationship getRelationshipById(String id) throws NotFoundException;

    /**
     * 关闭图连接
     */
    void shutdown();

    /**
     * 获取全部点
     * @return 点列表
     */
    Iterable<MongodbNode> allNodes();

    /**
     * 获取全部边
     * @return 边列表
     */
    Iterable<MongodbRelationship> allRelationships();

    /**
     * 通过标签查找点
     * @param label 标签
     * @return 点列表
     */
    Iterable<MongodbNode> findNodes(String label);

    /**
     * 通过标签和属性值查找点
     * @param label 标签
     * @param property 属性key
     * @param value 属性value
     * @return 点列表
     */
    Iterable<MongodbNode> findNodes(String label, String property, Object value);

    /**
     * 图事务
     * @return 获取mongodb图事务
     */
    MongodbTx tx();

    /**
     * 使用mongodb查询
     * @param query 查询语句
     * @param params 参数
     * @return 查询结果
     */
    Iterator<Map<String, Object>> execute(String query, Map<String, Object> params);

    /**
     * 判断索引是否存在
     * @param label 标签
     * @param property 属性key
     * @return 是否存在索引
     */
    boolean hasSchemaIndex(String label, String property);

    /**
     * 获取属性key列表
     * @return 属性key列表
     */
    Iterable<String> getKeys();

    /**
     * 获取属性value
     * @param key 属性key
     * @return 属性value
     */
    Object getProperty(String key);

    /**
     * 判断是否存在属性
     * @param key 属性key
     * @return 是否存在属性
     */
    boolean hasProperty(String key);

    /**
     * 移除属性
     * @param key 属性key
     * @return 被移除的属性value
     */
    Object removeProperty(String key);

    /**
     * 设置属性
     * @param key 属性key
     * @param value 属性value
     */
    void setProperty(String key, Object value);
}
