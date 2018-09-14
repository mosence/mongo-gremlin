package org.mosence.tinkerpop.gremlin.mongodb.structure.trait;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbNode;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbRelationship;
import org.mosence.tinkerpop.gremlin.mongodb.structure.MongodbGraph;
import org.mosence.tinkerpop.gremlin.mongodb.structure.MongodbVertex;
import org.mosence.tinkerpop.gremlin.mongodb.structure.MongodbVertexProperty;

import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

/**
 * @author MoSence
 */
public interface MongodbTrait {
    /**
     * 点Predicate
     * @return 点Predicate
     */
    Predicate<MongodbNode> getNodePredicate();

    /**
     * 边Predicate
     * @return 边Predicate
     */
    Predicate<MongodbRelationship> getRelationshipPredicate();

    /**
     * 移除点
     * @param vertex 点
     */
    void removeVertex(final MongodbVertex vertex);

    /**
     * 获取点的属性
     * @param vertex 点
     * @param key 属性key
     * @param <V> 属性值类型
     * @return 属性
     */
    <V> VertexProperty<V> getVertexProperty(final MongodbVertex vertex, final String key);

    /**
     * 获取点的属性
     * @param vertex 点
     * @param keys 属性key列表
     * @param <V> 属性值类型
     * @return 属性列表
     */
    <V> Iterator getVertexProperties(final MongodbVertex vertex, final String... keys);

    /**
     * 设置点的属性
     * @param vertex 点
     * @param cardinality 值类型
     * @param key 属性key
     * @param value 属性值
     * @param keyValues 键值对
     * @param <V> 属性值类型
     * @return 属性
     */
    <V> VertexProperty<V> setVertexProperty(final MongodbVertex vertex, final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues);

    /**
     * 是否支持多属性
     * @return boolean
     */
    boolean supportsMultiProperties();

    /**
     * 是否支持多属性
     * @return boolean
     */
    boolean supportsMetaProperties();

    /**
     * 获取属性值类型
     * @param key 属性key
     * @return 值类型
     */
    VertexProperty.Cardinality getCardinality(final String key);

    /**
     * 移除属性
     * @param vertexProperty 属性
     */
    void removeVertexProperty(final MongodbVertexProperty vertexProperty);

    /**
     * 设置属性
     * @param vertexProperty 属性
     * @param key 属性key
     * @param value 属性值
     * @param <V> 属性类型
     * @return 属性
     */
    <V> Property<V> setProperty(final MongodbVertexProperty vertexProperty, final String key, final V value);

    /**
     * 获取属性
     * @param vertexProperty 属性
     * @param key 属性key
     * @param <V> 属性类型
     * @return 属性
     */
    <V> Property<V> getProperty(final MongodbVertexProperty vertexProperty, final String key);

    /**
     * 获取属性
     * @param vertexProperty 属性
     * @param keys 属性key列表
     * @param <V> 属性类型
     * @return 属性列表
     */
    <V> Iterator<Property<V>> getProperties(final MongodbVertexProperty vertexProperty, final String... keys);

    /**
     * 查找点
     * @param graph 图
     * @param hasContainers 查询方案
     * @param ids ID列表
     * @return 点列表
     */
    Iterator<Vertex> lookupVertices(final MongodbGraph graph, final List<HasContainer> hasContainers, final Object... ids);
}
