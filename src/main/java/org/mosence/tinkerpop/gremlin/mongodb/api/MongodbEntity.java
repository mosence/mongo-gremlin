package org.mosence.tinkerpop.gremlin.mongodb.api;

/**
 * @author MoSence
 */
public interface MongodbEntity {
    /**
     * 获取ID
     * @return id
     */
    String getId();

    /**
     * 获取keys
     * @return keys
     */
    Iterable<String> getKeys();

    /**
     * 获取属性值
     * @param name 属性key
     * @return 属性值
     */
    Object getProperty(String name);

    /**
     * 获取属性值，为空则赋予默认值
     * @param name 属性key
     * @param defaultValue 默认属性值
     * @return 属性值
     */
    Object getProperty(String name, Object defaultValue);

    /**
     * 设置属性值
     * @param name 属性key
     * @param value 属性值
     */
    void setProperty(String name, Object value);

    /**
     * 移除属性
     * @param name 属性key
     * @return 被移除的属性值
     */
    Object removeProperty(String name);

    /**
     * 是否拥有属性
     * @param name 属性key
     * @return boolean
     */
    boolean hasProperty(String name);

    /**
     * 删除
     */
    void delete();

    /**
     * 是否删除
     * @return 是否删除
     */
    boolean isDelete();

    /**
     * json
     * @return json
     */
    String toJson();
}
