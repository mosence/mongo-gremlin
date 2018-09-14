package org.mosence.tinkerpop.gremlin.mongodb.api;

/**
 * mongodb 图事务
 * @author MoSence
 */
public interface MongodbTx extends AutoCloseable{
    /**
     * 事务失败
     */
    void failure();

    /**
     * 事务成功
     */
    void success();

    /**
     * 事务抛异常
     */
    void error(Throwable throwable);

    /**
     * 关闭事务
     */
    @Override
    void close();

    boolean isClose();
}
