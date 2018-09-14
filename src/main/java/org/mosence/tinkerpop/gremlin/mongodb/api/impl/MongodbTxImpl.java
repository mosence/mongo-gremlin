package org.mosence.tinkerpop.gremlin.mongodb.api.impl;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import org.mosence.tinkerpop.gremlin.mongodb.api.MongodbTx;
import org.mosence.tinkerpop.gremlin.mongodb.api.impl.session.MongodbLocalSession;

import java.util.Objects;

/**
 * mongodb 图事务实现
 * @author MoSence
 */
public class MongodbTxImpl implements MongodbTx {

    private ClientSession clientSession = null;

    private boolean support = true;

    private boolean close = false;

    private Throwable throwable;

    private static ThreadLocal<MongodbLocalSession> mongodbLocalSession = ThreadLocal.withInitial(()->null);

    public MongodbTxImpl(MongoClient mongoClient){
        try{
            this.clientSession = mongoClient.startSession();
        }catch (Exception ignore){}
        checkSupport();
    }

    private void checkSupport() {
        if(Objects.isNull(this.clientSession)){
            support = false;
            mongodbLocalSession.set(new MongodbLocalSession());
        }
    }

    public static void ram(BaseMongodbEntityImpl entity){
        getLocalSession().add(entity);
    }

    private static MongodbLocalSession getLocalSession(){
        if(Objects.nonNull(mongodbLocalSession.get())){
            return mongodbLocalSession.get();
        }
        return MongodbLocalSession.none();
    }

    @Override
    public void error(Throwable throwable){
        this.throwable = throwable;
    }

    @Override
    public void failure() {
        if(support){
            clientSession.abortTransaction();
        }else{
            getLocalSession().forEach(entry->{
                BaseMongodbEntityImpl element = entry.getValue();
                boolean create = element.isCreate();
                element.resetStatus();
                if(element.originExists()){
                    if(create){
                        element.delete();
                    }else{
                        element.needUpdate();
                    }
                }else{
                    element.needCreate();
                }
                element.persist();
            });
            getLocalSession().clear();
        }
        if(Objects.nonNull(throwable)){
            throw new RuntimeException(throwable);
        }
    }

    @Override
    public void success() {
        if(support){
            clientSession.commitTransaction();
        }else{
            getLocalSession().clear();
        }
        if(Objects.nonNull(throwable)){
            throw new RuntimeException(throwable);
        }
    }

    @Override
    public void close() {
        if(support){
            clientSession.close();
        }else{
            getLocalSession().clear();
            mongodbLocalSession.remove();
        }
        close = true;
    }

    @Override
    public boolean isClose() {
        return close;
    }
}
