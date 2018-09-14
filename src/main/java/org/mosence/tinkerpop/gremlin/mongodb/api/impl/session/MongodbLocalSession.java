package org.mosence.tinkerpop.gremlin.mongodb.api.impl.session;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.mosence.tinkerpop.gremlin.mongodb.api.impl.BaseMongodbEntityImpl;

import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * @author MoSence
 */
public class MongodbLocalSession {

    public MongodbLocalSession(){
        origin.clear();
    }

    private static CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
            .withCache("session",
                    CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, BaseMongodbEntityImpl.class,
                            ResourcePoolsBuilder.heap(1000000)).build())
            .build(true);

    private static Cache<String,BaseMongodbEntityImpl> origin = cacheManager.getCache("session",String.class, BaseMongodbEntityImpl.class);

    public static MongodbLocalSession none() {
        return new NoneMongodbLocalSession();
    }

    public void clear(){
        origin.clear();
    }

    public void add(BaseMongodbEntityImpl... elements){
        Stream.of(elements).forEach(this::addOne);
    }

    public void forEach(Consumer<Cache.Entry<String,BaseMongodbEntityImpl>> consumer){
        origin.forEach(consumer);
    }

    private void addOne(BaseMongodbEntityImpl one) {
        try {
            if(!origin.containsKey(one.getId())){
                origin.put(one.getId(),one.clone());
            }
        } catch (CloneNotSupportedException ignore) {
        }
    }

    public static class NoneMongodbLocalSession extends MongodbLocalSession{
        @Override
        public void clear(){
        }

        @Override
        public void add(BaseMongodbEntityImpl... elements){
        }

        @SuppressWarnings("unchecked")
        @Override
        public void forEach(Consumer<Cache.Entry<String,BaseMongodbEntityImpl>> consumer){
        }
    }

}
