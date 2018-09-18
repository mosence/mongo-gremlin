package org.mosence.tinkerpop.gremlin.mongodb.process.computer;

import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MongodbGraphReduceEmitter<OK, OV> implements MapReduce.ReduceEmitter<OK, OV> {

    protected Queue<KeyValue<OK, OV>> reduceQueue = new ConcurrentLinkedQueue<>();

    @Override
    public void emit(final OK key, final OV value) {
        this.reduceQueue.add(new KeyValue<>(key, value));
    }

    protected void complete(final MapReduce<?, ?, OK, OV, ?> mapReduce) {
        if (mapReduce.getReduceKeySort().isPresent()) {
            final Comparator<OK> comparator = mapReduce.getReduceKeySort().get();
            final List<KeyValue<OK, OV>> list = new ArrayList<>(this.reduceQueue);
            Collections.sort(list, Comparator.comparing(KeyValue::getKey, comparator));
            this.reduceQueue.clear();
            this.reduceQueue.addAll(list);
        }
    }
}
