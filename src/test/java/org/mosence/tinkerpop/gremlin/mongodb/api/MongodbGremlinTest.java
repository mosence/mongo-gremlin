package org.mosence.tinkerpop.gremlin.mongodb.api;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mosence.tinkerpop.gremlin.mongodb.api.impl.exception.NotFoundException;

import java.util.HashMap;
import java.util.Map;

public class MongodbGremlinTest {

    private static final String node = "node";
    private static final String edge = "edge";
    private static final String database = "gremlin";
    private static final String uri = "mongodb://localhost:27017/gremlin";
    private static MongodbGraphAPI db;

    @BeforeClass
    public static void setUp() {
        Map<String, String> config = new HashMap<>();
        config.put("gremlin.mongo.node.collection", node);
        config.put("gremlin.mongo.edge.collection", edge);
        config.put("gremlin.mongo.graph.database", database);
        db = MongodbFactory.Builder.open(uri, config);
    }

    @AfterClass
    public static void tearDown() {
        db.findNodes("A").forEach(MongodbEntity::delete);
        db.findNodes("B").forEach(MongodbEntity::delete);
        db.allRelationships().forEach(edge -> {
            if ("殴打".equals(edge.type())) {
                edge.delete();
            }
        });
        System.gc();
    }

    @Test
    public void testFindGraph() {
        System.out.println("=====FIND 父亲=====");
        db.findNodes("父亲").forEach(System.out::println);
        System.out.println("=====ALL NODES=====");
        db.allNodes().forEach(System.out::println);
        System.out.println("=====ALL EDGES=====");
        db.allRelationships().forEach(System.out::println);
    }

    @Test
    public void testFindWithAttribute() {
        System.out.println("=====FIND A：椿须枸=====");
        db.findNodes("A", "name", "椿须枸").forEach(System.out::println);
    }

    @Test
    public void testFindById() {
        String ida;
        String idb;
        String idr;
        MongodbNode a = db.createNode("A", "父亲");
        MongodbNode b = db.createNode("B", "撒比");
        ida = a.getId();
        idb = b.getId();
        idr = a.connectTo(b, "殴打").getId();
        System.out.println(db.getRelationshipById(idr));
        System.out.println(db.getNodeById(ida));
        System.out.println(db.getNodeById(idb));
    }

    @Test(expected = NotFoundException.class)
    public void testDelete() {
        String ida;
        String idb;
        String idr;
        MongodbNode a = db.createNode("A", "父亲");
        MongodbNode b = db.createNode("B", "撒比");
        ida = a.getId();
        idb = b.getId();
        idr = a.connectTo(b, "殴打").getId();
        a.delete();
        Assert.assertNotNull(db.getRelationshipById(idr));
        Assert.assertNotNull(db.getNodeById(idb));
        Assert.assertNull(db.getNodeById(ida));
    }

    @Test
    public void testCreateNode() {
        MongodbNode a = db.createNode("A", "父亲");
        MongodbNode w = db.createNode("A", "父亲");
        a.setProperty("name", "椿须枸");
        MongodbNode b = db.createNode("B", "撒比");
        b.setProperty("name", "碧特哔");
        a.connectTo(b, "殴打");
        db.findNodes("A").forEach(System.out::println);
        db.findNodes("B").forEach(System.out::println);
        db.allRelationships().forEach(edge -> {
            if ("殴打".equals(edge.type())) {
                System.out.println(edge);
            }
        });
    }

}