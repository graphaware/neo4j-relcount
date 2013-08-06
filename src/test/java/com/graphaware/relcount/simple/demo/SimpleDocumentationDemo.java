package com.graphaware.relcount.simple.demo;

import com.graphaware.framework.GraphAwareFramework;
import com.graphaware.relcount.common.counter.RelationshipCounter;
import com.graphaware.relcount.common.demo.BaseDocumentationDemo;
import com.graphaware.relcount.simple.counter.SimpleCachedRelationshipCounter;
import com.graphaware.relcount.simple.counter.SimpleNaiveRelationshipCounter;
import com.graphaware.relcount.simple.module.SimpleRelationshipCountModule;
import com.graphaware.tx.event.improved.strategy.RelationshipInclusionStrategy;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import static com.graphaware.relcount.common.demo.BaseDocumentationDemo.Rels.FOLLOWS;
import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

/**
 * Demonstration of different full relationship counting options.
 */
public class SimpleDocumentationDemo extends BaseDocumentationDemo {

    @Test
    public void demonstrateSimpleCachedRelationshipCounter() {
        GraphAwareFramework framework = new GraphAwareFramework(database);
        framework.registerModule(new SimpleRelationshipCountModule());
        framework.start();

        populateDatabase();

        Node tracy = database.getNodeById(2);

        RelationshipCounter followers = new SimpleCachedRelationshipCounter(FOLLOWS, INCOMING);
        assertEquals(9, followers.count(tracy));

        RelationshipCounter following = new SimpleCachedRelationshipCounter(FOLLOWS, OUTGOING);
        assertEquals(9, following.count(tracy));
    }

    @Test
    public void demonstrateSimpleCachedRelationshipCounterWithCustomRelationshipInclusionStrategy() {
        GraphAwareFramework framework = new GraphAwareFramework(database);

        RelationshipInclusionStrategy customRelationshipInclusionStrategy = new RelationshipInclusionStrategy() {
            @Override
            public boolean include(Relationship relationship) {
                return relationship.isType(FOLLOWS);
            }
        };

        framework.registerModule(new SimpleRelationshipCountModule(customRelationshipInclusionStrategy));
        framework.start();

        populateDatabase();

        Node tracy = database.getNodeById(2);

        RelationshipCounter followers = new SimpleCachedRelationshipCounter(FOLLOWS, INCOMING);
        assertEquals(9, followers.count(tracy));

        RelationshipCounter following = new SimpleCachedRelationshipCounter(FOLLOWS, OUTGOING);
        assertEquals(9, following.count(tracy));
    }

    @Test
    public void demonstrateSimpleNaiveRelationshipCounter() {
        populateDatabase();

        Node tracy = database.getNodeById(2);

        RelationshipCounter followers = new SimpleNaiveRelationshipCounter(FOLLOWS, INCOMING);
        assertEquals(9, followers.count(tracy));

        RelationshipCounter following = new SimpleNaiveRelationshipCounter(FOLLOWS, OUTGOING);
        assertEquals(9, following.count(tracy));
    }
}
