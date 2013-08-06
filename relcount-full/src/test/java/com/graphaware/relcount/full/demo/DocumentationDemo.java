package com.graphaware.relcount.full.demo;

import com.graphaware.framework.GraphAwareFramework;
import com.graphaware.relcount.common.counter.RelationshipCounter;
import com.graphaware.relcount.common.demo.BaseDocumentationDemo;
import com.graphaware.relcount.common.module.RelationshipCountModule;
import com.graphaware.relcount.full.counter.FullCachedRelationshipCounter;
import com.graphaware.relcount.full.module.FullRelationshipCountModule;
import org.junit.Test;
import org.neo4j.graphdb.Node;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

/**
 *  Demonstration of different full relationship counting options.
 */
public class DocumentationDemo extends BaseDocumentationDemo {

    @Test
    public void demonstrateFullCachedRelationshipCounter() {
        GraphAwareFramework framework = new GraphAwareFramework(database);
        RelationshipCountModule module = new FullRelationshipCountModule();
        framework.registerModule(module);
        framework.start();

        populateDatabase();

        Node tracy = database.getNodeById(2);

        RelationshipCounter followers = new FullCachedRelationshipCounter(Rels.FOLLOWS, INCOMING);
        assertEquals(9, followers.count(tracy));

        RelationshipCounter followersStrength2 = new FullCachedRelationshipCounter(Rels.FOLLOWS, INCOMING).with(STRENGTH, 2);
        assertEquals(3, followersStrength2.count(tracy));

        RelationshipCounter following = new FullCachedRelationshipCounter(Rels.FOLLOWS, OUTGOING);
        assertEquals(9, following.count(tracy));

        RelationshipCounter followingStrength1 = new FullCachedRelationshipCounter(Rels.FOLLOWS, OUTGOING).with(STRENGTH, 1);
        assertEquals(4, followingStrength1.count(tracy));
    }
}
