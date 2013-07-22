package com.graphaware.neo4j.relcount.simple;

import com.graphaware.neo4j.framework.GraphAwareFramework;
import com.graphaware.neo4j.framework.config.DefaultFrameworkConfiguration;
import com.graphaware.neo4j.framework.config.FrameworkConfiguration;
import com.graphaware.neo4j.relcount.common.IntegrationTest;
import com.graphaware.neo4j.relcount.simple.api.SimpleCachedRelationshipCounter;
import com.graphaware.neo4j.relcount.simple.api.SimpleNaiveRelationshipCounter;
import com.graphaware.neo4j.relcount.simple.module.SimpleRelationshipCountModule;
import org.junit.Test;
import org.neo4j.graphdb.Node;

import static com.graphaware.neo4j.relcount.common.IntegrationTest.RelationshipTypes.ONE;
import static com.graphaware.neo4j.relcount.common.IntegrationTest.RelationshipTypes.TWO;
import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Direction.*;

/**
 *
 */
public class SimpleRelationshipCountIntegrationTest extends IntegrationTest {

    @Test
    public void noFramework() {
        setUpTwoNodes();
        simulateUsage();

        verifyCountsUsingNaiveCounter(1);
        verifyCountsUsingCachingCounter(0);
    }

    @Test
    public void noFramework2() {
        setUpTwoNodes();
        simulateUsage();
        simulateUsage();

        verifyCountsUsingNaiveCounter(2);
        verifyCountsUsingCachingCounter(0);
    }

    @Test
    public void defaultFrameworkOnNewDatabase() {
        GraphAwareFramework framework = new GraphAwareFramework(database);
        framework.registerModule(new SimpleRelationshipCountModule());
        framework.start();

        setUpTwoNodes();
        simulateUsage();

        verifyCountsUsingNaiveCounter(1);
        verifyCountsUsingCachingCounter(1);
    }

    @Test
    public void defaultFrameworkOnExistingDatabase() {
        setUpTwoNodes();
        simulateUsage();

        GraphAwareFramework framework = new GraphAwareFramework(database);
        framework.registerModule(new SimpleRelationshipCountModule());
        framework.start();

        verifyCountsUsingNaiveCounter(1);
        verifyCountsUsingCachingCounter(1);
    }

    @Test
    public void customFrameworkOnNewDatabase() {
        GraphAwareFramework framework = new GraphAwareFramework(database, new CustomConfig());

        framework.registerModule(new SimpleRelationshipCountModule());
        framework.start();

        setUpTwoNodes();
        simulateUsage();

        verifyCountsUsingNaiveCounter(1);
        verifyCountsUsingCachingCounter(1, new CustomConfig());
    }

    @Test
    public void customFrameworkOnExistingDatabase() {
        setUpTwoNodes();
        simulateUsage();

        GraphAwareFramework framework = new GraphAwareFramework(database, new CustomConfig());

        framework.registerModule(new SimpleRelationshipCountModule());
        framework.start();

        verifyCountsUsingNaiveCounter(1);
        verifyCountsUsingCachingCounter(1, new CustomConfig());
    }

    private void verifyCountsUsingNaiveCounter(int factor) {
        Node one = database.getNodeById(1);
        Node two = database.getNodeById(2);

        assertEquals(3 * factor, new SimpleNaiveRelationshipCounter(ONE, INCOMING).count(one));
        assertEquals(7 * factor, new SimpleNaiveRelationshipCounter(ONE, OUTGOING).count(one));
        assertEquals(10 * factor, new SimpleNaiveRelationshipCounter(ONE, BOTH).count(one));
        assertEquals(0 * factor, new SimpleNaiveRelationshipCounter(TWO, INCOMING).count(one));
        assertEquals(1 * factor, new SimpleNaiveRelationshipCounter(TWO, OUTGOING).count(one));
        assertEquals(1 * factor, new SimpleNaiveRelationshipCounter(TWO, BOTH).count(one));

        assertEquals(6 * factor, new SimpleNaiveRelationshipCounter(ONE, INCOMING).count(two));
        assertEquals(2 * factor, new SimpleNaiveRelationshipCounter(ONE, OUTGOING).count(two));
        assertEquals(8 * factor, new SimpleNaiveRelationshipCounter(ONE, BOTH).count(two));
        assertEquals(1 * factor, new SimpleNaiveRelationshipCounter(TWO, INCOMING).count(two));
        assertEquals(0 * factor, new SimpleNaiveRelationshipCounter(TWO, OUTGOING).count(two));
        assertEquals(1 * factor, new SimpleNaiveRelationshipCounter(TWO, BOTH).count(two));
    }

    private void verifyCountsUsingCachingCounter(int factor) {
        verifyCountsUsingCachingCounter(factor, DefaultFrameworkConfiguration.getInstance());
    }

    private void verifyCountsUsingCachingCounter(int factor, FrameworkConfiguration config) {
        Node one = database.getNodeById(1);
        Node two = database.getNodeById(2);

        assertEquals(3 * factor, new SimpleCachedRelationshipCounter(ONE, INCOMING, config).count(one));
        assertEquals(7 * factor, new SimpleCachedRelationshipCounter(ONE, OUTGOING, config).count(one));
        assertEquals(10 * factor, new SimpleCachedRelationshipCounter(ONE, BOTH, config).count(one));
        assertEquals(0 * factor, new SimpleCachedRelationshipCounter(TWO, INCOMING, config).count(one));
        assertEquals(1 * factor, new SimpleCachedRelationshipCounter(TWO, OUTGOING, config).count(one));
        assertEquals(1 * factor, new SimpleCachedRelationshipCounter(TWO, BOTH, config).count(one));

        assertEquals(6 * factor, new SimpleCachedRelationshipCounter(ONE, INCOMING, config).count(two));
        assertEquals(2 * factor, new SimpleCachedRelationshipCounter(ONE, OUTGOING, config).count(two));
        assertEquals(8 * factor, new SimpleCachedRelationshipCounter(ONE, BOTH, config).count(two));
        assertEquals(1 * factor, new SimpleCachedRelationshipCounter(TWO, INCOMING, config).count(two));
        assertEquals(0 * factor, new SimpleCachedRelationshipCounter(TWO, OUTGOING, config).count(two));
        assertEquals(1 * factor, new SimpleCachedRelationshipCounter(TWO, BOTH, config).count(two));
    }
}
