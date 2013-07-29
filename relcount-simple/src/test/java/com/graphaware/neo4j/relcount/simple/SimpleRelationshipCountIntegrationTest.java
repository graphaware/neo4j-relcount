package com.graphaware.neo4j.relcount.simple;

import com.graphaware.neo4j.dto.common.relationship.SerializableTypeAndDirectionImpl;
import com.graphaware.neo4j.framework.GraphAwareFramework;
import com.graphaware.neo4j.framework.config.FrameworkConfiguration;
import com.graphaware.neo4j.relcount.common.IntegrationTest;
import com.graphaware.neo4j.relcount.simple.counter.SimpleCachedRelationshipCounter;
import com.graphaware.neo4j.relcount.simple.counter.SimpleNaiveRelationshipCounter;
import com.graphaware.neo4j.relcount.simple.module.SimpleRelationshipCountModule;
import com.graphaware.neo4j.tx.single.SimpleTransactionExecutor;
import com.graphaware.neo4j.tx.single.VoidReturningCallback;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import static com.graphaware.neo4j.framework.GraphAwareFramework.CORE;
import static com.graphaware.neo4j.framework.config.DefaultFrameworkConfiguration.DEFAULT_SEPARATOR;
import static com.graphaware.neo4j.framework.config.DefaultFrameworkConfiguration.getInstance;
import static com.graphaware.neo4j.relcount.common.IntegrationTest.RelationshipTypes.ONE;
import static com.graphaware.neo4j.relcount.common.IntegrationTest.RelationshipTypes.TWO;
import static com.graphaware.neo4j.relcount.simple.module.SimpleRelationshipCountModule.SIMPLE_RELCOUNT_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.Direction.*;

/**
 * Integration test for simple relationship counting.
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

    @Test
    public void countsOutOfSync() {
        GraphAwareFramework framework = new GraphAwareFramework(database);

        framework.registerModule(new SimpleRelationshipCountModule());
        framework.start();

        setUpTwoNodes();
        simulateUsage();

        new SimpleTransactionExecutor(database).executeInTransaction(new VoidReturningCallback() {
            @Override
            protected void doInTx(GraphDatabaseService database) {
                Node one = database.getNodeById(1);
                String key = new SerializableTypeAndDirectionImpl(ONE, INCOMING).toString(getInstance().createPrefix(SIMPLE_RELCOUNT_ID), DEFAULT_SEPARATOR);
                one.setProperty(key, (int) one.getProperty(key) - 7);
            }
        });

        simulateUsage();

        assertTrue(database.getNodeById(0).getProperty(getInstance().createPrefix(CORE) + SIMPLE_RELCOUNT_ID).toString().startsWith("FORCE_INIT:"));
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
        verifyCountsUsingCachingCounter(factor, getInstance());
    }

    private void verifyCountsUsingCachingCounter(int factor, FrameworkConfiguration config) {
        Node one = database.getNodeById(1);
        Node two = database.getNodeById(2);

        //just verifying the constructor with default config
        if (config.equals(getInstance())) {
            assertEquals(3 * factor, new SimpleCachedRelationshipCounter(ONE, INCOMING).count(one));
            assertEquals(7 * factor, new SimpleCachedRelationshipCounter(ONE, OUTGOING).count(one));
            assertEquals(10 * factor, new SimpleCachedRelationshipCounter(ONE, BOTH).count(one));
        }

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
