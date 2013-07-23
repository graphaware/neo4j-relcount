/*
 * Copyright (c) 2013 GraphAware
 *
 * This file is part of GraphAware.
 *
 * GraphAware is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

package com.graphaware.neo4j.relcount.full.logic;

import com.graphaware.neo4j.framework.GraphAwareFramework;
import com.graphaware.neo4j.framework.config.DefaultFrameworkConfiguration;
import com.graphaware.neo4j.relcount.common.counter.UnableToCountException;
import com.graphaware.neo4j.relcount.full.dto.relationship.CompactibleRelationshipImpl;
import com.graphaware.neo4j.relcount.full.dto.relationship.LiteralRelationshipDescription;
import com.graphaware.neo4j.relcount.full.dto.relationship.RelationshipDescription;
import com.graphaware.neo4j.relcount.full.dto.relationship.WildcardRelationshipDescription;
import com.graphaware.neo4j.relcount.full.module.FullRelationshipCountModule;
import com.graphaware.neo4j.relcount.full.strategy.RelationshipCountStrategiesImpl;
import com.graphaware.neo4j.tx.single.SimpleTransactionExecutor;
import com.graphaware.neo4j.tx.single.TransactionCallback;
import com.graphaware.neo4j.tx.single.TransactionExecutor;
import com.graphaware.neo4j.tx.single.VoidReturningCallback;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.test.TestGraphDatabaseFactory;

import static com.graphaware.neo4j.relcount.full.dto.property.CompactiblePropertiesImpl.ANY_VALUE;
import static com.graphaware.neo4j.relcount.full.module.FullRelationshipCountModule.FULL_RELCOUNT_DEFAULT_ID;
import static junit.framework.Assert.*;

/**
 * Compaction-related tests for {@link com.graphaware.neo4j.relcount.full.logic.FullRelationshipCountCache}.
 */
public class CompactionIntegrationTest {

    private FullRelationshipCountCache cache;
    private RelationshipCountCompactor compactor;
    private FullCachedRelationshipCountReader reader;
    private GraphDatabaseService database;
    private TransactionExecutor executor;

    @Before
    public void setUp() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();
        executor = new SimpleTransactionExecutor(database);

        RelationshipCountStrategiesImpl strategies = RelationshipCountStrategiesImpl.defaultStrategies().with(5);

        FullRelationshipCountModule module = new FullRelationshipCountModule(strategies);
        cache = new FullRelationshipCountCache(FULL_RELCOUNT_DEFAULT_ID, strategies);
        cache.configurationChanged(DefaultFrameworkConfiguration.getInstance());
        compactor = new ThresholdBasedRelationshipCountCompactor(5, cache);

        GraphAwareFramework framework = new GraphAwareFramework(database);
        framework.registerModule(module);
        framework.start();

        executor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                database.createNode();
                return null;
            }
        });

        reader = new FullCachedRelationshipCountReader(FULL_RELCOUNT_DEFAULT_ID, DefaultFrameworkConfiguration.getInstance());
    }

    @After
    public void tearDown() {
        database.shutdown();
    }

    @Test
    public void nothingShouldBeCompactedBeforeThresholdIsReached() {
        executor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                Node root = database.getNodeById(0);
                root.setProperty(wildcard("test#OUTGOING#k1#v1").toString(prefix(), hash()), 14);
                root.setProperty(wildcard("test#OUTGOING#k1#v2").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("test#OUTGOING#k1#v3").toString(prefix(), hash()), 2);
                root.setProperty(wildcard("test#OUTGOING#k1#v4").toString(prefix(), hash()), 3);

                compactor.compactRelationshipCounts(root);
            }
        });

        assertEquals(4, cache.getRelationshipCounts(database.getNodeById(0)).size());
        assertEquals(14, reader.getRelationshipCount(wildcard("test#OUTGOING#k1#v1"), database.getNodeById(0)));
        assertEquals(1, reader.getRelationshipCount(wildcard("test#OUTGOING#k1#v2"), database.getNodeById(0)));
        assertEquals(2, reader.getRelationshipCount(wildcard("test#OUTGOING#k1#v3"), database.getNodeById(0)));
        assertEquals(3, reader.getRelationshipCount(wildcard("test#OUTGOING#k1#v4"), database.getNodeById(0)));
        assertEquals(20, reader.getRelationshipCount(wildcard("test#OUTGOING#"), database.getNodeById(0)));
    }

    @Test
    public void countShouldBeCompactedWhenThresholdIsReached() {
        executor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                Node root = database.getNodeById(0);
                root.setProperty(wildcard("test#OUTGOING#k1#v1").toString(prefix(), hash()), 14);
                root.setProperty(wildcard("test#OUTGOING#k1#v2").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("test#OUTGOING#k1#v3").toString(prefix(), hash()), 2);
                root.setProperty(wildcard("test#OUTGOING#k1#v4").toString(prefix(), hash()), 3);
                root.setProperty(wildcard("test#OUTGOING#k1#v5").toString(prefix(), hash()), 4);

                compactor.compactRelationshipCounts(root);
            }
        });

        assertEquals(1, cache.getRelationshipCounts(database.getNodeById(0)).size());
        assertTrue(cache.getRelationshipCounts(database.getNodeById(0)).containsKey(new CompactibleRelationshipImpl("test#OUTGOING#k1#" + ANY_VALUE, null, hash())));
        assertEquals(24, reader.getRelationshipCount(wildcard("test#OUTGOING#"), database.getNodeById(0)));

        try {
            reader.getRelationshipCount(wildcard("test#OUTGOING#k1#v1"), database.getNodeById(0));
            fail();
        } catch (UnableToCountException e) {
            //OK
        }
    }

    @Test
    public void countShouldBeCompactedWhenThresholdIsReachedUsingAsync() throws InterruptedException {
        final AsyncThresholdBasedRelationshipCountCompactor relationshipCountCompactor = new AsyncThresholdBasedRelationshipCountCompactor(5, cache);

        executor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                Node root = database.getNodeById(0);
                root.setProperty(wildcard("test#OUTGOING#k1#v1").toString(prefix(), hash()), 14);
                root.setProperty(wildcard("test#OUTGOING#k1#v2").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("test#OUTGOING#k1#v3").toString(prefix(), hash()), 2);
                root.setProperty(wildcard("test#OUTGOING#k1#v4").toString(prefix(), hash()), 3);
                root.setProperty(wildcard("test#OUTGOING#k1#v5").toString(prefix(), hash()), 4);

                relationshipCountCompactor.compactRelationshipCounts(root);
            }
        });

        relationshipCountCompactor.shutdown();

        assertEquals(1, cache.getRelationshipCounts(database.getNodeById(0)).size());
        assertTrue(cache.getRelationshipCounts(database.getNodeById(0)).containsKey(new CompactibleRelationshipImpl("test#OUTGOING#k1#" + ANY_VALUE, null, hash())));
        assertEquals(24, reader.getRelationshipCount(wildcard("test#OUTGOING#"), database.getNodeById(0)));

        try {
            reader.getRelationshipCount(wildcard("test#OUTGOING#k1#v1"), database.getNodeById(0));
            fail();
        } catch (UnableToCountException e) {
            //OK
        }
    }

    @Test
    public void verifyMultipleCompactions() {
        executor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                Node root = database.getNodeById(0);
                root.setProperty(wildcard("test#OUTGOING#k1#v1#k2#v1").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("test#OUTGOING#k1#v1#k2#v2").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("test#OUTGOING#k1#v1#k2#v3").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("test#OUTGOING#k1#v1#k2#v4").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("test#OUTGOING#k1#v2#k2#v1").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("test#OUTGOING#k1#v2#k2#v2").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("test#OUTGOING#k1#v2#k2#v3").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("test#OUTGOING#k1#v2#k2#v4").toString(prefix(), hash()), 1);

                compactor.compactRelationshipCounts(root);
            }
        });

        assertEquals(2, cache.getRelationshipCounts(database.getNodeById(0)).size());
        assertEquals(4, reader.getRelationshipCount(wildcard("test#OUTGOING#k1#v1"), database.getNodeById(0)));
        assertEquals(4, reader.getRelationshipCount(wildcard("test#OUTGOING#k1#v2"), database.getNodeById(0)));
        assertEquals(8, reader.getRelationshipCount(wildcard("test#OUTGOING#"), database.getNodeById(0)));

        try {
            reader.getRelationshipCount(wildcard("test#OUTGOING#k2#v3"), database.getNodeById(0));
            fail();
        } catch (UnableToCountException e) {
            //ok
        }
    }

    @Test
    public void verifyMultiLevelCompaction() {
        executor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                Node root = database.getNodeById(0);
                root.setProperty(wildcard("test#OUTGOING#z1#v1#k2#v1#k3#v1").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("test#OUTGOING#z1#v1#k2#v2#k3#v2").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("test#OUTGOING#z1#v1#k2#v3#k3#v3").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("test#OUTGOING#z1#v1#k2#v4#k3#v4").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("test#OUTGOING#z1#v1#k2#v5#k3#v5").toString(prefix(), hash()), 1);

                compactor.compactRelationshipCounts(root);
            }
        });

        assertEquals(1, cache.getRelationshipCounts(database.getNodeById(0)).size());
        assertEquals(5, reader.getRelationshipCount(wildcard("test#OUTGOING#z1#v1"), database.getNodeById(0)));
        assertEquals(5, reader.getRelationshipCount(wildcard("test#OUTGOING#"), database.getNodeById(0)));
        assertEquals(0, reader.getRelationshipCount(literal("test#OUTGOING#"), database.getNodeById(0)));

        try {
            reader.getRelationshipCount(wildcard("test#OUTGOING#k2#whatever"), database.getNodeById(0));
            fail();
        } catch (UnableToCountException e) {
            //ok
        }

        try {
            reader.getRelationshipCount(wildcard("test#OUTGOING#k3#v4"), database.getNodeById(0));
            fail();
        } catch (UnableToCountException e) {
            //ok
        }

        try {
            reader.getRelationshipCount(literal("test#OUTGOING#z1#v1"), database.getNodeById(0));
            fail();
        } catch (UnableToCountException e) {
            //ok
        }
    }

    @Test
    public void verifyImpossibleCompaction() {
        executor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                Node root = database.getNodeById(0);
                root.setProperty(wildcard("test#OUTGOING#k1#v1#k2#v1#k3#v1").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("test#OUTGOING#k1#v1#k2#v2#k3#v2").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("test#OUTGOING#k1#v1#k2#v3#k3#v3").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("test#OUTGOING#k1#v1#k2#v4#k3#v4").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("test#OUTGOING#k1#v1#k2#v5#k3#v5").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("test#OUTGOING#k2#v2").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("test2#OUTGOING#k2#v2").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("test3#OUTGOING#k2#v2").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("test4#OUTGOING#k2#v2").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("test5#OUTGOING#k2#v2").toString(prefix(), hash()), 1);

                compactor.compactRelationshipCounts(root);
            }
        });

        assertEquals(5, cache.getRelationshipCounts(database.getNodeById(0)).size());
        assertEquals(6, reader.getRelationshipCount(wildcard("test#OUTGOING#"), database.getNodeById(0)));
        assertEquals(1, reader.getRelationshipCount(wildcard("test2#OUTGOING#"), database.getNodeById(0)));
        assertEquals(1, reader.getRelationshipCount(wildcard("test3#OUTGOING#"), database.getNodeById(0)));
        assertEquals(1, reader.getRelationshipCount(wildcard("test4#OUTGOING#"), database.getNodeById(0)));
        assertEquals(1, reader.getRelationshipCount(wildcard("test5#OUTGOING#"), database.getNodeById(0)));

        try {
            reader.getRelationshipCount(wildcard("test#OUTGOING#k1#v1"), database.getNodeById(0));
            fail();
        } catch (UnableToCountException e) {
            //ok
        }
    }

    @Test
    public void compactionIncludingWildcards() {
        compactor = new ThresholdBasedRelationshipCountCompactor(2, cache);

        executor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                Node root = database.getNodeById(0);
                root.setProperty(wildcard("ONE#INCOMING#k1#v1#k2#v2").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("ONE#INCOMING#k1#"+ANY_VALUE+"#w#"+ANY_VALUE).toString(prefix(), hash()), 2);

                compactor.compactRelationshipCounts(root);
            }
        });

        assertEquals(1, cache.getRelationshipCounts(database.getNodeById(0)).size());
    }

    @Test
    public void compactionIncludingWildcards2() {
        compactor = new ThresholdBasedRelationshipCountCompactor(2, cache);

        executor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                Node root = database.getNodeById(0);
                root.setProperty(wildcard("ONE#INCOMING#k1#"+ANY_VALUE).toString(prefix(), hash()), 1);
                root.setProperty(wildcard("ONE#INCOMING#k2#"+ANY_VALUE).toString(prefix(), hash()), 2);

                compactor.compactRelationshipCounts(root);
            }
        });

        assertEquals(1, cache.getRelationshipCounts(database.getNodeById(0)).size());
    }

    /**
     * just for readability
     */
    private RelationshipDescription wildcard(String s) {
        return new WildcardRelationshipDescription(s, null, hash());
    }

    private RelationshipDescription literal(String s) {
        return new LiteralRelationshipDescription(s, null, hash());
    }

    private String prefix() {
        return DefaultFrameworkConfiguration.getInstance().createPrefix(FULL_RELCOUNT_DEFAULT_ID);
    }

    private String hash() {
        return "#";
    }
}
