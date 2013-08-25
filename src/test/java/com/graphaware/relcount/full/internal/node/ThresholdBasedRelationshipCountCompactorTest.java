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

package com.graphaware.relcount.full.internal.node;

import com.graphaware.framework.config.DefaultFrameworkConfiguration;
import com.graphaware.relcount.common.counter.UnableToCountException;
import com.graphaware.relcount.common.internal.node.RelationshipCountCachingNode;
import com.graphaware.relcount.common.internal.node.RelationshipCountingNode;
import com.graphaware.relcount.full.internal.dto.property.CacheablePropertiesDescriptionImpl;
import com.graphaware.relcount.full.internal.dto.relationship.*;
import com.graphaware.relcount.full.module.FullRelationshipCountModule;
import com.graphaware.tx.executor.single.SimpleTransactionExecutor;
import com.graphaware.tx.executor.single.TransactionExecutor;
import com.graphaware.tx.executor.single.VoidReturningCallback;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.*;


/**
 * Unit test for {@link com.graphaware.relcount.full.internal.node.ThresholdBasedRelationshipCountCompactor}.
 */
public class ThresholdBasedRelationshipCountCompactorTest {

    private GraphDatabaseService database;
    private TransactionExecutor executor;

    @Before
    public void setUp() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();
        executor = new SimpleTransactionExecutor(database);
    }

    @After
    public void tearDown() {
        database.shutdown();
    }

    private RelationshipCountCachingNode<CacheableRelationshipDescription> cachingNode(RelationshipCountCompactor compactor) {
        return new FullRelationshipCountCachingNode(database.getNodeById(0), DefaultFrameworkConfiguration.getInstance().createPrefix(FullRelationshipCountModule.FULL_RELCOUNT_DEFAULT_ID), DefaultFrameworkConfiguration.getInstance().separator(), compactor);
    }

    private RelationshipCountingNode<RelationshipQueryDescription> countingNode() {
        return new FullCachedRelationshipCountingNode(database.getNodeById(0), DefaultFrameworkConfiguration.getInstance().createPrefix(FullRelationshipCountModule.FULL_RELCOUNT_DEFAULT_ID), DefaultFrameworkConfiguration.getInstance().separator());
    }

    @Test
    public void nothingShouldBeCompactedBeforeThresholdIsReached() {
        final ThresholdBasedRelationshipCountCompactor compactor = new ThresholdBasedRelationshipCountCompactor(4);

        executor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                Node root = database.getNodeById(0);
                root.setProperty(wildcard("test#OUTGOING#k1#v1").toString(prefix(), hash()), 14);
                root.setProperty(wildcard("test#OUTGOING#k1#v2").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("test#OUTGOING#k1#v3").toString(prefix(), hash()), 2);
                root.setProperty(wildcard("test#OUTGOING#k1#v4").toString(prefix(), hash()), 3);

                RelationshipCountCachingNode<CacheableRelationshipDescription> node = cachingNode(compactor);
                compactor.compactRelationshipCounts(node);
                node.flush();
            }
        });

        assertEquals(4, cachingNode(compactor).getCachedCounts().size());
        assertEquals(14, countingNode().getRelationshipCount(wildcard("test#OUTGOING#k1#v1")));
        assertEquals(1, countingNode().getRelationshipCount(wildcard("test#OUTGOING#k1#v2")));
        assertEquals(2, countingNode().getRelationshipCount(wildcard("test#OUTGOING#k1#v3")));
        assertEquals(3, countingNode().getRelationshipCount(wildcard("test#OUTGOING#k1#v4")));
        assertEquals(20, countingNode().getRelationshipCount(wildcard("test#OUTGOING#")));
    }

    @Test
    public void countShouldBeCompactedWhenThresholdIsReached() {
        final ThresholdBasedRelationshipCountCompactor compactor = new ThresholdBasedRelationshipCountCompactor(4);

        executor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                Node root = database.getNodeById(0);
                root.setProperty(wildcard("test#OUTGOING#k1#v1").toString(prefix(), hash()), 14);
                root.setProperty(wildcard("test#OUTGOING#k1#v2").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("test#OUTGOING#k1#v3").toString(prefix(), hash()), 2);
                root.setProperty(wildcard("test#OUTGOING#k1#v4").toString(prefix(), hash()), 3);
                root.setProperty(wildcard("test#OUTGOING#k1#v5").toString(prefix(), hash()), 4);

                RelationshipCountCachingNode<CacheableRelationshipDescription> node = cachingNode(compactor);
                compactor.compactRelationshipCounts(node);
                node.flush();
            }
        });

        assertEquals(1, cachingNode(compactor).getCachedCounts().size());
        assertTrue(cachingNode(compactor).getCachedCounts().containsKey(new CacheableRelationshipDescriptionImpl("test#OUTGOING#k1#" + CacheablePropertiesDescriptionImpl.ANY_VALUE, null, hash())));
        assertEquals(24, countingNode().getRelationshipCount(wildcard("test#OUTGOING#")));

        try {
            countingNode().getRelationshipCount(wildcard("test#OUTGOING#k1#v1"));
            fail();
        } catch (UnableToCountException e) {
            //OK
        }
    }

    @Test
    public void verifyMultipleCompactions() {
        final ThresholdBasedRelationshipCountCompactor compactor = new ThresholdBasedRelationshipCountCompactor(4);

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

                RelationshipCountCachingNode<CacheableRelationshipDescription> node = cachingNode(compactor);
                compactor.compactRelationshipCounts(node);
                node.flush();
            }
        });

        assertEquals(2, cachingNode(compactor).getCachedCounts().size());
        assertEquals(4, countingNode().getRelationshipCount(wildcard("test#OUTGOING#k1#v1")));
        assertEquals(4, countingNode().getRelationshipCount(wildcard("test#OUTGOING#k1#v2")));
        assertEquals(8, countingNode().getRelationshipCount(wildcard("test#OUTGOING#")));

        try {
            countingNode().getRelationshipCount(wildcard("test#OUTGOING#k2#v3"));
            fail();
        } catch (UnableToCountException e) {
            //ok
        }
    }

    @Test
    public void verifyMultiLevelCompaction() {
        final ThresholdBasedRelationshipCountCompactor compactor = new ThresholdBasedRelationshipCountCompactor(4);

        executor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                Node root = database.getNodeById(0);
                root.setProperty(wildcard("test#OUTGOING#z1#v1#k2#v1#k3#v1").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("test#OUTGOING#z1#v1#k2#v2#k3#v2").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("test#OUTGOING#z1#v1#k2#v3#k3#v3").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("test#OUTGOING#z1#v1#k2#v4#k3#v4").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("test#OUTGOING#z1#v1#k2#v5#k3#v5").toString(prefix(), hash()), 1);

                RelationshipCountCachingNode<CacheableRelationshipDescription> node = cachingNode(compactor);
                compactor.compactRelationshipCounts(node);
                node.flush();
            }
        });

        assertEquals(1, cachingNode(compactor).getCachedCounts().size());
        assertEquals(5, countingNode().getRelationshipCount(wildcard("test#OUTGOING#z1#v1")));
        assertEquals(5, countingNode().getRelationshipCount(wildcard("test#OUTGOING#")));
        assertEquals(0, countingNode().getRelationshipCount(literal("test#OUTGOING#")));

        try {
            countingNode().getRelationshipCount(wildcard("test#OUTGOING#k2#whatever"));
            fail();
        } catch (UnableToCountException e) {
            //ok
        }

        try {
            countingNode().getRelationshipCount(wildcard("test#OUTGOING#k3#v4"));
            fail();
        } catch (UnableToCountException e) {
            //ok
        }

        try {
            countingNode().getRelationshipCount(literal("test#OUTGOING#z1#v1"));
            fail();
        } catch (UnableToCountException e) {
            //ok
        }
    }

    @Test
    public void verifyImpossibleCompaction() {
        final ThresholdBasedRelationshipCountCompactor compactor = new ThresholdBasedRelationshipCountCompactor(4);

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

                RelationshipCountCachingNode<CacheableRelationshipDescription> node = cachingNode(compactor);
                compactor.compactRelationshipCounts(node);
                node.flush();
            }
        });

        assertEquals(5, cachingNode(compactor).getCachedCounts().size());
        assertEquals(6, countingNode().getRelationshipCount(wildcard("test#OUTGOING#")));
        assertEquals(1, countingNode().getRelationshipCount(wildcard("test2#OUTGOING#")));
        assertEquals(1, countingNode().getRelationshipCount(wildcard("test3#OUTGOING#")));
        assertEquals(1, countingNode().getRelationshipCount(wildcard("test4#OUTGOING#")));
        assertEquals(1, countingNode().getRelationshipCount(wildcard("test5#OUTGOING#")));

        try {
            countingNode().getRelationshipCount(wildcard("test#OUTGOING#k1#v1"));
            fail();
        } catch (UnableToCountException e) {
            //ok
        }
    }

    @Test
    public void compactionIncludingWildcards() {
        final ThresholdBasedRelationshipCountCompactor compactor = new ThresholdBasedRelationshipCountCompactor(1);

        executor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                Node root = database.getNodeById(0);
                root.setProperty(wildcard("ONE#INCOMING#k1#v1#k2#v2").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("ONE#INCOMING#k1#" + CacheablePropertiesDescriptionImpl.ANY_VALUE + "#w#" + CacheablePropertiesDescriptionImpl.ANY_VALUE).toString(prefix(), hash()), 2);

                RelationshipCountCachingNode<CacheableRelationshipDescription> node = cachingNode(compactor);
                compactor.compactRelationshipCounts(node);
                node.flush();
            }
        });

        assertEquals(1, cachingNode(compactor).getCachedCounts().size());
    }

    @Test
    public void compactionIncludingWildcards2() {
        final ThresholdBasedRelationshipCountCompactor compactor = new ThresholdBasedRelationshipCountCompactor(1);

        executor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                Node root = database.getNodeById(0);
                root.setProperty(wildcard("ONE#INCOMING#k1#" + CacheablePropertiesDescriptionImpl.ANY_VALUE).toString(prefix(), hash()), 1);
                root.setProperty(wildcard("ONE#INCOMING#k2#" + CacheablePropertiesDescriptionImpl.ANY_VALUE).toString(prefix(), hash()), 2);

                RelationshipCountCachingNode<CacheableRelationshipDescription> node = cachingNode(compactor);
                compactor.compactRelationshipCounts(node);
                node.flush();
            }
        });

        assertEquals(1, cachingNode(compactor).getCachedCounts().size());
    }

    @Test
    public void anotherCompactionSmokeTest() {
        final ThresholdBasedRelationshipCountCompactor compactor = new ThresholdBasedRelationshipCountCompactor(9);

        executor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                Node root = database.getNodeById(0);
                root.setProperty(wildcard("ONE#INCOMING#level#1#timestamp#123345135123").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("ONE#INCOMING#level#2#timestamp#121432135123").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("ONE#INCOMING#level#0#timestamp#127682135123").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("ONE#INCOMING#level#1#timestamp#123139855123").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("ONE#INCOMING#level#0#timestamp#123133445123").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("ONE#INCOMING#level#0#timestamp#123872575123").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("ONE#INCOMING#level#1#timestamp#123114335123").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("ONE#INCOMING#level#2#timestamp#123132135362").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("ONE#INCOMING#level#2#timestamp#123132135766").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("ONE#INCOMING#level#1#timestamp#123132135763").toString(prefix(), hash()), 1);

                RelationshipCountCachingNode<CacheableRelationshipDescription> node = cachingNode(compactor);
                compactor.compactRelationshipCounts(node);
                node.flush();
            }
        });

        assertEquals(8, cachingNode(compactor).getCachedCounts().size());
    }

    @Test
    public void anotherCompactionSmokeTest2() {
        final ThresholdBasedRelationshipCountCompactor compactor = new ThresholdBasedRelationshipCountCompactor(3);

        executor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                Node root = database.getNodeById(0);
                root.setProperty(wildcard("ONE#INCOMING#level#1#timestamp#123345135123").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("ONE#INCOMING#level#2#timestamp#121432135123").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("ONE#INCOMING#level#0#timestamp#127682135123").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("ONE#INCOMING#level#1#timestamp#123139855123").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("ONE#INCOMING#level#0#timestamp#123133445123").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("ONE#INCOMING#level#0#timestamp#123872575123").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("ONE#INCOMING#level#1#timestamp#123114335123").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("ONE#INCOMING#level#2#timestamp#123132135362").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("ONE#INCOMING#level#2#timestamp#123132135766").toString(prefix(), hash()), 1);
                root.setProperty(wildcard("ONE#INCOMING#level#1#timestamp#123132135763").toString(prefix(), hash()), 1);

                RelationshipCountCachingNode<CacheableRelationshipDescription> node = cachingNode(compactor);
                compactor.compactRelationshipCounts(node);
                node.flush();
            }
        });

        assertEquals(3, cachingNode(compactor).getCachedCounts().size());
        assertEquals(3, countingNode().getRelationshipCount(wildcard("ONE#INCOMING#level#0")));
        assertEquals(4, countingNode().getRelationshipCount(wildcard("ONE#INCOMING#level#1")));
        assertEquals(3, countingNode().getRelationshipCount(wildcard("ONE#INCOMING#level#2")));

    }

    /**
     * just for readability
     */
    private RelationshipQueryDescription wildcard(String s) {
        return new WildcardRelationshipQueryDescription(s, null, hash());
    }

    private RelationshipQueryDescription literal(String s) {
        return new LiteralRelationshipQueryDescription(s, null, hash());
    }

    private String prefix() {
        return DefaultFrameworkConfiguration.getInstance().createPrefix(FullRelationshipCountModule.FULL_RELCOUNT_DEFAULT_ID);
    }

    private String hash() {
        return "#";
    }
}
