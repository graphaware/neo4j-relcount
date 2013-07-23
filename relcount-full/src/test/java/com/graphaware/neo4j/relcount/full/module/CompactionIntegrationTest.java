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

package com.graphaware.neo4j.relcount.full.module;

import com.graphaware.neo4j.framework.GraphAwareFramework;
import com.graphaware.neo4j.framework.config.DefaultFrameworkConfiguration;
import com.graphaware.neo4j.relcount.common.api.UnableToCountException;
import com.graphaware.neo4j.relcount.full.dto.relationship.CompactibleRelationshipImpl;
import com.graphaware.neo4j.relcount.full.dto.relationship.RelationshipDescription;
import com.graphaware.neo4j.relcount.full.dto.relationship.WildcardRelationshipDescription;
import com.graphaware.neo4j.relcount.full.logic.FullCachedRelationshipCountReader;
import com.graphaware.neo4j.relcount.full.logic.FullRelationshipCountCache;
import com.graphaware.neo4j.relcount.full.logic.RelationshipCountCompactor;
import com.graphaware.neo4j.relcount.full.logic.ThresholdBasedRelationshipCountCompactor;
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

import static com.graphaware.neo4j.relcount.full.Constants.FULL_RELCOUNT_DEFAULT_ID;
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

        FullRelationshipCountModule module = new FullRelationshipCountModule(RelationshipCountStrategiesImpl.defaultStrategies().with(5));
        cache = (FullRelationshipCountCache) module.getRelationshipCountCache();
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
                root.setProperty(rel("test#OUTGOING#k1#v1").toString(prefix(), "#"), 14);
                root.setProperty(rel("test#OUTGOING#k1#v2").toString(prefix(), "#"), 1);
                root.setProperty(rel("test#OUTGOING#k1#v3").toString(prefix(), "#"), 2);
                root.setProperty(rel("test#OUTGOING#k1#v4").toString(prefix(), "#"), 3);

                compactor.compactRelationshipCounts(root);
            }
        });

        assertEquals(4, cache.getRelationshipCounts(database.getNodeById(0)).size());
        assertEquals(14, reader.getRelationshipCount(rel("test#OUTGOING#k1#v1"), database.getNodeById(0)));
        assertEquals(1, reader.getRelationshipCount(rel("test#OUTGOING#k1#v2"), database.getNodeById(0)));
        assertEquals(2, reader.getRelationshipCount(rel("test#OUTGOING#k1#v3"), database.getNodeById(0)));
        assertEquals(3, reader.getRelationshipCount(rel("test#OUTGOING#k1#v4"), database.getNodeById(0)));
        assertEquals(20, reader.getRelationshipCount(rel("test#OUTGOING#"), database.getNodeById(0)));
    }

    private String prefix() {
        return DefaultFrameworkConfiguration.getInstance().createPrefix(FULL_RELCOUNT_DEFAULT_ID);
    }

    @Test
    public void countShouldBeCompactedWhenThresholdIsReached() {
        executor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                Node root = database.getNodeById(0);
                root.setProperty(rel("test#OUTGOING#k1#v1").toString(prefix(), "#"), 14);
                root.setProperty(rel("test#OUTGOING#k1#v2").toString(prefix(), "#"), 1);
                root.setProperty(rel("test#OUTGOING#k1#v3").toString(prefix(), "#"), 2);
                root.setProperty(rel("test#OUTGOING#k1#v4").toString(prefix(), "#"), 3);
                root.setProperty(rel("test#OUTGOING#k1#v5").toString(prefix(), "#"), 4);

                compactor.compactRelationshipCounts(root);
            }
        });

        assertEquals(1, cache.getRelationshipCounts(database.getNodeById(0)).size());
        assertTrue(cache.getRelationshipCounts(database.getNodeById(0)).containsKey(new CompactibleRelationshipImpl("test#OUTGOING#k1#_ANY_", null, "#")));
        assertEquals(24, reader.getRelationshipCount(rel("test#OUTGOING#"), database.getNodeById(0)));

        try {
            reader.getRelationshipCount(rel("test#OUTGOING#k1#v1"), database.getNodeById(0));
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
                root.setProperty(rel("test#OUTGOING#k1#v1#k2#v1").toString(prefix(), "#"), 1);
                root.setProperty(rel("test#OUTGOING#k1#v1#k2#v2").toString(prefix(), "#"), 1);
                root.setProperty(rel("test#OUTGOING#k1#v1#k2#v3").toString(prefix(), "#"), 1);
                root.setProperty(rel("test#OUTGOING#k1#v1#k2#v4").toString(prefix(), "#"), 1);
                root.setProperty(rel("test#OUTGOING#k1#v2#k2#v1").toString(prefix(), "#"), 1);
                root.setProperty(rel("test#OUTGOING#k1#v2#k2#v2").toString(prefix(), "#"), 1);
                root.setProperty(rel("test#OUTGOING#k1#v2#k2#v3").toString(prefix(), "#"), 1);
                root.setProperty(rel("test#OUTGOING#k1#v2#k2#v4").toString(prefix(), "#"), 1);

                compactor.compactRelationshipCounts(root);
            }
        });

        assertEquals(2, cache.getRelationshipCounts(database.getNodeById(0)).size());
        assertEquals(4, reader.getRelationshipCount(rel("test#OUTGOING#k1#v1"), database.getNodeById(0)));
        assertEquals(4, reader.getRelationshipCount(rel("test#OUTGOING#k1#v2"), database.getNodeById(0)));
        assertEquals(8, reader.getRelationshipCount(rel("test#OUTGOING#"), database.getNodeById(0)));

        try {
            reader.getRelationshipCount(rel("test#OUTGOING#k2#v3"), database.getNodeById(0));
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
                root.setProperty(rel("test#OUTGOING#k1#v1#k2#v1#k3#v1").toString(prefix(), "#"), 1);
                root.setProperty(rel("test#OUTGOING#k1#v1#k2#v2#k3#v2").toString(prefix(), "#"), 1);
                root.setProperty(rel("test#OUTGOING#k1#v1#k2#v3#k3#v3").toString(prefix(), "#"), 1);
                root.setProperty(rel("test#OUTGOING#k1#v1#k2#v4#k3#v4").toString(prefix(), "#"), 1);
                root.setProperty(rel("test#OUTGOING#k1#v1#k2#v5#k3#v5").toString(prefix(), "#"), 1);

                compactor.compactRelationshipCounts(root);
            }
        });

        assertEquals(1, cache.getRelationshipCounts(database.getNodeById(0)).size());
        assertEquals(5, reader.getRelationshipCount(rel("test#OUTGOING#k1#v1"), database.getNodeById(0)));
        assertEquals(5, reader.getRelationshipCount(rel("test#OUTGOING#"), database.getNodeById(0)));

        try {
            reader.getRelationshipCount(rel("test#OUTGOING#k3#v4"), database.getNodeById(0));
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
                root.setProperty(rel("test#OUTGOING#k1#v1#k2#v1#k3#v1").toString(prefix(), "#"), 1);
                root.setProperty(rel("test#OUTGOING#k1#v1#k2#v2#k3#v2").toString(prefix(), "#"), 1);
                root.setProperty(rel("test#OUTGOING#k1#v1#k2#v3#k3#v3").toString(prefix(), "#"), 1);
                root.setProperty(rel("test#OUTGOING#k1#v1#k2#v4#k3#v4").toString(prefix(), "#"), 1);
                root.setProperty(rel("test#OUTGOING#k1#v1#k2#v5#k3#v5").toString(prefix(), "#"), 1);
                root.setProperty(rel("test#OUTGOING#k2#v2").toString(prefix(), "#"), 1);
                root.setProperty(rel("test2#OUTGOING#k2#v2").toString(prefix(), "#"), 1);
                root.setProperty(rel("test3#OUTGOING#k2#v2").toString(prefix(), "#"), 1);
                root.setProperty(rel("test4#OUTGOING#k2#v2").toString(prefix(), "#"), 1);
                root.setProperty(rel("test5#OUTGOING#k2#v2").toString(prefix(), "#"), 1);

                compactor.compactRelationshipCounts(root);
            }
        });

        assertEquals(5, cache.getRelationshipCounts(database.getNodeById(0)).size());
        assertEquals(6, reader.getRelationshipCount(rel("test#OUTGOING#"), database.getNodeById(0)));
        assertEquals(1, reader.getRelationshipCount(rel("test2#OUTGOING#"), database.getNodeById(0)));
        assertEquals(1, reader.getRelationshipCount(rel("test3#OUTGOING#"), database.getNodeById(0)));
        assertEquals(1, reader.getRelationshipCount(rel("test4#OUTGOING#"), database.getNodeById(0)));
        assertEquals(1, reader.getRelationshipCount(rel("test5#OUTGOING#"), database.getNodeById(0)));

        try {
            reader.getRelationshipCount(rel("test#OUTGOING#k1#v1"), database.getNodeById(0));
            fail();
        } catch (UnableToCountException e) {
            //ok
        }
    }

    /**
     * just for readability
     */
    private RelationshipDescription rel(String s) {
        return new WildcardRelationshipDescription(s, null, "#");
    }
}
