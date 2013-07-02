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

package com.graphaware.neo4j.relcount.logic;

import com.graphaware.neo4j.relcount.dto.ComparableRelationship;
import com.graphaware.neo4j.tx.single.SimpleTransactionExecutor;
import com.graphaware.neo4j.tx.single.TransactionCallback;
import com.graphaware.neo4j.tx.single.TransactionExecutor;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.test.TestGraphDatabaseFactory;

import static com.graphaware.neo4j.common.Constants.GA_REL_PREFIX;
import static junit.framework.Assert.assertEquals;

/**
 * Unit tests for {@link com.graphaware.neo4j.relcount.logic.ThresholdBasedRelationshipCountCompactor}
 */
public class ThresholdBasedRelationshipCountCompactorTest {

    private RelationshipCountCompactor compactor;
    private RelationshipCountManager manager;
    private GraphDatabaseService database;
    private TransactionExecutor executor;

    @Before
    public void setUp() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();
        executor = new SimpleTransactionExecutor(database);
        executor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                database.createNode();
                return null;
            }
        });

        manager = new RelationshipCountManagerImpl();
    }

    @Test
    public void nothingShouldBeCompactedBeforeThresholdIsReached() {
        executor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                Node root = database.getNodeById(0);
                root.setProperty(rel("test#OUTGOING#k1#v1").toString(), 14);
                root.setProperty(rel("test#OUTGOING#k1#v2").toString(), 1);
                root.setProperty(rel("test#OUTGOING#k1#v3").toString(), 2);
                root.setProperty(rel("test#OUTGOING#k1#v4").toString(), 3);
                return null;
            }
        });

        compactor = new ThresholdBasedRelationshipCountCompactor(5, manager);

        executor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                compactor.compactRelationshipCounts(database.getNodeById(0));
                return null;
            }
        });

        assertEquals(4, manager.getRelationshipCounts(database.getNodeById(0)).size());
        assertEquals(14, manager.getRelationshipCount(rel("test#OUTGOING#k1#v1"), database.getNodeById(0)));
        assertEquals(1, manager.getRelationshipCount(rel("test#OUTGOING#k1#v2"), database.getNodeById(0)));
        assertEquals(2, manager.getRelationshipCount(rel("test#OUTGOING#k1#v3"), database.getNodeById(0)));
        assertEquals(3, manager.getRelationshipCount(rel("test#OUTGOING#k1#v4"), database.getNodeById(0)));
        assertEquals(20, manager.getRelationshipCount(rel("test#OUTGOING#"), database.getNodeById(0)));
    }

    @Test
    public void countShouldBeCompactedWhenThresholdIsReached() {
        executor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                Node root = database.getNodeById(0);
                root.setProperty(rel("test#OUTGOING#k1#v1").toString(), 14);
                root.setProperty(rel("test#OUTGOING#k1#v2").toString(), 1);
                root.setProperty(rel("test#OUTGOING#k1#v3").toString(), 2);
                root.setProperty(rel("test#OUTGOING#k1#v4").toString(), 3);
                root.setProperty(rel("test#OUTGOING#k1#v5").toString(), 4);
                return null;
            }
        });

        compactor = new ThresholdBasedRelationshipCountCompactor(5, manager);

        executor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                compactor.compactRelationshipCounts(database.getNodeById(0));
                return null;
            }
        });

        assertEquals(1, manager.getRelationshipCounts(database.getNodeById(0)).size());
        assertEquals(0, manager.getRelationshipCount(rel("test#OUTGOING#k1#v1"), database.getNodeById(0)));
        assertEquals(0, manager.getRelationshipCount(rel("test#OUTGOING#k1#v2"), database.getNodeById(0)));
        assertEquals(0, manager.getRelationshipCount(rel("test#OUTGOING#k1#v3"), database.getNodeById(0)));
        assertEquals(0, manager.getRelationshipCount(rel("test#OUTGOING#k1#v4"), database.getNodeById(0)));
        assertEquals(24, manager.getRelationshipCount(rel("test#OUTGOING#"), database.getNodeById(0)));
    }

    @Test
    public void verifyMultipleCompactions() {
        executor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                Node root = database.getNodeById(0);
                root.setProperty(rel("test#OUTGOING#k1#v1#k2#v1").toString(), 1);
                root.setProperty(rel("test#OUTGOING#k1#v1#k2#v2").toString(), 1);
                root.setProperty(rel("test#OUTGOING#k1#v1#k2#v3").toString(), 1);
                root.setProperty(rel("test#OUTGOING#k1#v1#k2#v4").toString(), 1);
                root.setProperty(rel("test#OUTGOING#k1#v2#k2#v1").toString(), 1);
                root.setProperty(rel("test#OUTGOING#k1#v2#k2#v2").toString(), 1);
                root.setProperty(rel("test#OUTGOING#k1#v2#k2#v3").toString(), 1);
                root.setProperty(rel("test#OUTGOING#k1#v2#k2#v4").toString(), 1);
                return null;
            }
        });

        compactor = new ThresholdBasedRelationshipCountCompactor(5, manager);

        executor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                compactor.compactRelationshipCounts(database.getNodeById(0));
                return null;
            }
        });

        assertEquals(2, manager.getRelationshipCounts(database.getNodeById(0)).size());
        assertEquals(4, manager.getRelationshipCount(rel("test#OUTGOING#k1#v1"), database.getNodeById(0)));
        assertEquals(4, manager.getRelationshipCount(rel("test#OUTGOING#k1#v2"), database.getNodeById(0)));
        assertEquals(8, manager.getRelationshipCount(rel("test#OUTGOING#"), database.getNodeById(0)));
    }

    @Test
    public void verifyMultiLevelCompaction() {
        executor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                Node root = database.getNodeById(0);
                root.setProperty(rel("test#OUTGOING#k1#v1#k2#v1#k3#v1").toString(), 1);
                root.setProperty(rel("test#OUTGOING#k1#v1#k2#v2#k3#v2").toString(), 1);
                root.setProperty(rel("test#OUTGOING#k1#v1#k2#v3#k3#v3").toString(), 1);
                root.setProperty(rel("test#OUTGOING#k1#v1#k2#v4#k3#v4").toString(), 1);
                root.setProperty(rel("test#OUTGOING#k1#v1#k2#v5#k3#v5").toString(), 1);
                return null;
            }
        });

        compactor = new ThresholdBasedRelationshipCountCompactor(5, manager);

        executor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                compactor.compactRelationshipCounts(database.getNodeById(0));
                return null;
            }
        });

        assertEquals(1, manager.getRelationshipCounts(database.getNodeById(0)).size());
        assertEquals(5, manager.getRelationshipCount(rel("test#OUTGOING#k1#v1"), database.getNodeById(0)));
        assertEquals(5, manager.getRelationshipCount(rel("test#OUTGOING#"), database.getNodeById(0)));
    }

    @Test
    public void verifyImpossibleCompaction() {
        executor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                Node root = database.getNodeById(0);
                root.setProperty(rel("test#OUTGOING#k1#v1#k2#v1#k3#v1").toString(), 1);
                root.setProperty(rel("test#OUTGOING#k1#v1#k2#v2#k3#v2").toString(), 1);
                root.setProperty(rel("test#OUTGOING#k1#v1#k2#v3#k3#v3").toString(), 1);
                root.setProperty(rel("test#OUTGOING#k1#v1#k2#v4#k3#v4").toString(), 1);
                root.setProperty(rel("test#OUTGOING#k1#v1#k2#v5#k3#v5").toString(), 1);
                root.setProperty(rel("test#OUTGOING#k2#v2").toString(), 1);
                root.setProperty(rel("test2#OUTGOING#k2#v2").toString(), 1);
                root.setProperty(rel("test3#OUTGOING#k2#v2").toString(), 1);
                root.setProperty(rel("test4#OUTGOING#k2#v2").toString(), 1);
                root.setProperty(rel("test5#OUTGOING#k2#v2").toString(), 1);
                return null;
            }
        });

        compactor = new ThresholdBasedRelationshipCountCompactor(5, manager);

        executor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                compactor.compactRelationshipCounts(database.getNodeById(0));
                return null;
            }
        });

        assertEquals(5, manager.getRelationshipCounts(database.getNodeById(0)).size());
        assertEquals(0, manager.getRelationshipCount(rel("test#OUTGOING#k1#v1"), database.getNodeById(0)));
        assertEquals(6, manager.getRelationshipCount(rel("test#OUTGOING#"), database.getNodeById(0)));
        assertEquals(1, manager.getRelationshipCount(rel("test2#OUTGOING#"), database.getNodeById(0)));
        assertEquals(1, manager.getRelationshipCount(rel("test3#OUTGOING#"), database.getNodeById(0)));
        assertEquals(1, manager.getRelationshipCount(rel("test4#OUTGOING#"), database.getNodeById(0)));
        assertEquals(1, manager.getRelationshipCount(rel("test5#OUTGOING#"), database.getNodeById(0)));
    }

    /**
     * just for readability
     */
    private ComparableRelationship rel(String s) {
        return new ComparableRelationship(GA_REL_PREFIX + s);
    }
}
