/*
 * Copyright (c) 2013-2016 GraphAware
 *
 * This file is part of the GraphAware Framework.
 *
 * GraphAware Framework is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

package com.graphaware.module.relcount.cache;

import com.graphaware.common.description.relationship.DetachedRelationshipDescription;
import com.graphaware.common.serialize.Serializer;
import com.graphaware.tx.executor.single.SimpleTransactionExecutor;
import com.graphaware.tx.executor.single.TransactionExecutor;
import com.graphaware.tx.executor.single.VoidReturningCallback;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.graphaware.common.description.predicate.Predicates.any;
import static com.graphaware.common.description.predicate.Predicates.equalTo;
import static com.graphaware.common.description.relationship.RelationshipDescriptionFactory.literal;
import static org.junit.Assert.*;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

/**
 * Unit test for {@link com.graphaware.module.relcount.cache.DegreeCachingStrategy}s.
 */
public abstract class DegreeCachingStrategyTest {

    private GraphDatabaseService database;
    private TransactionExecutor txExecutor;

    @Before
    public void setUp() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        try (Transaction tx = database.beginTx()) {
            database.createNode(); //ID=0
            tx.success();
        }

        txExecutor = new SimpleTransactionExecutor(database);
    }

    @After
    public void tearDown() {
        database.shutdown();
    }

    @Test
    public void shouldReadEmptyDegreesWhenNoDegreesHaveBeenWritten() {
        try (Transaction tx = database.beginTx()) {
            assertTrue(strategy().readDegrees(database.getNodeById(0), "TEST").isEmpty());
        }
    }

    @Test
    public void shouldReadEmptyDegreesWhenEmptyDegreesHaveBeenWritten() {
        txExecutor.executeInTransaction(new VoidReturningCallback() {
            @Override
            protected void doInTx(GraphDatabaseService database) {
                strategy().writeDegrees(database.getNodeById(0), "TEST", new HashMap<DetachedRelationshipDescription, Integer>(), new HashSet<DetachedRelationshipDescription>(), new HashSet<DetachedRelationshipDescription>());
            }
        });

        try (Transaction tx = database.beginTx()) {
            assertTrue(strategy().readDegrees(database.getNodeById(0), "TEST").isEmpty());
        }
    }

    @Test
    public void readDegreesShouldEqualToWrittenOnes() {
        final Map<DetachedRelationshipDescription, Integer> cachedDegrees = new HashMap<>();
        cachedDegrees.put(literal("TEST", OUTGOING).with("k1", equalTo("v1")), 1);
        cachedDegrees.put(literal("TEST", OUTGOING).with("k1", equalTo("v2")), 2);
        cachedDegrees.put(literal("TEST", INCOMING).with("k2", any()), 3);

        final Set<DetachedRelationshipDescription> updatedDegrees = new HashSet<>();
        updatedDegrees.add(literal("TEST", OUTGOING).with("k1", equalTo("v1")));
        updatedDegrees.add(literal("TEST", OUTGOING).with("k1", equalTo("v2")));
        updatedDegrees.add(literal("TEST", INCOMING).with("k2", any()));

        txExecutor.executeInTransaction(new VoidReturningCallback() {
            @Override
            protected void doInTx(GraphDatabaseService database) {
                strategy().writeDegrees(database.getNodeById(0), "TEST", cachedDegrees, updatedDegrees, new HashSet<DetachedRelationshipDescription>());
            }
        });

        try (Transaction tx = database.beginTx()) {
            assertEquals(cachedDegrees, strategy().readDegrees(database.getNodeById(0), "TEST"));
        }

        final Map<DetachedRelationshipDescription, Integer> cachedDegrees2 = new HashMap<>();
        cachedDegrees2.put(literal("TEST", OUTGOING).with("k1", equalTo("v1")), 1);
        cachedDegrees2.put(literal("TEST", INCOMING).with("k2", any()), 2);

        final Set<DetachedRelationshipDescription> updatedDegrees2 = new HashSet<>();
        updatedDegrees2.add(literal("TEST", INCOMING).with("k2", any()));

        final Set<DetachedRelationshipDescription> removedDegrees2 = new HashSet<>();
        removedDegrees2.add(literal("TEST", OUTGOING).with("k1", equalTo("v2")));

        txExecutor.executeInTransaction(new VoidReturningCallback() {
            @Override
            protected void doInTx(GraphDatabaseService database) {
                strategy().writeDegrees(database.getNodeById(0), "TEST", cachedDegrees2, updatedDegrees2, removedDegrees2);
            }
        });

        try (Transaction tx = database.beginTx()) {
            assertEquals(cachedDegrees2, strategy().readDegrees(database.getNodeById(0), "TEST"));
        }
    }

    @Test
    public void strategiesShouldHaveDifferentStringSerializations() {
        assertNotSame(Serializer.toString(new SingleNodePropertyDegreeCachingStrategy(), "test"), Serializer.toString(new NodePropertiesDegreeCachingStrategy(), "test"));
    }

    protected abstract DegreeCachingStrategy strategy();
}
