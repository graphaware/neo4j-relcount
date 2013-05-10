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

package com.graphaware.neo4j.relcount;

import com.graphaware.neo4j.relcount.api.RelationshipCounter;
import com.graphaware.neo4j.relcount.api.RelationshipCounterImpl;
import com.graphaware.neo4j.utils.test.RandomUsageSimulator;
import com.graphaware.neo4j.utils.test.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import static com.graphaware.neo4j.utils.iterable.IterableUtils.count;
import static com.graphaware.neo4j.utils.test.RandomUsageSimulator.FRIEND_OF;
import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Direction.OUTGOING;

/**
 * Smoke test for relationship count consistency, checking with lots of random data.
 */
public class RelationshipCountingSmokeTest {

    private GraphDatabaseService database;
    private static final int STEPS = 1000;

    @Before
    public void setUp() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();
        database.registerTransactionEventHandler(new RelationshipCountTransactionEventHandlerFactory().create());
        System.out.println(System.currentTimeMillis());
    }

    @Test
    public void smokeTest() {
        long load = TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                RandomUsageSimulator simulator = new RandomUsageSimulator(database);
                simulator.simulate(STEPS);
            }
        });

        System.out.println("Took " + load / 1000 + "s to simulate usage");

        int fakeCount = count(GlobalGraphOperations.at(database).getAllRelationships());

        int realCount = 0;
        RelationshipCounterImpl relationshipCounter = new RelationshipCounterImpl(FRIEND_OF, OUTGOING);
        for (Node node : GlobalGraphOperations.at(database).getAllNodes()) {
            realCount += relationshipCounter.count(node);
        }

        assertEquals(fakeCount, realCount);
    }

    private void demo() {

    }
}
