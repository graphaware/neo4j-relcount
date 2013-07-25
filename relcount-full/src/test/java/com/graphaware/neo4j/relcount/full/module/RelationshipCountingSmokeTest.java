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
import com.graphaware.neo4j.relcount.common.AnotherRandomUsageSimulator;
import com.graphaware.neo4j.relcount.common.counter.UnableToCountException;
import com.graphaware.neo4j.relcount.full.counter.FullCachedRelationshipCounter;
import com.graphaware.neo4j.relcount.full.counter.FullRelationshipCounter;
import com.graphaware.neo4j.relcount.full.strategy.RelationshipCountStrategiesImpl;
import com.graphaware.neo4j.utils.TestUtils;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import static com.graphaware.neo4j.relcount.common.AnotherRandomUsageSimulator.FRIEND_OF;
import static com.graphaware.neo4j.relcount.common.AnotherRandomUsageSimulator.LEVEL;
import static org.neo4j.graphdb.Direction.OUTGOING;

/**
 * Smoke test for relationship count consistency, checking with lots of random data.
 */
@Ignore
public class RelationshipCountingSmokeTest {
    private static final Logger LOGGER = Logger.getLogger(RelationshipCountingSmokeTest.class);

    private GraphDatabaseService database;
    private static final int STEPS = 10000;

    @Before
    public void setUp() {
        database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder("/tmp/relcount21")
                .loadPropertiesFromFile("/Users/bachmanm/DEV/graphaware/neo4j-relcount/relcount-full/src/test/resources/neo4j.properties")
                .newGraphDatabase();

        registerShutdownHook(database);

//        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        GraphAwareFramework framework = new GraphAwareFramework(database);
        framework.registerModule(new FullRelationshipCountModule(RelationshipCountStrategiesImpl.defaultStrategies()));
//        framework.registerModule(new FullRelationshipCountModule(RelationshipCountStrategiesImpl.defaultStrategies().with(new RelationshipPropertyInclusionStrategy() {
//            @Override
//            public boolean include(String key, Relationship propertyContainer) {
//                return !key.equals(TIMESTAMP);
//            }
//
//            @Override
//            public int hashCode() {
//                return 123;
//            }
//        })));
        framework.start();
    }

    @Test
    public void smokeTest() {
        final AnotherRandomUsageSimulator simulator = new AnotherRandomUsageSimulator(database);
//        simulator.loadIds();
//        simulator.populateDatabase();

//        long load = TestUtils.time(new TestUtils.Timed() {
//            @Override
//            public void time() {
//                simulator.batchSimulate(STEPS);
//            }
//        });

//        LOGGER.info("Took " + load / 1000 + "s to simulate usage");

//        int fakeCount = count(GlobalGraphOperations.at(database).getAllRelationships());

//        FullRelationshipCounter relationshipCounter = new FullCachedRelationshipCounter(FRIEND_OF, OUTGOING);

        long fake = TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                int fakeCount = 0;
                for (Node node : GlobalGraphOperations.at(database).getAllNodes()) {
                    for (Relationship r : node.getRelationships(FRIEND_OF, OUTGOING)) {
                        if (r.getProperty(LEVEL, 0).equals(3)) {
                            fakeCount += 1;
                        }
                    }
                }
                System.out.println("Fake count: " + fakeCount);
            }
        });

        LOGGER.info("Took " + fake  + "ms to count fake");

        long real = TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                int realCount = 0;
                FullRelationshipCounter relationshipCounter = new FullCachedRelationshipCounter(FRIEND_OF, OUTGOING).with(LEVEL, 3);
                for (Node node : GlobalGraphOperations.at(database).getAllNodes()) {
                    try {
                        realCount += relationshipCounter.count(node);
                    } catch (UnableToCountException e) {
                        System.out.println("Unable to count for node: " + node.getId());
                    }
                }
                System.out.println("Real count: " + realCount);
            }
        });

        LOGGER.info("Took " + real + "ms to count really");


    }

//        assertEquals(fakeCount, realCount);

    private static void registerShutdownHook(final GraphDatabaseService graphDb) {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running example before it's completed)
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();
            }
        });
    }
}