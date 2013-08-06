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

package com.graphaware.relcount.full.module;

import com.graphaware.framework.GraphAwareFramework;
import com.graphaware.relcount.common.AnotherRandomUsageSimulator;
import com.graphaware.relcount.common.counter.UnableToCountException;
import com.graphaware.relcount.full.counter.FullCachedRelationshipCounter;
import com.graphaware.relcount.full.counter.FullRelationshipCounter;
import com.graphaware.relcount.full.strategy.RelationshipCountStrategiesImpl;
import com.graphaware.test.IterableUtils;
import com.graphaware.test.TestUtils;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.HashMap;
import java.util.Map;

import static org.neo4j.graphdb.Direction.OUTGOING;

/**
 * Smoke test for relationship count consistency, checking with lots of random data.
 */
@Ignore
public class RelationshipCountingSmokeTest {
    private static final Logger LOGGER = Logger.getLogger(RelationshipCountingSmokeTest.class);

    private GraphDatabaseService database;
    private static final int STEPS = 10000;

    private static final String DBNAME = "/tmp/relcount-50k-1000-5000-each";
    private static final String CONFIG = "/Users/bachmanm/DEV/graphaware/neo4j-relcount/relcount-full/src/test/resources/neo4j.properties";

    @Before
    public void setUp() {


        database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(DBNAME)
                .loadPropertiesFromFile(CONFIG)
                .newGraphDatabase();

//        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        //database = new TransactionSimulatingBatchGraphDatabase(DBNAME, getDefaultParams());

        registerShutdownHook(database);

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
        framework.start(true);
//        framework.start();
    }

    @Test
    public void smokeTest() {
//        final AnotherRandomUsageSimulator simulator = new AnotherRandomUsageSimulator(database);
//        simulator.loadIds();
//        simulator.populateDatabase();

//        database.shutdown();

//        database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(DBNAME)
//                .loadPropertiesFromFile(CONFIG)
//                .newGraphDatabase();

//        long load = TestUtils.time(new TestUtils.Timed() {
//            @Override
//            public void time() {
//                simulator.batchSimulate(STEPS);
//            }
//        });

//        LOGGER.info("Took " + load / 1000 + "s to simulate usage");

        int fakeCount = IterableUtils.count(GlobalGraphOperations.at(database).getAllRelationships());

        System.out.println("Ultrafake count: " + fakeCount);
//        FullRelationshipCounter relationshipCounter = new FullCachedRelationshipCounter(FRIEND_OF, OUTGOING);

        long fake = TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                int fakeCount = 0;
                for (Node node : GlobalGraphOperations.at(database).getAllNodes()) {
                    for (Relationship r : node.getRelationships(AnotherRandomUsageSimulator.FRIEND_OF, OUTGOING)) {
                        if (r.getProperty(AnotherRandomUsageSimulator.LEVEL, 0).equals(3)) {
                            fakeCount += 1;
                        }
                    }
                }
                System.out.println("Fake count: " + fakeCount);
            }
        });

        LOGGER.info("Took " + fake + "ms to count fake");

        long real = TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                int realCount = 0;
                FullRelationshipCounter relationshipCounter = new FullCachedRelationshipCounter(AnotherRandomUsageSimulator.FRIEND_OF, OUTGOING).with(AnotherRandomUsageSimulator.LEVEL, 3);
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

    private Map<String, String> getDefaultParams() {
        Map<String, String> params = new HashMap<String, String>();
        params.put("neostore.nodestore.db.mapped_memory", "20M");
        params.put("neostore.propertystore.db.mapped_memory", "500M");
        params.put("neostore.propertystore.db.index.mapped_memory", "10M");
        params.put("neostore.propertystore.db.index.keys.mapped_memory", "10M");
        params.put("neostore.propertystore.db.strings.mapped_memory", "500M");
        params.put("neostore.propertystore.db.arrays.mapped_memory", "10M");
        params.put("neostore.relationshipstore.db.mapped_memory", "50M");
        return params;
    }
}