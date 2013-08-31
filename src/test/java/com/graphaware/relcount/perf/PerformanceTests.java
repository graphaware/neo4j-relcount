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

package com.graphaware.relcount.perf;

import com.graphaware.test.TestUtils;
import com.graphaware.tx.executor.NullItem;
import com.graphaware.tx.executor.batch.NoInputBatchTransactionExecutor;
import com.graphaware.tx.executor.batch.UnitOfWork;
import com.graphaware.tx.executor.single.SimpleTransactionExecutor;
import com.graphaware.tx.executor.single.VoidReturningCallback;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.HashMap;
import java.util.Map;

/**
 * Smoke test for relationship count consistency, checking with lots of random data.
 */
@Ignore
public class PerformanceTests {
    private static final Logger LOGGER = Logger.getLogger(PerformanceTests.class);

    private GraphDatabaseService database;
    private static final int STEPS = 10000;

    private static final String DBNAME = "/tmp/avg-million3";
    private static final String CONFIG = "/Users/bachmanm/DEV/graphaware/neo4j-relcount/src/test/resources/neo4j.properties";

    @Before
    public void setUp() {
        database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(DBNAME).loadPropertiesFromFile(CONFIG).newGraphDatabase();
//        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        registerShutdownHook(database);
    }

    @Test
    public void countingTest() {
//        populateDatabase();

        countSum();
        countSum();
        countSum();
        countSum();
        countSum();
        countSum();
    }

    private void countSum() {
        long time = TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                double sum = 0.0;
                for (Node node : GlobalGraphOperations.at(database).getAllNodes()) {
                    sum += (double) node.getProperty("age");
                }
                System.out.println("Sum: " + sum);
            }
        });

        LOGGER.info("Took " + time + "ms to count sum");
    }

    private void populateDatabase() {
        new SimpleTransactionExecutor(database).executeInTransaction(new VoidReturningCallback() {
            @Override
            protected void doInTx(GraphDatabaseService database) {
                database.getNodeById(0).setProperty("age", Math.random());
            }
        });

        new NoInputBatchTransactionExecutor(database, 1000, 1000000, new UnitOfWork<NullItem>() {
            @Override
            public void execute(GraphDatabaseService database, NullItem input, int batchNumber, int stepNumber) {
                Node node = database.createNode();
                node.setProperty("age", Math.random());
            }
        }).execute();
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
