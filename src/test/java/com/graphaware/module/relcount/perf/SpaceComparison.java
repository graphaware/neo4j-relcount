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

package com.graphaware.module.relcount.perf;

import com.graphaware.module.relcount.RelationshipCountModule;
import com.graphaware.runtime.GraphAwareRuntime;
import com.graphaware.runtime.GraphAwareRuntimeFactory;
import com.graphaware.tx.executor.NullItem;
import com.graphaware.tx.executor.batch.NoInputBatchTransactionExecutor;
import com.graphaware.tx.executor.batch.UnitOfWork;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.util.Random;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

@Ignore
public class SpaceComparison {

    protected static final Random RANDOM = new Random(System.currentTimeMillis());

    @Test
    public void createTwoDatabases() {
        GraphDatabaseService one = new GraphDatabaseFactory().newEmbeddedDatabase("/tmp/space/one");
        populateDatabase(one);
        one.shutdown();

        GraphDatabaseService two = new GraphDatabaseFactory().newEmbeddedDatabase("/tmp/space/two");
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(two);
        runtime.registerModule(new RelationshipCountModule());
        runtime.start();
        populateDatabase(two);
        two.shutdown();
    }

    private void populateDatabase(GraphDatabaseService database) {

        new NoInputBatchTransactionExecutor(database, 1000, 1000, new UnitOfWork<NullItem>() {
            @Override
            public void execute(GraphDatabaseService database, NullItem input, int batchNumber, int stepNumber) {
                database.createNode();
            }
        }).execute();

        new NoInputBatchTransactionExecutor(database, 1000, 1000000, new UnitOfWork<NullItem>() {
            @Override
            public void execute(GraphDatabaseService database, NullItem input, int batchNumber, int stepNumber) {
                final Node node1 = database.getNodeById(RANDOM.nextInt(1000) + 1);
                final Node node2 = database.getNodeById(RANDOM.nextInt(1000) + 1);

                Relationship rel = node1.createRelationshipTo(node2, withName("TEST" + ((1000 * (batchNumber - 1) + stepNumber) % 2)));
                rel.setProperty("rating", RANDOM.nextInt(5) + 1);
                rel.setProperty("timestamp", RANDOM.nextLong());
            }
        }).execute();
    }
}
