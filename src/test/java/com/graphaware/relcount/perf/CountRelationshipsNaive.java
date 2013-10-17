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

import com.graphaware.description.relationship.DetachedRelationshipDescription;
import com.graphaware.framework.GraphAwareFramework;
import com.graphaware.performance.Exponential;
import com.graphaware.performance.Parameter;
import com.graphaware.relcount.count.NaiveRelationshipCounter;
import com.graphaware.relcount.count.RelationshipCounter;
import com.graphaware.relcount.module.RelationshipCountModule;
import com.graphaware.test.TestUtils;
import com.graphaware.tx.executor.NullItem;
import com.graphaware.tx.executor.batch.NoInputBatchTransactionExecutor;
import com.graphaware.tx.executor.batch.UnitOfWork;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.graphaware.description.predicate.Predicates.equalTo;
import static com.graphaware.description.relationship.RelationshipDescriptionFactory.wildcard;

/**
 * Performance test for counting relationships.
 */
public class CountRelationshipsNaive extends RelcountPerformanceTest {

    private static final String DEGREE = "degree";
    private static final String CACHE = "cache";

    private static final int NO_NODES = 100;
    private static final int COUNT_NO = 10;

    private int lastAvgDegree = 10;

    enum FrameworkInvolvement {
        //NO_FRAMEWORK,
        NAIVE,
        //CACHED
    }

    private enum Properties {
        //NO_PROPS,
        //TWO_PROPS,
    }

    @Override
    public String shortName() {
        return "countRelationshipsNaive";
    }

    @Override
    public String longName() {
        return "Count degree of 10 random nodes";
    }

    @Override
    public List<Parameter> parameters() {
        List<Parameter> result = new LinkedList<>();

        //result.add(new CacheParameter(CACHE));
        result.add(new Exponential(DEGREE, 10, 3, 3, 0.25));
        //result.add(new EnumParameter(FW, FrameworkInvolvement.class));
        //result.add(new EnumParameter(PROPS, Properties.class));

        return result;
    }

    @Override
    public int dryRuns(Map<String, Object> params) {
        return 10000;
    }

    @Override
    public int measuredRuns() {
        return 100;
    }

    @Override
    public Map<String, String> databaseParameters(Map<String, Object> params) {
        return Collections.emptyMap();
    }

    @Override
    public void prepareDatabase(GraphDatabaseService database, Map<String, Object> params) {
        GraphAwareFramework framework = new GraphAwareFramework(database);
        framework.registerModule(new RelationshipCountModule());
        framework.start();

        new NoInputBatchTransactionExecutor(database, 1000, NO_NODES, new UnitOfWork<NullItem>() {
            @Override
            public void execute(GraphDatabaseService database, NullItem input, int batchNumber, int stepNumber) {
                database.createNode();
            }
        }).execute();

        int noRelationships = NO_NODES * (int) params.get(DEGREE) / 2;

        new NoInputBatchTransactionExecutor(database, 1000, noRelationships, new UnitOfWork<NullItem>() {
            @Override
            public void execute(GraphDatabaseService database, NullItem input, int batchNumber, int stepNumber) {
                final Node node1 = randomNode(database, NO_NODES);
                final Node node2 = randomNode(database, NO_NODES);
                Relationship relationship = node1.createRelationshipTo(node2, randomType());
                relationship.setProperty("rating", RANDOM.nextInt(2));
                relationship.setProperty("another", RANDOM.nextInt(2));
            }
        }).execute();
    }

    @Override
    public long run(final GraphDatabaseService database, final Map<String, Object> params) {
        long time = 0;

        final AtomicLong result = new AtomicLong(0);
        for (int i = 0; i < COUNT_NO; i++) {

            time += TestUtils.time(new TestUtils.Timed() {
                @Override
                public void time() {
//                    FrameworkInvolvement frameworkInvolvement = ((FrameworkInvolvement) params.get(FW));
//                    switch (frameworkInvolvement) {
//                        case NO_FRAMEWORK:
//                            countAsIfThereWasNoFramework(database, params);
//                            break;
//                        case CACHED:
//                            countUsingFramework(database, params, new CachedRelationshipCounter());
//                            break;
//                        case NAIVE:
                    countUsingFramework(database, params, new NaiveRelationshipCounter());
//                            break;
//                        default:
//                            throw new RuntimeException("unknown option");
                    //    }
                }
            });
        }

        return time;
    }

    private long countAsIfThereWasNoFramework(final GraphDatabaseService database, Map<String, Object> params) {
        final AtomicLong result = new AtomicLong(0);

        final Node node = randomNode(database, NO_NODES);
        for (Relationship r : node.getRelationships(randomType(), randomDirection())) {
            if (RANDOM.nextInt(2) == r.getProperty("rating") && RANDOM.nextInt(2) == r.getProperty("another")) {
                result.incrementAndGet();
            }
        }

        return result.get();
    }

    protected long countUsingFramework(final GraphDatabaseService database, Map<String, Object> params, RelationshipCounter counter) {
        final Node node = randomNode(database, NO_NODES);
        DetachedRelationshipDescription description = wildcard(randomType(), randomDirection()).with("rating", equalTo(RANDOM.nextInt(2))).with("another", equalTo(RANDOM.nextInt(2)));
        return counter.count(node, description);
    }

    @Override
    public RebuildDatabase rebuildDatabase() {
        return RebuildDatabase.TEST_DECIDES;
    }

    @Override
    public boolean rebuildDatabase(Map<String, Object> params) {
        int degree = (int) params.get(DEGREE);
        boolean result = lastAvgDegree != degree;
        lastAvgDegree = degree;
        return result;
    }
}
