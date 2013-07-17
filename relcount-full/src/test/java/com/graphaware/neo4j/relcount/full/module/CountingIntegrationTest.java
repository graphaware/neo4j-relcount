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
import com.graphaware.neo4j.relcount.full.Constants;
import com.graphaware.neo4j.relcount.full.dto.relationship.GeneralRelationshipDescription;
import com.graphaware.neo4j.relcount.full.dto.relationship.LiteralRelationshipDescription;
import com.graphaware.neo4j.relcount.full.dto.relationship.RelationshipDescription;
import com.graphaware.neo4j.relcount.full.logic.FullCachedRelationshipCountReader;
import com.graphaware.neo4j.relcount.full.logic.FullRelationshipCountCache;
import com.graphaware.neo4j.relcount.full.strategy.RelationshipCountStrategiesImpl;
import com.graphaware.neo4j.tx.single.SimpleTransactionExecutor;
import com.graphaware.neo4j.tx.single.TransactionCallback;
import com.graphaware.neo4j.tx.single.TransactionExecutor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Iterator;
import java.util.Map;

import static com.graphaware.neo4j.relcount.full.Constants.FULL_RELCOUNT_DEFAULT_ID;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link com.graphaware.neo4j.relcount.full.logic.FullRelationshipCountCache}.
 */
public class CountingIntegrationTest {

    private FullRelationshipCountCache cache;
    private FullCachedRelationshipCountReader reader;
    private GraphDatabaseService database;
    private TransactionExecutor txExecutor;

    @Before
    public void setUp() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();
        txExecutor = new SimpleTransactionExecutor(database);

        FullRelationshipCountModule module = new FullRelationshipCountModule(RelationshipCountStrategiesImpl.defaultStrategies().with(5));
        cache = (FullRelationshipCountCache) module.getRelationshipCountCache();

        GraphAwareFramework framework = new GraphAwareFramework(database);
        framework.registerModule(module);
        framework.start();

        reader = new FullCachedRelationshipCountReader(FULL_RELCOUNT_DEFAULT_ID);
    }

    @After
    public void tearDown() {
        database.shutdown();
    }

    @Test
    public void shouldCorrectlyReportBasicRelationshipCounts() {
        setUpBasicRelationshipCounts();

        final Node root = database.getNodeById(0);

        assertEquals(2, reader.getRelationshipCount(gen("test#OUTGOING#key1#value1"), root));
        assertEquals(2, reader.getRelationshipCount(lit("test#OUTGOING#key1#value1"), root));
        assertEquals(3, reader.getRelationshipCount(gen("test#OUTGOING#key1#value2"), root));
        assertEquals(3, reader.getRelationshipCount(lit("test#OUTGOING#key1#value2"), root));
        assertEquals(4, reader.getRelationshipCount(gen("test#OUTGOING#key2#value2"), root));
        assertEquals(4, reader.getRelationshipCount(lit("test#OUTGOING#key2#value2"), root));
        assertEquals(0, reader.getRelationshipCount(gen("test#OUTGOING#key1#value3"), root));
        assertEquals(0, reader.getRelationshipCount(lit("test#OUTGOING#key1#value3"), root));
        assertEquals(0, reader.getRelationshipCount(gen("test#INCOMING#key1#value1"), root));
        assertEquals(0, reader.getRelationshipCount(lit("test#INCOMING#key1#value1"), root));
    }

    @Test
    public void shouldCorrectlyReportAggregatedCounts() {
        setUpBasicRelationshipCounts();

        final Node root = database.getNodeById(0);

        assertEquals(9, reader.getRelationshipCount(gen("test#OUTGOING"), root));
        assertEquals(0, reader.getRelationshipCount(gen("wrong#OUTGOING"), root));
        assertEquals(0, reader.getRelationshipCount(gen("test#INCOMING"), root));
    }

    @Test
    public void shouldCorrectlyReportAggregatedCountsWhenGeneralityIsMixed() {
        setUpRelationshipCounts();

        final Node root = database.getNodeById(0);

        assertEquals(39, reader.getRelationshipCount(gen("test#OUTGOING"), root));
        assertEquals(11, reader.getRelationshipCount(gen("test#OUTGOING#key1#value2"), root));
        assertEquals(15, reader.getRelationshipCount(gen("test#OUTGOING#key1#value1"), root));
        assertEquals(3, reader.getRelationshipCount(gen("test#OUTGOING#key1#value1#key2#value2"), root));
    }

    @Test
    public void shouldNotAggregateCountsWhenAskedForAll() {
        setUpRelationshipCounts();

        final Node root = database.getNodeById(0);

        Map<RelationshipDescription, Integer> relationshipCounts = cache.getRelationshipCounts(root);
        Iterator<Map.Entry<RelationshipDescription, Integer>> iterator = relationshipCounts.entrySet().iterator();

        //also testing order

        Map.Entry<RelationshipDescription, Integer> next = iterator.next();
        assertEquals(gen("test#OUTGOING#key1#value1#key2#value2"), next.getKey());
        assertEquals(3, (int) next.getValue());

        next = iterator.next();
        assertEquals(gen("test#OUTGOING#key1#value1#key2#value3"), next.getKey());
        assertEquals(5, (int) next.getValue());

        next = iterator.next();
        assertEquals(gen("test#OUTGOING#key1#value1"), next.getKey());
        assertEquals(7, (int) next.getValue());

        next = iterator.next();
        assertEquals(gen("test#OUTGOING#key1#value2"), next.getKey());
        assertEquals(11, (int) next.getValue());

        next = iterator.next();
        assertEquals(gen("test#OUTGOING"), next.getKey());
        assertEquals(13, (int) next.getValue());

        assertEquals(5, relationshipCounts.size());
    }

    @Test
    public void incrementingCountOnNonExistingCachedRelationshipShouldMakeItOne() {
        setUpBasicRelationshipCounts();

        final Node root = database.getNodeById(0);

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                assertTrue(cache.incrementCount(gen("test#OUTGOING#key1#value3"), root, 1));
                return null;
            }
        });

        assertEquals(1, reader.getRelationshipCount(gen("test#OUTGOING#key1#value3"), root));
    }

    @Test
    public void incrementingCountByFiveOnNonExistingCachedRelationshipShouldMakeItFive() {
        setUpBasicRelationshipCounts();

        final Node root = database.getNodeById(0);

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                assertTrue(cache.incrementCount(gen("test#OUTGOING#key1#value3"), root, 5));
                return null;
            }
        });

        assertEquals(5, reader.getRelationshipCount(gen("test#OUTGOING#key1#value3"), root));
    }

    @Test
    public void incrementingCountOnExistingCachedRelationshipShouldMakeItPlusOne() {
        setUpBasicRelationshipCounts();

        final Node root = database.getNodeById(0);

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                assertFalse(cache.incrementCount(gen("test#OUTGOING#key1#value2"), root, 1));
                return null;
            }
        });

        assertEquals(4, reader.getRelationshipCount(gen("test#OUTGOING#key1#value2"), root));
    }

    @Test
    public void incrementingCountByFiveOnExistingCachedRelationshipShouldMakeItPlusFive() {
        setUpBasicRelationshipCounts();

        final Node root = database.getNodeById(0);

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                assertFalse(cache.incrementCount(gen("test#OUTGOING#key1#value1"), root, 5));
                return null;
            }
        });

        assertEquals(7, reader.getRelationshipCount(gen("test#OUTGOING#key1#value1"), root));
    }

    @Test
    public void incrementingShouldNotAffectMoreGeneralRelationships() {
        setUpRelationshipCounts();

        final Node root = database.getNodeById(0);

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                assertFalse(cache.incrementCount(gen("test#OUTGOING#key1#value1"), root, 5));
                return null;
            }
        });

        assertEquals(12, (int) cache.getRelationshipCounts(root).get(gen("test#OUTGOING#key1#value1")));
        assertEquals(13, (int) cache.getRelationshipCounts(root).get(gen("test#OUTGOING")));
    }

    @Test
    public void decrementingCountOnNonExistingCachedRelationshipShouldNotChangeAnything() {
        setUpBasicRelationshipCounts();

        final Node root = database.getNodeById(0);

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                assertFalse(cache.decrementCount(gen("test#OUTGOING#key1#value3"), root, 1));
                return null;
            }
        });

        assertEquals(2, reader.getRelationshipCount(gen("test#OUTGOING#key1#value1"), root));
        assertEquals(3, reader.getRelationshipCount(gen("test#OUTGOING#key1#value2"), root));
        assertEquals(4, reader.getRelationshipCount(gen("test#OUTGOING#key2#value2"), root));
    }

    @Test
    public void decrementingCountByTwoOnNonExistingCachedRelationshipShouldNotChangeAnything() {
        setUpBasicRelationshipCounts();

        final Node root = database.getNodeById(0);

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                assertFalse(cache.decrementCount(gen("test#OUTGOING#key1#value3"), root, 2));
                return null;
            }
        });

        assertEquals(2, reader.getRelationshipCount(gen("test#OUTGOING#key1#value1"), root));
        assertEquals(3, reader.getRelationshipCount(gen("test#OUTGOING#key1#value2"), root));
        assertEquals(4, reader.getRelationshipCount(gen("test#OUTGOING#key2#value2"), root));
    }

    @Test
    public void decrementingCountOnExistingCachedRelationshipShouldMakeItMinusOne() {
        setUpBasicRelationshipCounts();

        final Node root = database.getNodeById(0);

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                assertTrue(cache.decrementCount(gen("test#OUTGOING#key1#value2"), root, 1));
                return null;
            }
        });

        assertEquals(2, reader.getRelationshipCount(gen("test#OUTGOING#key1#value2"), root));
    }

    @Test
    public void decrementingCountByTwoOnExistingCachedRelationshipShouldMakeItMinusTwo() {
        setUpBasicRelationshipCounts();

        final Node root = database.getNodeById(0);

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                assertTrue(cache.decrementCount(gen("test#OUTGOING#key2#value2"), root, 2));
                return null;
            }
        });

        assertEquals(2, reader.getRelationshipCount(gen("test#OUTGOING#key2#value2"), root));
    }

    @Test
    public void decrementingShouldNotAffectMoreGeneralRelationships() {
        setUpRelationshipCounts();

        final Node root = database.getNodeById(0);

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                assertTrue(cache.decrementCount(gen("test#OUTGOING#key1#value1"), root, 5));
                return null;
            }
        });

        assertEquals(2, (int) cache.getRelationshipCounts(root).get(gen("test#OUTGOING#key1#value1")));
        assertEquals(13, (int) cache.getRelationshipCounts(root).get(gen("test#OUTGOING")));
    }

    @Test
    public void decrementingCountByTooMuchShouldBeIndicated() {
        setUpRelationshipCounts();

        final Node root = database.getNodeById(0);

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                assertFalse(cache.decrementCount(gen("test#OUTGOING#key1#value1"), root, 8));
                return null;
            }
        });

        assertFalse(cache.getRelationshipCounts(root).containsKey(gen("test#OUTGOING#key1#value1")));
        assertEquals(8, reader.getRelationshipCount(gen("test#OUTGOING#key1#value1"), root));
        assertEquals(13, (int) cache.getRelationshipCounts(root).get(gen("test#OUTGOING")));
    }

    @Test
    public void decrementingCountByToZeroShouldDeleteCachedCount() {
        setUpRelationshipCounts();

        final Node root = database.getNodeById(0);

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                assertTrue(cache.decrementCount(gen("test#OUTGOING#key1#value1"), root, 7));
                return null;
            }
        });

        assertFalse(cache.getRelationshipCounts(root).containsKey(gen("test#OUTGOING#key1#value1")));
        assertEquals(8, reader.getRelationshipCount(gen("test#OUTGOING#key1#value1"), root));
        assertEquals(13, (int) cache.getRelationshipCounts(root).get(gen("test#OUTGOING")));
    }

    @Test
    public void shouldProperlyDeleteCounts() {
        setUpRelationshipCounts();

        final Node root = database.getNodeById(0);

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                cache.deleteCount(gen("test#OUTGOING"), root);
                return null;
            }
        });

        assertEquals(26, reader.getRelationshipCount(gen("test#OUTGOING"), root));
        assertFalse(cache.getRelationshipCounts(root).containsKey(gen("test#OUTGOING")));

        assertEquals(11, reader.getRelationshipCount(gen("test#OUTGOING#key1#value2"), root));
        assertEquals(15, reader.getRelationshipCount(gen("test#OUTGOING#key1#value1"), root));
        assertEquals(3, reader.getRelationshipCount(gen("test#OUTGOING#key1#value1#key2#value2"), root));

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                cache.deleteCount(gen("test#OUTGOING#key1#value2"), root);
                return null;
            }
        });

        assertFalse(cache.getRelationshipCounts(root).containsKey(gen("test#OUTGOING")));
        assertFalse(cache.getRelationshipCounts(root).containsKey(gen("test#OUTGOING#key1#value2")));

        assertEquals(15, reader.getRelationshipCount(gen("test#OUTGOING#key1#value1"), root));
        assertEquals(3, reader.getRelationshipCount(gen("test#OUTGOING#key1#value1#key2#value2"), root));
    }

    private void setUpBasicRelationshipCounts() {
        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                Node root = database.getNodeById(0);
                root.setProperty(lit("test#OUTGOING#key1#value1").toString(), 2);
                root.setProperty(lit("test#OUTGOING#key1#value2").toString(), 3);
                root.setProperty(lit("test#OUTGOING#key2#value2").toString(), 4);
                return null;
            }
        });
    }

    private void setUpRelationshipCounts() {
        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                Node root = database.getNodeById(0);
                root.setProperty(gen("test#OUTGOING#key1#value1#key2#value2").toString(), 3);
                root.setProperty(gen("test#OUTGOING#key1#value1#key2#value3").toString(), 5);
                root.setProperty(gen("test#OUTGOING#key1#value1").toString(), 7);
                root.setProperty(gen("test#OUTGOING#key1#value2").toString(), 11);
                root.setProperty(gen("test#OUTGOING").toString(), 13);
                return null;
            }
        });
    }

    /**
     * just for readability
     */
    private RelationshipDescription gen(String s) {
        return new GeneralRelationshipDescription(s, com.graphaware.neo4j.common.Constants.GA_PREFIX + Constants.FULL_RELCOUNT_DEFAULT_ID);
    }

    private RelationshipDescription lit(String s) {
        return new LiteralRelationshipDescription(s, com.graphaware.neo4j.common.Constants.GA_PREFIX + Constants.FULL_RELCOUNT_DEFAULT_ID);
    }

}
