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

import com.graphaware.neo4j.relcount.representation.ComparableRelationship;
import com.graphaware.neo4j.utils.tx.single.SimpleTransactionExecutor;
import com.graphaware.neo4j.utils.tx.single.TransactionCallback;
import com.graphaware.neo4j.utils.tx.single.TransactionExecutor;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Iterator;
import java.util.Map;

import static com.graphaware.neo4j.utils.Constants.GA_REL_PREFIX;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link com.graphaware.neo4j.relcount.logic.RelationshipCountManagerImpl}.
 */
public class RelationshipCountManagerImplTest {

    private RelationshipCountManager mgr;
    private GraphDatabaseService database;
    private TransactionExecutor txExecutor;

    @Before
    public void setUp() {
        mgr = new RelationshipCountManagerImpl();
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();
        txExecutor = new SimpleTransactionExecutor(database);
    }

//    @Test
//    public void shouldIgnoreInternalGraphAwareRelationships() {
//        Relationship r1 = mock(Relationship.class);
//        Relationship r2 = mock(Relationship.class);
//        when(r1.getType()).thenReturn(withName(GA_REL_PREFIX + "test"));
//        when(r2.getType()).thenReturn(withName("test"));
//
//        assertTrue(mgr.isIgnored(r1));
//        assertFalse(mgr.isIgnored(r2));
//    }

    @Test
    public void shouldCorrectlyReportBasicRelationshipCounts() {
        setUpBasicRelationshipCounts();

        final Node root = database.getNodeById(0);

        assertEquals(2, mgr.getRelationshipCount(rel("test#OUTGOING#key1#value1"), root));
        assertEquals(3, mgr.getRelationshipCount(rel("test#OUTGOING#key1#value2"), root));
        assertEquals(4, mgr.getRelationshipCount(rel("test#OUTGOING#key2#value2"), root));
        assertEquals(0, mgr.getRelationshipCount(rel("test#OUTGOING#key1#value3"), root));
        assertEquals(0, mgr.getRelationshipCount(rel("test#INCOMING#key1#value1"), root));
    }

    @Test
    public void shouldCorrectlyReportAggregatedCounts() {
        setUpBasicRelationshipCounts();

        final Node root = database.getNodeById(0);

        assertEquals(9, mgr.getRelationshipCount(rel("test#OUTGOING"), root));
        assertEquals(0, mgr.getRelationshipCount(rel("wrong#OUTGOING"), root));
        assertEquals(0, mgr.getRelationshipCount(rel("test#INCOMING"), root));
    }

    @Test
    public void shouldCorrectlyReportAggregatedCountsWhenGeneralityIsMixed() {
        setUpRelationshipCounts();

        final Node root = database.getNodeById(0);

        assertEquals(39, mgr.getRelationshipCount(rel("test#OUTGOING"), root));
        assertEquals(11, mgr.getRelationshipCount(rel("test#OUTGOING#key1#value2"), root));
        assertEquals(15, mgr.getRelationshipCount(rel("test#OUTGOING#key1#value1"), root));
        assertEquals(3, mgr.getRelationshipCount(rel("test#OUTGOING#key1#value1#key2#value2"), root));
    }

    @Test
    public void shouldNotAggregateCountsWhenAskedForAll() {
        setUpRelationshipCounts();

        final Node root = database.getNodeById(0);

        Map<ComparableRelationship,Integer> relationshipCounts = mgr.getRelationshipCounts(root);
        Iterator<Map.Entry<ComparableRelationship,Integer>> iterator = relationshipCounts.entrySet().iterator();

        //also testing order

        Map.Entry<ComparableRelationship, Integer> next = iterator.next();
        assertEquals(rel("test#OUTGOING#key1#value1#key2#value2"), next.getKey());
        assertEquals(3, (int) next.getValue());

        next = iterator.next();
        assertEquals(rel("test#OUTGOING#key1#value1#key2#value3"), next.getKey());
        assertEquals(5, (int) next.getValue());

        next = iterator.next();
        assertEquals(rel("test#OUTGOING#key1#value1"), next.getKey());
        assertEquals(7, (int) next.getValue());

        next = iterator.next();
        assertEquals(rel("test#OUTGOING#key1#value2"), next.getKey());
        assertEquals(11, (int) next.getValue());

        next = iterator.next();
        assertEquals(rel("test#OUTGOING"), next.getKey());
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
                assertTrue(mgr.incrementCount(rel("test#OUTGOING#key1#value3"), root));
                return null;
            }
        });

        assertEquals(1, mgr.getRelationshipCount(rel("test#OUTGOING#key1#value3"), root));
    }

    @Test
    public void incrementingCountByFiveOnNonExistingCachedRelationshipShouldMakeItFive() {
        setUpBasicRelationshipCounts();

        final Node root = database.getNodeById(0);

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                assertTrue(mgr.incrementCount(rel("test#OUTGOING#key1#value3"), root, 5));
                return null;
            }
        });

        assertEquals(5, mgr.getRelationshipCount(rel("test#OUTGOING#key1#value3"), root));
    }

    @Test
    public void incrementingCountOnExistingCachedRelationshipShouldMakeItPlusOne() {
        setUpBasicRelationshipCounts();

        final Node root = database.getNodeById(0);

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                assertFalse(mgr.incrementCount(rel("test#OUTGOING#key1#value2"), root));
                return null;
            }
        });

        assertEquals(4, mgr.getRelationshipCount(rel("test#OUTGOING#key1#value2"), root));
    }

    @Test
    public void incrementingCountByFiveOnExistingCachedRelationshipShouldMakeItPlusFive() {
        setUpBasicRelationshipCounts();

        final Node root = database.getNodeById(0);

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                assertFalse(mgr.incrementCount(rel("test#OUTGOING#key1#value1"), root, 5));
                return null;
            }
        });

        assertEquals(7, mgr.getRelationshipCount(rel("test#OUTGOING#key1#value1"), root));
    }

    @Test
    public void incrementingShouldNotAffectMoreGeneralRelationships() {
        setUpRelationshipCounts();

        final Node root = database.getNodeById(0);

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                assertFalse(mgr.incrementCount(rel("test#OUTGOING#key1#value1"), root, 5));
                return null;
            }
        });

        assertEquals(12, (int) mgr.getRelationshipCounts(root).get(rel("test#OUTGOING#key1#value1")));
        assertEquals(13, (int) mgr.getRelationshipCounts(root).get(rel("test#OUTGOING")));
    }

    @Test
    public void decrementingCountOnNonExistingCachedRelationshipShouldNotChangeAnything() {
        setUpBasicRelationshipCounts();

        final Node root = database.getNodeById(0);

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                assertFalse(mgr.decrementCount(rel("test#OUTGOING#key1#value3"), root));
                return null;
            }
        });

        assertEquals(2, mgr.getRelationshipCount(rel("test#OUTGOING#key1#value1"), root));
        assertEquals(3, mgr.getRelationshipCount(rel("test#OUTGOING#key1#value2"), root));
        assertEquals(4, mgr.getRelationshipCount(rel("test#OUTGOING#key2#value2"), root));
    }

    @Test
    public void decrementingCountByTwoOnNonExistingCachedRelationshipShouldNotChangeAnything() {
        setUpBasicRelationshipCounts();

        final Node root = database.getNodeById(0);

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                assertFalse(mgr.decrementCount(rel("test#OUTGOING#key1#value3"), root, 2));
                return null;
            }
        });

        assertEquals(2, mgr.getRelationshipCount(rel("test#OUTGOING#key1#value1"), root));
        assertEquals(3, mgr.getRelationshipCount(rel("test#OUTGOING#key1#value2"), root));
        assertEquals(4, mgr.getRelationshipCount(rel("test#OUTGOING#key2#value2"), root));
    }

    @Test
    public void decrementingCountOnExistingCachedRelationshipShouldMakeItMinusOne() {
        setUpBasicRelationshipCounts();

        final Node root = database.getNodeById(0);

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                assertTrue(mgr.decrementCount(rel("test#OUTGOING#key1#value2"), root));
                return null;
            }
        });

        assertEquals(2, mgr.getRelationshipCount(rel("test#OUTGOING#key1#value2"), root));
    }

    @Test
    public void decrementingCountByTwoOnExistingCachedRelationshipShouldMakeItMinusTwo() {
        setUpBasicRelationshipCounts();

        final Node root = database.getNodeById(0);

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                assertTrue(mgr.decrementCount(rel("test#OUTGOING#key2#value2"), root, 2));
                return null;
            }
        });

        assertEquals(2, mgr.getRelationshipCount(rel("test#OUTGOING#key2#value2"), root));
    }

    @Test
    public void decrementingShouldNotAffectMoreGeneralRelationships() {
        setUpRelationshipCounts();

        final Node root = database.getNodeById(0);

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                assertTrue(mgr.decrementCount(rel("test#OUTGOING#key1#value1"), root, 5));
                return null;
            }
        });

        assertEquals(2, (int) mgr.getRelationshipCounts(root).get(rel("test#OUTGOING#key1#value1")));
        assertEquals(13, (int) mgr.getRelationshipCounts(root).get(rel("test#OUTGOING")));
    }

    @Test
    public void decrementingCountByTooMuchShouldBeIndicated() {
        setUpRelationshipCounts();

        final Node root = database.getNodeById(0);

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                assertFalse(mgr.decrementCount(rel("test#OUTGOING#key1#value1"), root, 8));
                return null;
            }
        });

        assertFalse(mgr.getRelationshipCounts(root).containsKey(rel("test#OUTGOING#key1#value1")));
        assertEquals(8, mgr.getRelationshipCount(rel("test#OUTGOING#key1#value1"), root));
        assertEquals(13, (int) mgr.getRelationshipCounts(root).get(rel("test#OUTGOING")));
    }

    @Test
    public void decrementingCountByToZeroShouldDeleteCachedCount() {
        setUpRelationshipCounts();

        final Node root = database.getNodeById(0);

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                assertTrue(mgr.decrementCount(rel("test#OUTGOING#key1#value1"), root, 7));
                return null;
            }
        });

        assertFalse(mgr.getRelationshipCounts(root).containsKey(rel("test#OUTGOING#key1#value1")));
        assertEquals(8, mgr.getRelationshipCount(rel("test#OUTGOING#key1#value1"), root));
        assertEquals(13, (int) mgr.getRelationshipCounts(root).get(rel("test#OUTGOING")));
    }

    @Test
    public void shouldProperlyDeleteCounts() {
        setUpRelationshipCounts();

        final Node root = database.getNodeById(0);

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                mgr.deleteCount(rel("test#OUTGOING"), root);
                return null;
            }
        });

        assertEquals(26, mgr.getRelationshipCount(rel("test#OUTGOING"), root));
        assertFalse(mgr.getRelationshipCounts(root).containsKey(rel("test#OUTGOING")));

        assertEquals(11, mgr.getRelationshipCount(rel("test#OUTGOING#key1#value2"), root));
        assertEquals(15, mgr.getRelationshipCount(rel("test#OUTGOING#key1#value1"), root));
        assertEquals(3, mgr.getRelationshipCount(rel("test#OUTGOING#key1#value1#key2#value2"), root));

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                mgr.deleteCount(rel("test#OUTGOING#key1#value2"), root);
                return null;
            }
        });

        assertFalse(mgr.getRelationshipCounts(root).containsKey(rel("test#OUTGOING")));
        assertFalse(mgr.getRelationshipCounts(root).containsKey(rel("test#OUTGOING#key1#value2")));

        assertEquals(15, mgr.getRelationshipCount(rel("test#OUTGOING#key1#value1"), root));
        assertEquals(3, mgr.getRelationshipCount(rel("test#OUTGOING#key1#value1#key2#value2"), root));
    }

    private void setUpBasicRelationshipCounts() {
        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                Node root = database.getNodeById(0);
                root.setProperty(rel("test#OUTGOING#key1#value1").toString(), 2);
                root.setProperty(rel("test#OUTGOING#key1#value2").toString(), 3);
                root.setProperty(rel("test#OUTGOING#key2#value2").toString(), 4);
                return null;
            }
        });
    }

    private void setUpRelationshipCounts() {
        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                Node root = database.getNodeById(0);
                root.setProperty(rel("test#OUTGOING#key1#value1#key2#value2").toString(), 3);
                root.setProperty(rel("test#OUTGOING#key1#value1#key2#value3").toString(), 5);
                root.setProperty(rel("test#OUTGOING#key1#value1").toString(), 7);
                root.setProperty(rel("test#OUTGOING#key1#value2").toString(), 11);
                root.setProperty(rel("test#OUTGOING").toString(), 13);
                return null;
            }
        });
    }

    /**
     * just for readability
     */
    private ComparableRelationship rel(String s) {
        return new ComparableRelationship(GA_REL_PREFIX + s);
    }

}
