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

package com.graphaware.relcount.full.internal.node;

import com.graphaware.framework.NeedsInitializationException;
import com.graphaware.framework.config.DefaultFrameworkConfiguration;
import com.graphaware.relcount.common.counter.UnableToCountException;
import com.graphaware.relcount.common.internal.node.RelationshipCountCachingNode;
import com.graphaware.relcount.common.internal.node.RelationshipCountingNode;
import com.graphaware.relcount.full.internal.cache.ThresholdBasedRelationshipCountCompactor;
import com.graphaware.relcount.full.internal.dto.property.CompactiblePropertiesImpl;
import com.graphaware.relcount.full.internal.dto.relationship.*;
import com.graphaware.relcount.full.module.FullRelationshipCountModule;
import com.graphaware.tx.executor.single.SimpleTransactionExecutor;
import com.graphaware.tx.executor.single.TransactionExecutor;
import com.graphaware.tx.executor.single.VoidReturningCallback;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Counting-related tests for {@link com.graphaware.relcount.full.internal.cache.FullRelationshipCountCache}.
 */
public class FullRelationshipCountCachingNodeTest {

    private GraphDatabaseService database;
    private TransactionExecutor txExecutor;

    @Before
    public void setUp() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();
        txExecutor = new SimpleTransactionExecutor(database);
    }

    @After
    public void tearDown() {
        database.shutdown();
    }

    private RelationshipCountingNode<RelationshipDescription> countingNode() {
        return new FullCachedRelationshipCountingNode(database.getNodeById(0), DefaultFrameworkConfiguration.getInstance().createPrefix(FullRelationshipCountModule.FULL_RELCOUNT_DEFAULT_ID), DefaultFrameworkConfiguration.getInstance().separator());
    }

    private RelationshipCountCachingNode<CompactibleRelationship> cachingNode() {
        return new FullRelationshipCountCachingNode(database.getNodeById(0), DefaultFrameworkConfiguration.getInstance().createPrefix(FullRelationshipCountModule.FULL_RELCOUNT_DEFAULT_ID), DefaultFrameworkConfiguration.getInstance().separator(), new ThresholdBasedRelationshipCountCompactor(10));
    }


    @Test
    public void shouldCorrectlyReportBasicRelationshipCounts() {
        setUpBasicRelationshipCounts();

        Assert.assertEquals(2, countingNode().getRelationshipCount(wildcard("test#OUTGOING#key1#value1")));
        Assert.assertEquals(2, countingNode().getRelationshipCount(literal("test#OUTGOING#key1#value1")));
        Assert.assertEquals(3, countingNode().getRelationshipCount(wildcard("test#OUTGOING#key1#value2")));
        Assert.assertEquals(3, countingNode().getRelationshipCount(literal("test#OUTGOING#key1#value2")));
        Assert.assertEquals(4, countingNode().getRelationshipCount(wildcard("test#OUTGOING#key2#value2")));
        Assert.assertEquals(4, countingNode().getRelationshipCount(literal("test#OUTGOING#key2#value2")));
        Assert.assertEquals(0, countingNode().getRelationshipCount(wildcard("test#OUTGOING#key1#value3")));
        Assert.assertEquals(0, countingNode().getRelationshipCount(literal("test#OUTGOING#key1#value3")));
        Assert.assertEquals(0, countingNode().getRelationshipCount(wildcard("test#INCOMING#key1#value1")));
        Assert.assertEquals(0, countingNode().getRelationshipCount(literal("test#INCOMING#key1#value1")));
    }

    @Test
    public void shouldCorrectlyReportAggregatedCounts() {
        setUpBasicRelationshipCounts();

        Assert.assertEquals(9, countingNode().getRelationshipCount(wildcard("test#OUTGOING")));
        Assert.assertEquals(0, countingNode().getRelationshipCount(literal("test#OUTGOING")));
        Assert.assertEquals(0, countingNode().getRelationshipCount(wildcard("wrong#OUTGOING")));
        Assert.assertEquals(0, countingNode().getRelationshipCount(literal("wrong#OUTGOING")));
        Assert.assertEquals(0, countingNode().getRelationshipCount(wildcard("test#INCOMING")));
        Assert.assertEquals(0, countingNode().getRelationshipCount(literal("test#INCOMING")));
    }

    @Test
    public void shouldCorrectlyReportAggregatedCountsWhenGeneralityIsMixed() {
        setUpRelationshipCounts();

        Assert.assertEquals(39, countingNode().getRelationshipCount(wildcard("test#OUTGOING")));
        Assert.assertEquals(11, countingNode().getRelationshipCount(wildcard("test#OUTGOING#key1#value2")));
        Assert.assertEquals(11, countingNode().getRelationshipCount(literal("test#OUTGOING#key1#value2")));
        Assert.assertEquals(15, countingNode().getRelationshipCount(wildcard("test#OUTGOING#key1#value1")));
        Assert.assertEquals(7, countingNode().getRelationshipCount(literal("test#OUTGOING#key1#value1")));
        Assert.assertEquals(3, countingNode().getRelationshipCount(wildcard("test#OUTGOING#key1#value1#key2#value2")));
        Assert.assertEquals(3, countingNode().getRelationshipCount(literal("test#OUTGOING#key1#value1#key2#value2")));
        Assert.assertEquals(20, countingNode().getRelationshipCount(wildcard("test2#OUTGOING#key3#" + CompactiblePropertiesImpl.ANY_VALUE))); //this includes undefined => potential idea for improvement, split ANY_VALUE into ANY_VALUE_INCLUDING_UNDEF and ANY_CONCRETE_VALUE
        Assert.assertEquals(20, countingNode().getRelationshipCount(literal("test2#OUTGOING#key3#" + CompactiblePropertiesImpl.ANY_VALUE)));

        try {
            countingNode().getRelationshipCount(wildcard("test2#OUTGOING#key3#anything"));
            fail();
        } catch (UnableToCountException e) {
            //OK
        }

        try {
            countingNode().getRelationshipCount(literal("test2#OUTGOING#key3#anything"));
            fail();
        } catch (UnableToCountException e) {
            //OK
        }

        try {
            countingNode().getRelationshipCount(literal("test2#OUTGOING"));
            fail();
        } catch (UnableToCountException e) {
            //OK
        }
    }

    @Test
    public void shouldNotAggregateCountsWhenAskedForAll() {
        setUpRelationshipCounts();

        Map<CompactibleRelationship, Integer> relationshipCounts = cachingNode().getCachedCounts();
        Iterator<Map.Entry<CompactibleRelationship, Integer>> iterator = relationshipCounts.entrySet().iterator();

        //also testing order

        Map.Entry<CompactibleRelationship, Integer> next = iterator.next();
        Assert.assertEquals(compactible("test#OUTGOING"), next.getKey());
        assertEquals(13, (int) next.getValue());

        next = iterator.next();
        Assert.assertEquals(compactible("test#OUTGOING#key1#value1"), next.getKey());
        assertEquals(7, (int) next.getValue());

        next = iterator.next();
        Assert.assertEquals(compactible("test#OUTGOING#key1#value2"), next.getKey());
        assertEquals(11, (int) next.getValue());

        next = iterator.next();
        Assert.assertEquals(compactible("test#OUTGOING#key1#value1#key2#value2"), next.getKey());
        assertEquals(3, (int) next.getValue());

        next = iterator.next();
        Assert.assertEquals(compactible("test#OUTGOING#key1#value1#key2#value3"), next.getKey());
        assertEquals(5, (int) next.getValue());

        next = iterator.next();
        Assert.assertEquals(compactible("test2#OUTGOING#key3#" + CompactiblePropertiesImpl.ANY_VALUE), next.getKey());
        assertEquals(20, (int) next.getValue());

        assertEquals(6, relationshipCounts.size());
    }

    @Test
    public void incrementingCountOnNonExistingCachedRelationshipShouldMakeItOne() {
        setUpBasicRelationshipCounts();

        txExecutor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                cachingNode().incrementCount(compactible("test#OUTGOING#key1#value3"), 1);
            }
        });

        Assert.assertEquals(1, countingNode().getRelationshipCount(wildcard("test#OUTGOING#key1#value3")));
    }

    @Test
    public void incrementingCountByFiveOnNonExistingCachedRelationshipShouldMakeItFive() {
        setUpBasicRelationshipCounts();

        txExecutor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                cachingNode().incrementCount(compactible("test#OUTGOING#key1#value3"), 5);
            }
        });

        Assert.assertEquals(5, countingNode().getRelationshipCount(wildcard("test#OUTGOING#key1#value3")));
    }

    @Test
    public void incrementingCountOnExistingCachedRelationshipShouldMakeItPlusOne() {
        setUpBasicRelationshipCounts();

        txExecutor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                cachingNode().incrementCount(compactible("test#OUTGOING#key1#value2"), 1);
            }
        });

        Assert.assertEquals(4, countingNode().getRelationshipCount(wildcard("test#OUTGOING#key1#value2")));
    }

    @Test
    public void incrementingCountByFiveOnExistingCachedRelationshipShouldMakeItPlusFive() {
        setUpBasicRelationshipCounts();

        txExecutor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                cachingNode().incrementCount(compactible("test#OUTGOING#key1#value1"), 5);
            }
        });

        Assert.assertEquals(7, countingNode().getRelationshipCount(wildcard("test#OUTGOING#key1#value1")));
    }

    @Test
    public void incrementingShouldNotAffectMoreGeneralRelationships() {
        setUpRelationshipCounts();

        txExecutor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                cachingNode().incrementCount(compactible("test#OUTGOING#key1#value1"), 5);
            }
        });

        assertEquals(12, (int) cachingNode().getCachedCounts().get(compactible("test#OUTGOING#key1#value1")));
        assertEquals(13, (int) cachingNode().getCachedCounts().get(compactible("test#OUTGOING")));
    }

    @Test
    public void decrementingCountOnNonExistingCachedRelationshipShouldThrowException() {
        setUpBasicRelationshipCounts();

        txExecutor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                try {
                    cachingNode().decrementCount(compactible("test#OUTGOING#key1#value3"), 1);
                    fail();
                } catch (NeedsInitializationException e) {
                    //ok
                }
            }
        });
    }

    @Test
    public void decrementingCountByTwoOnNonExistingCachedRelationshipShouldThrowException() {
        setUpBasicRelationshipCounts();

        txExecutor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                try {
                    cachingNode().decrementCount(compactible("test#OUTGOING#key1#value3"), 2);
                    fail();
                } catch (NeedsInitializationException e) {
                    //OK
                }
            }
        });
    }

    @Test
    public void decrementingCountOnExistingCachedRelationshipShouldMakeItMinusOne() {
        setUpBasicRelationshipCounts();

        txExecutor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                cachingNode().decrementCount(compactible("test#OUTGOING#key1#value2"), 1);
            }
        });

        Assert.assertEquals(2, countingNode().getRelationshipCount(wildcard("test#OUTGOING#key1#value2")));
    }

    @Test
    public void decrementingCountByTwoOnExistingCachedRelationshipShouldMakeItMinusTwo() {
        setUpBasicRelationshipCounts();

        txExecutor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                cachingNode().decrementCount(compactible("test#OUTGOING#key2#value2"), 2);
            }
        });

        Assert.assertEquals(2, countingNode().getRelationshipCount(wildcard("test#OUTGOING#key2#value2")));
    }

    @Test
    public void decrementingShouldNotAffectMoreGeneralRelationships() {
        setUpRelationshipCounts();

        txExecutor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                cachingNode().decrementCount(compactible("test#OUTGOING#key1#value1"), 5);
            }
        });

        assertEquals(2, (int) cachingNode().getCachedCounts().get(compactible("test#OUTGOING#key1#value1")));
        assertEquals(13, (int) cachingNode().getCachedCounts().get(compactible("test#OUTGOING")));
    }

    @Test
    public void decrementingCountByTooMuchShouldBeIndicated() {
        setUpRelationshipCounts();

        txExecutor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                try {
                    cachingNode().decrementCount(compactible("test#OUTGOING#key1#value1"), 8);
                    fail();
                } catch (NeedsInitializationException e) {
                    //OK
                }
            }
        });
    }

    @Test
    public void decrementingCountByToZeroShouldDeleteCachedCount() {
        setUpRelationshipCounts();

        txExecutor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                cachingNode().decrementCount(compactible("test#OUTGOING#key1#value1"), 7);
            }
        });

        assertFalse(cachingNode().getCachedCounts().containsKey(compactible("test#OUTGOING#key1#value1")));
        Assert.assertEquals(8, countingNode().getRelationshipCount(wildcard("test#OUTGOING#key1#value1")));
        assertEquals(13, (int) cachingNode().getCachedCounts().get(compactible("test#OUTGOING")));
    }

    @Test
    public void shouldProperlyDeleteCounts() {
        setUpRelationshipCounts();

        txExecutor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                cachingNode().deleteCount(compactible("test#OUTGOING"));
            }
        });

        Assert.assertEquals(26, countingNode().getRelationshipCount(wildcard("test#OUTGOING")));
        assertFalse(cachingNode().getCachedCounts().containsKey(compactible("test#OUTGOING")));

        Assert.assertEquals(11, countingNode().getRelationshipCount(wildcard("test#OUTGOING#key1#value2")));
        Assert.assertEquals(15, countingNode().getRelationshipCount(wildcard("test#OUTGOING#key1#value1")));
        Assert.assertEquals(3, countingNode().getRelationshipCount(wildcard("test#OUTGOING#key1#value1#key2#value2")));

        txExecutor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                cachingNode().deleteCount(compactible("test#OUTGOING#key1#value2"));
            }
        });

        assertFalse(cachingNode().getCachedCounts().containsKey(compactible("test#OUTGOING")));
        assertFalse(cachingNode().getCachedCounts().containsKey(compactible("test#OUTGOING#key1#value2")));

        Assert.assertEquals(15, countingNode().getRelationshipCount(wildcard("test#OUTGOING#key1#value1")));
        Assert.assertEquals(3, countingNode().getRelationshipCount(wildcard("test#OUTGOING#key1#value1#key2#value2")));
    }

    private void setUpBasicRelationshipCounts() {
        txExecutor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                Node root = database.getNodeById(0);
                root.setProperty(compactible("test#OUTGOING#key1#value1").toString(prefix(), hash()), 2);
                root.setProperty(compactible("test#OUTGOING#key1#value2").toString(prefix(), hash()), 3);
                root.setProperty(compactible("test#OUTGOING#key2#value2").toString(prefix(), hash()), 4);
            }
        });
    }

    private void setUpRelationshipCounts() {
        txExecutor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                Node root = database.getNodeById(0);
                root.setProperty(compactible("test#OUTGOING#key1#value1#key2#value2").toString(prefix(), hash()), 3);
                root.setProperty(compactible("test#OUTGOING#key1#value1#key2#value3").toString(prefix(), hash()), 5);
                root.setProperty(compactible("test#OUTGOING#key1#value1").toString(prefix(), hash()), 7);
                root.setProperty(compactible("test#OUTGOING#key1#value2").toString(prefix(), hash()), 11);
                root.setProperty(compactible("test#OUTGOING").toString(prefix(), hash()), 13);
                root.setProperty(compactible("test2#OUTGOING#key3#" + CompactiblePropertiesImpl.ANY_VALUE).toString(prefix(), hash()), 20);
            }
        });
    }

    private String prefix() {
        return DefaultFrameworkConfiguration.getInstance().createPrefix(FullRelationshipCountModule.FULL_RELCOUNT_DEFAULT_ID);
    }

    private String hash() {
        return "#";
    }

    private CompactibleRelationship compactible(String s) {
        return new CompactibleRelationshipImpl(prefix() + s, prefix(), hash());
    }

    private RelationshipDescription wildcard(String s) {
        return new WildcardRelationshipDescription(s, null, hash());
    }

    private RelationshipDescription literal(String s) {
        return new LiteralRelationshipDescription(s, null, hash());
    }

}
