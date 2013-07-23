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
import com.graphaware.neo4j.relcount.common.api.UnableToCountException;
import com.graphaware.neo4j.relcount.full.api.FullCachedRelationshipCounter;
import com.graphaware.neo4j.relcount.full.dto.relationship.CompactibleRelationshipImpl;
import com.graphaware.neo4j.relcount.full.dto.relationship.LiteralRelationshipDescription;
import com.graphaware.neo4j.relcount.full.strategy.RelationshipCountStrategiesImpl;
import com.graphaware.neo4j.relcount.full.strategy.RelationshipPropertiesExtractionStrategy;
import com.graphaware.neo4j.tx.event.strategy.IncludeNoRelationships;
import com.graphaware.neo4j.tx.single.SimpleTransactionExecutor;
import com.graphaware.neo4j.tx.single.TransactionCallback;
import com.graphaware.neo4j.tx.single.TransactionExecutor;
import com.graphaware.neo4j.tx.single.VoidReturningCallback;
import com.graphaware.neo4j.utils.TestDataBuilder;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.graphaware.neo4j.framework.config.FrameworkConfiguration.GA_PREFIX;
import static com.graphaware.neo4j.utils.DeleteUtils.deleteNodeAndRelationships;
import static com.graphaware.neo4j.utils.PropertyContainerUtils.*;
import static java.lang.String.valueOf;
import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;

/**
 * Integration test for relationship counting.
 */
public class FullCachedMoreGeneralRelationshipCounterIntegrationTest {

    private GraphDatabaseService database;
    private TransactionExecutor txExecutor;

    @Before
    public void setUp() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();
        txExecutor = new SimpleTransactionExecutor(database);

        GraphAwareFramework framework = new GraphAwareFramework(database);
        framework.registerModule(new FullRelationshipCountModule(RelationshipCountStrategiesImpl.defaultStrategies().with(5)));
        framework.start();
    }

    @Test
    public void noRelationshipsShouldExistInEmptyDatabase() {
        assertEquals(0, new FullCachedRelationshipCounter(withName("test"), OUTGOING).count(database.getNodeById(0)));
        assertEquals(0, new FullCachedRelationshipCounter(withName("test"), OUTGOING).countLiterally(database.getNodeById(0)));
    }

    @Test
    public void noRelationshipsShouldExistInDatabaseWithNoRelationships() {
        createNodes();

        assertEquals(0, new FullCachedRelationshipCounter(withName("test"), OUTGOING).count(database.getNodeById(0)));
        assertEquals(0, new FullCachedRelationshipCounter(withName("test"), OUTGOING).countLiterally(database.getNodeById(0)));
    }

    @Test
    public void relationshipsBelowThresholdShouldBeCountedOneByOne() {
        createNodes();
        createFirstRelationships();

        assertEquals(1, new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value1").count(database.getNodeById(1)));
        assertEquals(2, new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value2").count(database.getNodeById(1)));
        assertEquals(0, new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value3").count(database.getNodeById(1)));
        assertEquals(4, new FullCachedRelationshipCounter(withName("test"), OUTGOING).count(database.getNodeById(1)));
        assertEquals(1, new FullCachedRelationshipCounter(withName("test"), INCOMING).count(database.getNodeById(1)));
        assertEquals(1, new FullCachedRelationshipCounter(withName("test"), INCOMING).with("key2", "value1").count(database.getNodeById(1)));
    }

    @Test
    public void relationshipsAboveThresholdShouldNotBeCountableOneByOne() {
        createNodes();
        createFirstRelationships();
        createSecondRelationships();

        try {
            new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value1").count(database.getNodeById(1));
            fail();
        } catch (UnableToCountException e) {
            //OK
        }

        try {
            new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value2").count(database.getNodeById(1));
            fail();
        } catch (UnableToCountException e) {
            //OK
        }

        try {
            new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value3").count(database.getNodeById(1));
            fail();
        } catch (UnableToCountException e) {
            //OK
        }

        assertEquals(8, new FullCachedRelationshipCounter(withName("test"), OUTGOING).count(database.getNodeById(1)));
        assertEquals(1, new FullCachedRelationshipCounter(withName("test"), INCOMING).count(database.getNodeById(1)));
    }

    @Test
    public void deletingRelationshipsShouldCorrectlyDecrementCounts1() {
        createNodes();
        createFirstRelationships();

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                for (Relationship relationship : database.getNodeById(1).getRelationships(withName("test"), OUTGOING)) {
                    if (relationship.getEndNode().getId() == 2) {
                        relationship.delete();
                        break;
                    }
                }
                return null;
            }
        });

        assertEquals(0, new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value1").count(database.getNodeById(1)));
        assertEquals(2, new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value2").count(database.getNodeById(1)));
        assertEquals(0, new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value3").count(database.getNodeById(1)));
        assertEquals(3, new FullCachedRelationshipCounter(withName("test"), OUTGOING).count(database.getNodeById(1)));
        assertEquals(1, new FullCachedRelationshipCounter(withName("test"), INCOMING).count(database.getNodeById(1)));
    }

    @Test
    public void deletingRelationshipsShouldCorrectlyDecrementCounts2() {
        createNodes();
        createFirstRelationships();
        createSecondRelationships();

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                for (Relationship relationship : database.getNodeById(1).getRelationships(withName("test"), OUTGOING)) {
                    if (relationship.getEndNode().getId() == 2) {
                        relationship.delete();
                        break;
                    }
                }
                return null;
            }
        });

        try {
            new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value1").count(database.getNodeById(1));
            fail();
        } catch (UnableToCountException e) {
            //OK
        }

        try {
            new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value2").count(database.getNodeById(1));
            fail();
        } catch (UnableToCountException e) {
            //OK
        }

        try {
            new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value3").count(database.getNodeById(1));
            fail();
        } catch (UnableToCountException e) {
            //OK
        }

        assertEquals(7, new FullCachedRelationshipCounter(withName("test"), OUTGOING).count(database.getNodeById(1)));
        assertEquals(1, new FullCachedRelationshipCounter(withName("test"), INCOMING).count(database.getNodeById(1)));
    }

    @Test
    public void deletingBelowZeroShouldNotDoAnyHarm() {
        createNodes();
        createFirstRelationships();

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                database.getNodeById(1).setProperty(new CompactibleRelationshipImpl(withName("test"), OUTGOING, MapUtil.stringMap("key1", "value1", "_LITERAL_", "true")).toString(), 0);
                return null;
            }
        });

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                for (Relationship relationship : database.getNodeById(1).getRelationships(withName("test"), OUTGOING)) {
                    if (relationship.getEndNode().getId() == 2) {
                        relationship.delete();
                        break;
                    }
                }
                return null;
            }
        });

        assertEquals(0, new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value1").count(database.getNodeById(1)));
        assertEquals(2, new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value2").count(database.getNodeById(1)));
        assertEquals(0, new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value3").count(database.getNodeById(1)));
        assertEquals(3, new FullCachedRelationshipCounter(withName("test"), OUTGOING).count(database.getNodeById(1)));
        assertEquals(1, new FullCachedRelationshipCounter(withName("test"), INCOMING).count(database.getNodeById(1)));
    }

    @Test
    public void deletingNonExistingShouldNotDoAnyHarm() {
        createNodes();
        createFirstRelationships();

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                database.getNodeById(2).createRelationshipTo(database.getNodeById(10), withName("test2")).setProperty("key1", "value3");
                return null;
            }
        });

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                database.getNodeById(2).removeProperty(new LiteralRelationshipDescription(withName("test2"), OUTGOING, Collections.singletonMap("key1", "value3")).toString());
                return null;
            }
        });

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                database.getNodeById(2).getSingleRelationship(withName("test2"), OUTGOING).delete();
                return null;
            }
        });

        assertEquals(1, new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value1").count(database.getNodeById(1)));
        assertEquals(2, new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value2").count(database.getNodeById(1)));
        assertEquals(0, new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value3").count(database.getNodeById(1)));
        assertEquals(4, new FullCachedRelationshipCounter(withName("test"), OUTGOING).count(database.getNodeById(1)));
        assertEquals(1, new FullCachedRelationshipCounter(withName("test"), INCOMING).count(database.getNodeById(1)));
        assertEquals(1, new FullCachedRelationshipCounter(withName("test"), INCOMING).with("key2", "value1").count(database.getNodeById(1)));
    }

    @Test
    public void deletingNodeWithAllRelationshipsWorkCorrectly() {
        createNodes();
        createFirstRelationships();
        createSecondRelationships();

        assertEquals(1, new FullCachedRelationshipCounter(withName("test"), INCOMING).count(database.getNodeById(2)));
        assertEquals(1, new FullCachedRelationshipCounter(withName("test"), INCOMING).count(database.getNodeById(3)));

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                deleteNodeAndRelationships(database.getNodeById(1));
                return null;
            }
        });

        assertEquals(0, new FullCachedRelationshipCounter(withName("test"), INCOMING).count(database.getNodeById(2)));
        assertEquals(0, new FullCachedRelationshipCounter(withName("test"), INCOMING).count(database.getNodeById(3)));
    }

    @Test
    public void changingRelationshipsWithNoActualChangeWorksCorrectly() {
        createNodes();
        createFirstRelationships();

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                for (Relationship relationship : database.getNodeById(1).getRelationships(withName("test"), OUTGOING)) {
                    if (relationship.getEndNode().getId() == 3) {
                        relationship.setProperty("key1", "value2");
                        break;
                    }
                }
                return null;
            }
        });

        assertEquals(1, new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value1").count(database.getNodeById(1)));
        assertEquals(2, new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value2").count(database.getNodeById(1)));
        assertEquals(0, new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value3").count(database.getNodeById(1)));
        assertEquals(4, new FullCachedRelationshipCounter(withName("test"), OUTGOING).count(database.getNodeById(1)));
        assertEquals(1, new FullCachedRelationshipCounter(withName("test"), INCOMING).count(database.getNodeById(1)));
    }

    @Test
    public void changingRelationshipsWorksWhenNotYetCompacted() {
        createNodes();
        createFirstRelationships();

        txExecutor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                for (Relationship relationship : database.getNodeById(1).getRelationships(withName("test"), OUTGOING)) {
                    if (relationship.getEndNode().getId() == 3) {
                        relationship.setProperty("key1", "value1");
                        break;
                    }
                }
            }
        });

        assertEquals(2, new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value1").count(database.getNodeById(1)));
        assertEquals(1, new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value2").count(database.getNodeById(1)));
        assertEquals(0, new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value3").count(database.getNodeById(1)));
        assertEquals(4, new FullCachedRelationshipCounter(withName("test"), OUTGOING).count(database.getNodeById(1)));
        assertEquals(1, new FullCachedRelationshipCounter(withName("test"), INCOMING).count(database.getNodeById(1)));
    }

    @Test
    public void changingRelationshipsWithNoActualChangeWorksWhenAlreadyCompacted() {
        createNodes();
        createFirstRelationships();
        createSecondRelationships();

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                for (Relationship relationship : database.getNodeById(1).getRelationships(withName("test"), OUTGOING)) {
                    if (relationship.getEndNode().getId() == 3) {
                        relationship.setProperty("key1", "value2");
                        break;
                    }
                }
                return null;
            }
        });

        try {
            new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value1").count(database.getNodeById(1));
            fail();
        } catch (UnableToCountException e) {
            //OK
        }

        try {
            new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value2").count(database.getNodeById(1));
            fail();
        } catch (UnableToCountException e) {
            //OK
        }

        try {
            new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value3").count(database.getNodeById(1));
            fail();
        } catch (UnableToCountException e) {
            //OK
        }

        assertEquals(8, new FullCachedRelationshipCounter(withName("test"), OUTGOING).count(database.getNodeById(1)));
        assertEquals(1, new FullCachedRelationshipCounter(withName("test"), INCOMING).count(database.getNodeById(1)));
    }

    @Test
    public void changingRelationshipsWorksCorrectlyWhenAlreadyCompacted() {
        createNodes();
        createFirstRelationships();
        createSecondRelationships();

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                for (Relationship relationship : database.getNodeById(1).getRelationships(withName("test"), OUTGOING)) {
                    if (relationship.getEndNode().getId() == 3) {
                        relationship.setProperty("key1", "value1");
                        break;
                    }
                }
                return null;
            }
        });

        try {
            new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value1").count(database.getNodeById(1));
            fail();
        } catch (UnableToCountException e) {
            //OK
        }

        try {
            new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value2").count(database.getNodeById(1));
            fail();
        } catch (UnableToCountException e) {
            //OK
        }

        try {
            new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value3").count(database.getNodeById(1));
            fail();
        } catch (UnableToCountException e) {
            //OK
        }

        assertEquals(8, new FullCachedRelationshipCounter(withName("test"), OUTGOING).count(database.getNodeById(1)));
        assertEquals(1, new FullCachedRelationshipCounter(withName("test"), INCOMING).count(database.getNodeById(1)));
    }

    @Test
    public void internalRelationshipsAreIgnored() {
        createNodes();

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                database.getNodeById(0).createRelationshipTo(database.getNodeById(1), withName(GA_PREFIX + "IGNORED")).setProperty("key1", "value1");
                return null;
            }
        });

        assertEquals(0, new FullCachedRelationshipCounter(withName(GA_PREFIX + "IGNORED"), OUTGOING).count(database.getNodeById(0)));

    }

    @Test
    public void inclusionStrategyIsHonored() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();
        txExecutor = new SimpleTransactionExecutor(database);

        GraphAwareFramework framework = new GraphAwareFramework(database);
        framework.registerModule(new FullRelationshipCountModule(RelationshipCountStrategiesImpl.defaultStrategies()
                .with(5)
                .with(IncludeNoRelationships.getInstance())));

        framework.start();

        createNodes();

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                database.getNodeById(0).createRelationshipTo(database.getNodeById(1), withName("test")).setProperty("key1", "value1");
                return null;
            }
        });

        assertEquals(0, new FullCachedRelationshipCounter(withName("test"), OUTGOING).count(database.getNodeById(0)));
    }

    @Test
    public void extractionStrategyIsHonored() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();
        txExecutor = new SimpleTransactionExecutor(database);

        GraphAwareFramework framework = new GraphAwareFramework(database);
        framework.registerModule(new FullRelationshipCountModule(RelationshipCountStrategiesImpl.defaultStrategies()
                .with(5)
                .with(new RelationshipPropertiesExtractionStrategy() {
                    @Override
                    public Map<String, String> extractProperties(Relationship relationship, Node pointOfView) {
                        Map<String, String> result = new HashMap<>();
                        for (String key : relationship.getPropertyKeys()) {
                            if (!"test".equals(key)) {
                                result.put(cleanKey(key), valueToString(relationship.getProperty(key)));
                            }
                        }
                        return result;
                    }
                })));

        framework.start();


        createNodes();

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                database.getNodeById(0).createRelationshipTo(database.getNodeById(1), withName("test")).setProperty("test", "value1");
                database.getNodeById(0).createRelationshipTo(database.getNodeById(1), withName("test")).setProperty("key1", "value1");
                return null;
            }
        });

        assertEquals(0, new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("test", "value1").count(database.getNodeById(0)));
        assertEquals(1, new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value1").count(database.getNodeById(0)));
        assertEquals(2, new FullCachedRelationshipCounter(withName("test"), OUTGOING).count(database.getNodeById(0)));
    }

    @Test
    public void extractionStrategyIsHonored2() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();
        txExecutor = new SimpleTransactionExecutor(database);

        GraphAwareFramework framework = new GraphAwareFramework(database);
        framework.registerModule(new FullRelationshipCountModule(RelationshipCountStrategiesImpl.defaultStrategies()
                .with(5)
                .with(new RelationshipPropertiesExtractionStrategy.SimpleAdapter() {
                    @Override
                    protected Map<String, String> extractProperties(Map<String, String> properties) {
                        Map<String, String> result = new HashMap<>(properties);
                        result.remove("test");
                        return result;
                    }
                })));

        framework.start();

        createNodes();

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                database.getNodeById(0).createRelationshipTo(database.getNodeById(1), withName("test")).setProperty("test", "value1");
                database.getNodeById(0).createRelationshipTo(database.getNodeById(1), withName("test")).setProperty("key1", "value1");
                return null;
            }
        });

        assertEquals(0, new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("test", "value1").count(database.getNodeById(0)));
        assertEquals(1, new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value1").count(database.getNodeById(0)));
        assertEquals(2, new FullCachedRelationshipCounter(withName("test"), OUTGOING).count(database.getNodeById(0)));
    }

    @Test
    public void extractionStrategyCanAccessOtherNode() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();
        txExecutor = new SimpleTransactionExecutor(database);

        GraphAwareFramework framework = new GraphAwareFramework(database);
        framework.registerModule(new FullRelationshipCountModule(RelationshipCountStrategiesImpl.defaultStrategies()
                .with(5)
                .with(new RelationshipPropertiesExtractionStrategy() {
                    @Override
                    public Map<String, String> extractProperties(Relationship relationship, Node pointOfView) {
                        Map<String, String> result = new HashMap<>();
                        result.putAll(propertiesToStringMap(relationship));
                        result.put("otherNodeName", relationship.getOtherNode(pointOfView).getProperty("name", "default").toString());
                        return result;
                    }
                })));

        framework.start();

        createNodes();

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                database.getNodeById(0).createRelationshipTo(database.getNodeById(1), withName("test")).setProperty("key1", "value1");
                return null;
            }
        });

        assertEquals(1, new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value1").with("otherNodeName", "node 1").count(database.getNodeById(0)));
        assertEquals(1, new FullCachedRelationshipCounter(withName("test"), INCOMING).with("key1", "value1").with("otherNodeName", "default").count(database.getNodeById(1)));
        assertEquals(1, new FullCachedRelationshipCounter(withName("test"), OUTGOING).count(database.getNodeById(0)));
    }

    @Test
    public void extractionStrategyCanAccessOtherNode2() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();
        txExecutor = new SimpleTransactionExecutor(database);

        GraphAwareFramework framework = new GraphAwareFramework(database);
        framework.registerModule(new FullRelationshipCountModule(RelationshipCountStrategiesImpl.defaultStrategies()
                .with(5)
                .with(new RelationshipPropertiesExtractionStrategy.OtherNodeIncludingAdapter() {
                    @Override
                    protected Map<String, String> extractProperties(Map<String, String> properties, Node otherNode) {
                        properties.put("otherNodeName", otherNode.getProperty("name", "default").toString());
                        return properties;
                    }
                })));

        framework.start();

        createNodes();

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                database.getNodeById(0).createRelationshipTo(database.getNodeById(1), withName("test")).setProperty("key1", "value1");
                return null;
            }
        });

        assertEquals(1, new FullCachedRelationshipCounter(withName("test"), OUTGOING).with("key1", "value1").with("otherNodeName", "node 1").count(database.getNodeById(0)));
        assertEquals(1, new FullCachedRelationshipCounter(withName("test"), INCOMING).with("key1", "value1").with("otherNodeName", "default").count(database.getNodeById(1)));
        assertEquals(1, new FullCachedRelationshipCounter(withName("test"), OUTGOING).count(database.getNodeById(0)));
    }

    @Test
    public void scenario() {
        TestDataBuilder builder = new TestDataBuilder(database);

        builder.node().setProp("name", "node1")
                .node().setProp("name", "node2")
                .node().setProp("name", "node3")
                .node().setProp("name", "node4")
                .node().setProp("name", "node5")
                .node().setProp("name", "node6")
                .node().setProp("name", "node7")
                .node().setProp("name", "node8")
                .node().setProp("name", "node9")
                .node().setProp("name", "node10")
                .relationshipTo(1, "FRIEND_OF").setProp("level", "1").setProp("timestamp", valueOf(currentTimeMillis()))
                .relationshipTo(2, "FRIEND_OF").setProp("level", "2").setProp("timestamp", valueOf(currentTimeMillis()))
                .relationshipTo(3, "FRIEND_OF").setProp("level", "3").setProp("timestamp", valueOf(currentTimeMillis()))
                .relationshipTo(4, "FRIEND_OF").setProp("level", "1").setProp("timestamp", valueOf(currentTimeMillis()))
                .relationshipTo(5, "FRIEND_OF").setProp("level", "1").setProp("timestamp", valueOf(currentTimeMillis()))
                .relationshipTo(6, "FRIEND_OF").setProp("level", "1").setProp("timestamp", valueOf(currentTimeMillis()))
                .relationshipTo(7, "FRIEND_OF").setProp("level", "1").setProp("timestamp", valueOf(currentTimeMillis()))
                .relationshipTo(8, "FRIEND_OF").setProp("level", "2").setProp("timestamp", valueOf(currentTimeMillis()))
                .relationshipTo(9, "FRIEND_OF").setProp("level", "2").setProp("timestamp", valueOf(currentTimeMillis()));

        assertEquals(5, new FullCachedRelationshipCounter(withName("FRIEND_OF"), OUTGOING).with("level", "1").count(database.getNodeById(10)));
        assertEquals(3, new FullCachedRelationshipCounter(withName("FRIEND_OF"), OUTGOING).with("level", "2").count(database.getNodeById(10)));
        assertEquals(1, new FullCachedRelationshipCounter(withName("FRIEND_OF"), OUTGOING).with("level", "3").count(database.getNodeById(10)));
        assertEquals(9, new FullCachedRelationshipCounter(withName("FRIEND_OF"), OUTGOING).count(database.getNodeById(10)));

        builder.relationshipTo(1, "FRIEND_OF").setProp("level", "4").setProp("timestamp", valueOf(currentTimeMillis()));
        builder.relationshipTo(2, "FRIEND_OF").setProp("level", "5").setProp("timestamp", valueOf(currentTimeMillis()));
        builder.relationshipTo(3, "FRIEND_OF").setProp("level", "6").setProp("timestamp", valueOf(currentTimeMillis()));
        builder.relationshipTo(4, "FRIEND_OF").setProp("level", "7").setProp("timestamp", valueOf(currentTimeMillis()));

        try {
            new FullCachedRelationshipCounter(withName("FRIEND_OF"), OUTGOING).with("level", "1").count(database.getNodeById(10));
            fail();
        } catch (UnableToCountException e) {
            //ok
        }

        try {
            new FullCachedRelationshipCounter(withName("FRIEND_OF"), OUTGOING).with("level", "2").count(database.getNodeById(10));
            fail();
        } catch (UnableToCountException e) {
            //ok
        }

        try {
            new FullCachedRelationshipCounter(withName("FRIEND_OF"), OUTGOING).with("level", "3").count(database.getNodeById(10));
            fail();
        } catch (UnableToCountException e) {
            //ok
        }

        try {
            new FullCachedRelationshipCounter(withName("FRIEND_OF"), OUTGOING).with("level", "4").count(database.getNodeById(10));
            fail();
        } catch (UnableToCountException e) {
            //ok
        }

        try {
            new FullCachedRelationshipCounter(withName("FRIEND_OF"), OUTGOING).with("level", "5").count(database.getNodeById(10));
            fail();
        } catch (UnableToCountException e) {
            //ok
        }

        try {
            new FullCachedRelationshipCounter(withName("FRIEND_OF"), OUTGOING).with("level", "6").count(database.getNodeById(10));
            fail();
        } catch (UnableToCountException e) {
            //ok
        }

        try {
            new FullCachedRelationshipCounter(withName("FRIEND_OF"), OUTGOING).with("level", "7").count(database.getNodeById(10));
            fail();
        } catch (UnableToCountException e) {
            //ok
        }

        assertEquals(13, new FullCachedRelationshipCounter(withName("FRIEND_OF"), OUTGOING).count(database.getNodeById(10)));
    }

    private void createFirstRelationships() {
        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                Node node1 = database.getNodeById(1);
                Node node2 = database.getNodeById(2);
                Node node3 = database.getNodeById(3);
                Node node4 = database.getNodeById(4);
                Node node5 = database.getNodeById(5);
                Node node6 = database.getNodeById(6);

                node1.createRelationshipTo(node2, withName("test")).setProperty("key1", "value1");
                node1.createRelationshipTo(node3, withName("test")).setProperty("key1", "value2");
                node1.createRelationshipTo(node4, withName("test"));
                node1.createRelationshipTo(node5, withName("test")).setProperty("key1", "value2");
                node6.createRelationshipTo(node1, withName("test")).setProperty("key2", "value1");

                return null;
            }
        });
    }

    private void createSecondRelationships() {
        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                Node node1 = database.getNodeById(1);
                Node node7 = database.getNodeById(7);
                Node node8 = database.getNodeById(8);
                Node node9 = database.getNodeById(9);
                Node node10 = database.getNodeById(10);

                node1.createRelationshipTo(node7, withName("test")).setProperty("key1", "value3");
                node1.createRelationshipTo(node8, withName("test")).setProperty("key1", "value4");
                node1.createRelationshipTo(node9, withName("test")).setProperty("key1", "value5");
                node1.createRelationshipTo(node10, withName("test")).setProperty("key1", "value6");

                return null;
            }
        });
    }

    private void createNodes() {
        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                for (int i = 0; i < 10; i++) {
                    Node node = database.createNode();
                    node.setProperty("name", "node " + (i + 1));
                }
                return null;
            }
        });
    }
}
