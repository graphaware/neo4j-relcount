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

package com.graphaware.module.relcount.count;

import com.graphaware.common.description.relationship.RelationshipDescription;
import com.graphaware.common.policy.none.IncludeNoRelationships;
import com.graphaware.common.serialize.Serializer;
import com.graphaware.module.relcount.RelationshipCountModule;
import com.graphaware.module.relcount.compact.ThresholdBasedCompactionStrategy;
import com.graphaware.runtime.GraphAwareRuntime;
import com.graphaware.runtime.GraphAwareRuntimeFactory;
import com.graphaware.runtime.config.FluentRuntimeConfiguration;
import com.graphaware.test.util.TestDataBuilder;
import com.graphaware.tx.executor.single.SimpleTransactionExecutor;
import com.graphaware.tx.executor.single.TransactionCallback;
import com.graphaware.tx.executor.single.TransactionExecutor;
import com.graphaware.tx.executor.single.VoidReturningCallback;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import static com.graphaware.common.description.predicate.Predicates.equalTo;
import static com.graphaware.common.description.relationship.RelationshipDescriptionFactory.literal;
import static com.graphaware.common.description.relationship.RelationshipDescriptionFactory.wildcard;
import static com.graphaware.common.util.PropertyContainerUtils.deleteNodeAndRelationships;
import static com.graphaware.module.relcount.RelationshipCountConfigurationImpl.defaultConfiguration;
import static com.graphaware.runtime.config.RuntimeConfiguration.GA_PREFIX;
import static java.lang.String.valueOf;
import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;

/**
 * Test for {@link CachedRelationshipCounter}.
 */
public class CachedRelationshipCounterTest {

    private GraphDatabaseService database;
    private TransactionExecutor txExecutor;

    @Before
    public void setUp() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();
        txExecutor = new SimpleTransactionExecutor(database);

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new RelationshipCountModule(defaultConfiguration().with(new ThresholdBasedCompactionStrategy(5))));
        runtime.start();
    }

    @After
    public void tearDown() {
        database.shutdown();
    }

    @Test
    public void noRelationshipsShouldExistInEmptyDatabase() {
        try (Transaction tx = database.beginTx()) {
            database.createNode();
            tx.success();
        }

        assertEquals(0, count(wildcard("test", OUTGOING), 0));
        assertEquals(0, count(literal("test", OUTGOING), 0));
    }

    @Test
    public void noRelationshipsShouldExistInDatabaseWithNoRelationships() {
        createNodes();

        assertEquals(0, count(wildcard("test", OUTGOING), 0));
        assertEquals(0, count(literal("test", OUTGOING), 0));
    }

    private int count(RelationshipDescription description, long nodeId) {
        int count;

        try (Transaction tx = database.beginTx()) {
            count = new CachedRelationshipCounter(database).count(database.getNodeById(nodeId), description);
            tx.success();
        }

        return count;
    }

    @Test
    public void relationshipsBelowThresholdShouldBeCountedOneByOne() {
        createNodes();
        createFirstRelationships();

        assertEquals(1, count(wildcard("test", OUTGOING).with("key1", equalTo("value1")), 0));
        assertEquals(2, count(wildcard("test", OUTGOING).with("key1", equalTo("value2")), 0));
        assertEquals(0, count(wildcard("test", OUTGOING).with("key1", equalTo("value3")), 0));
        assertEquals(4, count(wildcard("test", OUTGOING), 0));
        assertEquals(1, count(wildcard("test", INCOMING), 0));
        assertEquals(1, count(wildcard("test", INCOMING).with("key2", equalTo("value1")), 0));
    }

    @Test
    public void relationshipsAboveThresholdShouldNotBeCountableOneByOne() {
        createNodes();
        createFirstRelationships();
        createSecondRelationships();

        try {
            count(wildcard("test", OUTGOING).with("key1", equalTo("value1")), 0);
            fail();
        } catch (UnableToCountException e) {
            //OK
        }

        try {
            count(wildcard("test", OUTGOING).with("key1", equalTo("value2")), 0);
            fail();
        } catch (UnableToCountException e) {
            //OK
        }

        try {
            count(wildcard("test", OUTGOING).with("key1", equalTo("value3")), 0);
            fail();
        } catch (UnableToCountException e) {
            //OK
        }

        assertEquals(8, count(wildcard("test", OUTGOING), 0));
        assertEquals(1, count(wildcard("test", INCOMING), 0));
    }

    @Test
    public void deletingRelationshipsShouldCorrectlyDecrementCounts1() {
        createNodes();
        createFirstRelationships();

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                for (Relationship relationship : database.getNodeById(0).getRelationships(withName("test"), OUTGOING)) {
                    if (relationship.getEndNode().getId() == 1) {
                        relationship.delete();
                        break;
                    }
                }
                return null;
            }
        });

        assertEquals(0, count(wildcard("test", OUTGOING).with("key1", equalTo("value1")), 0));
        assertEquals(2, count(wildcard("test", OUTGOING).with("key1", equalTo("value2")), 0));
        assertEquals(0, count(wildcard("test", OUTGOING).with("key1", equalTo("value3")), 0));
        assertEquals(3, count(wildcard("test", OUTGOING), 0));
        assertEquals(1, count(wildcard("test", INCOMING), 0));
    }

    @Test
    public void deletingRelationshipsShouldCorrectlyDecrementCounts2() {
        createNodes();
        createFirstRelationships();
        createSecondRelationships();

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                for (Relationship relationship : database.getNodeById(0).getRelationships(withName("test"), OUTGOING)) {
                    if (relationship.getEndNode().getId() == 1) {
                        relationship.delete();
                        break;
                    }
                }
                return null;
            }
        });

        try {
            count(wildcard("test", OUTGOING).with("key1", equalTo("value1")), 0);
            fail();
        } catch (UnableToCountException e) {
            //OK
        }

        try {
            count(wildcard("test", OUTGOING).with("key1", equalTo("value2")), 0);
            fail();
        } catch (UnableToCountException e) {
            //OK
        }

        try {
            count(wildcard("test", OUTGOING).with("key1", equalTo("value3")), 0);
            fail();
        } catch (UnableToCountException e) {
            //OK
        }

        assertEquals(7, count(wildcard("test", OUTGOING), 0));
        assertEquals(1, count(wildcard("test", INCOMING), 0));
    }

    @Test
    public void deletingBelowZeroShouldNotDoAnyHarm() {
        createNodes();
        createFirstRelationships();

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                database.getNodeById(0).setProperty(serialize(literal("test", OUTGOING).with("key1", equalTo("value1"))), 0);
                return null;
            }
        });

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                for (Relationship relationship : database.getNodeById(0).getRelationships(withName("test"), OUTGOING)) {
                    if (relationship.getEndNode().getId() == 1) {
                        relationship.delete();
                        break;
                    }
                }
                return null;
            }
        });

        assertEquals(0, count(wildcard("test", OUTGOING).with("key1", equalTo("value1")), 0));
        assertEquals(2, count(wildcard("test", OUTGOING).with("key1", equalTo("value2")), 0));
        assertEquals(0, count(wildcard("test", OUTGOING).with("key1", equalTo("value3")), 0));
        assertEquals(3, count(wildcard("test", OUTGOING), 0));
        assertEquals(1, count(wildcard("test", INCOMING), 0));
    }

    @Test
    public void deletingNonExistingShouldNotDoAnyHarm() {
        createNodes();
        createFirstRelationships();

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                database.getNodeById(1).createRelationshipTo(database.getNodeById(9), withName("test2")).setProperty("key1", "value3");
                return null;
            }
        });

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                database.getNodeById(1).removeProperty(serialize(literal("test2", OUTGOING).with("key1", equalTo("value3"))));
                return null;
            }
        });

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                database.getNodeById(1).getSingleRelationship(withName("test2"), OUTGOING).delete();
                return null;
            }
        });

        assertEquals(1, count(wildcard("test", OUTGOING).with("key1", equalTo("value1")), 0));
        assertEquals(2, count(wildcard("test", OUTGOING).with("key1", equalTo("value2")), 0));
        assertEquals(0, count(wildcard("test", OUTGOING).with("key1", equalTo("value3")), 0));
        assertEquals(4, count(wildcard("test", OUTGOING), 0));
        assertEquals(1, count(wildcard("test", INCOMING), 0));
        assertEquals(1, count(wildcard("test", INCOMING).with("key2", equalTo("value1")), 0));
    }

    @Test
    public void deletingNodeWithAllRelationshipsWorkCorrectly() {
        createNodes();
        createFirstRelationships();
        createSecondRelationships();

        assertEquals(1, count(wildcard("test", INCOMING), 1));
        assertEquals(1, count(wildcard("test", INCOMING), 2));

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                deleteNodeAndRelationships(database.getNodeById(0));
                return null;
            }
        });

        assertEquals(0, count(wildcard("test", INCOMING), 1));
        assertEquals(0, count(wildcard("test", INCOMING), 2));
    }

    @Test
    public void changingRelationshipsWithNoActualChangeWorksCorrectly() {
        createNodes();
        createFirstRelationships();

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                for (Relationship relationship : database.getNodeById(0).getRelationships(withName("test"), OUTGOING)) {
                    if (relationship.getEndNode().getId() == 2) {
                        relationship.setProperty("key1", "value2");
                        break;
                    }
                }
                return null;
            }
        });

        assertEquals(1, count(wildcard("test", OUTGOING).with("key1", equalTo("value1")), 0));
        assertEquals(2, count(wildcard("test", OUTGOING).with("key1", equalTo("value2")), 0));
        assertEquals(0, count(wildcard("test", OUTGOING).with("key1", equalTo("value3")), 0));
        assertEquals(4, count(wildcard("test", OUTGOING), 0));
        assertEquals(1, count(wildcard("test", INCOMING), 0));
    }

    @Test
    public void changingRelationshipsWorksWhenNotYetCompacted() {
        createNodes();
        createFirstRelationships();

        txExecutor.executeInTransaction(new VoidReturningCallback() {
            @Override
            public void doInTx(GraphDatabaseService database) {
                for (Relationship relationship : database.getNodeById(0).getRelationships(withName("test"), OUTGOING)) {
                    if (relationship.getEndNode().getId() == 2) {
                        relationship.setProperty("key1", "value1");
                        break;
                    }
                }
            }
        });

        assertEquals(2, count(wildcard("test", OUTGOING).with("key1", equalTo("value1")), 0));
        assertEquals(1, count(wildcard("test", OUTGOING).with("key1", equalTo("value2")), 0));
        assertEquals(0, count(wildcard("test", OUTGOING).with("key1", equalTo("value3")), 0));
        assertEquals(4, count(wildcard("test", OUTGOING), 0));
        assertEquals(1, count(wildcard("test", INCOMING), 0));
    }

    @Test
    public void changingRelationshipsWithNoActualChangeWorksWhenAlreadyCompacted() {
        createNodes();
        createFirstRelationships();
        createSecondRelationships();

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                for (Relationship relationship : database.getNodeById(0).getRelationships(withName("test"), OUTGOING)) {
                    if (relationship.getEndNode().getId() == 2) {
                        relationship.setProperty("key1", "value2");
                        break;
                    }
                }
                return null;
            }
        });

        try {
            count(wildcard("test", OUTGOING).with("key1", equalTo("value1")), 0);
            fail();
        } catch (UnableToCountException e) {
            //OK
        }

        try {
            count(wildcard("test", OUTGOING).with("key1", equalTo("value2")), 0);
            fail();
        } catch (UnableToCountException e) {
            //OK
        }

        try {
            count(wildcard("test", OUTGOING).with("key1", equalTo("value3")), 0);
            fail();
        } catch (UnableToCountException e) {
            //OK
        }

        assertEquals(8, count(wildcard("test", OUTGOING), 0));
        assertEquals(1, count(wildcard("test", INCOMING), 0));
    }

    @Test
    public void changingRelationshipsWorksCorrectlyWhenAlreadyCompacted() {
        createNodes();
        createFirstRelationships();
        createSecondRelationships();

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                for (Relationship relationship : database.getNodeById(0).getRelationships(withName("test"), OUTGOING)) {
                    if (relationship.getEndNode().getId() == 2) {
                        relationship.setProperty("key1", "value1");
                        break;
                    }
                }
                return null;
            }
        });

        try {
            count(wildcard("test", OUTGOING).with("key1", equalTo("value1")), 0);
            fail();
        } catch (UnableToCountException e) {
            //OK
        }

        try {
            count(wildcard("test", OUTGOING).with("key1", equalTo("value2")), 0);
            fail();
        } catch (UnableToCountException e) {
            //OK
        }

        try {
            count(wildcard("test", OUTGOING).with("key1", equalTo("value3")), 0);
            fail();
        } catch (UnableToCountException e) {
            //OK
        }

        assertEquals(8, count(wildcard("test", OUTGOING), 0));
        assertEquals(1, count(wildcard("test", INCOMING), 0));
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

        assertEquals(0, count(wildcard(withName(GA_PREFIX + "IGNORED"), OUTGOING), 0));

    }

    @Test
    public void inclusionPolicyIsHonored() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();
        txExecutor = new SimpleTransactionExecutor(database);

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new RelationshipCountModule(defaultConfiguration()
                .with(new ThresholdBasedCompactionStrategy(5))
                .with(IncludeNoRelationships.getInstance())));

        runtime.start();

        createNodes();

        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                database.getNodeById(0).createRelationshipTo(database.getNodeById(1), withName("test")).setProperty("key1", "value1");
                return null;
            }
        });

        assertEquals(0, count(wildcard("test", OUTGOING), 0));
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
                .relationshipTo(0, "FRIEND_OF").setProp("level", "1").setProp("timestamp", valueOf(currentTimeMillis()))
                .relationshipTo(1, "FRIEND_OF").setProp("level", "2").setProp("timestamp", valueOf(currentTimeMillis()))
                .relationshipTo(2, "FRIEND_OF").setProp("level", "3").setProp("timestamp", valueOf(currentTimeMillis()))
                .relationshipTo(3, "FRIEND_OF").setProp("level", "1").setProp("timestamp", valueOf(currentTimeMillis()))
                .relationshipTo(4, "FRIEND_OF").setProp("level", "1").setProp("timestamp", valueOf(currentTimeMillis()))
                .relationshipTo(5, "FRIEND_OF").setProp("level", "1").setProp("timestamp", valueOf(currentTimeMillis()))
                .relationshipTo(6, "FRIEND_OF").setProp("level", "1").setProp("timestamp", valueOf(currentTimeMillis()))
                .relationshipTo(7, "FRIEND_OF").setProp("level", "2").setProp("timestamp", valueOf(currentTimeMillis()))
                .relationshipTo(8, "FRIEND_OF").setProp("level", "2").setProp("timestamp", valueOf(currentTimeMillis()));

        assertEquals(5, count(wildcard(withName("FRIEND_OF"), OUTGOING).with("level", equalTo("1")), 9));
        assertEquals(3, count(wildcard(withName("FRIEND_OF"), OUTGOING).with("level", equalTo("2")), 9));
        assertEquals(1, count(wildcard(withName("FRIEND_OF"), OUTGOING).with("level", equalTo("3")), 9));
        assertEquals(9, count(wildcard(withName("FRIEND_OF"), OUTGOING), 9));

        builder.relationshipTo(0, "FRIEND_OF").setProp("level", "4").setProp("timestamp", valueOf(currentTimeMillis()));
        builder.relationshipTo(1, "FRIEND_OF").setProp("level", "5").setProp("timestamp", valueOf(currentTimeMillis()));
        builder.relationshipTo(2, "FRIEND_OF").setProp("level", "6").setProp("timestamp", valueOf(currentTimeMillis()));
        builder.relationshipTo(3, "FRIEND_OF").setProp("level", "7").setProp("timestamp", valueOf(currentTimeMillis()));

        try {
            count(wildcard(withName("FRIEND_OF"), OUTGOING).with("level", equalTo("1")), 9);
            fail();
        } catch (UnableToCountException e) {
            //ok
        }

        try {
            count(wildcard(withName("FRIEND_OF"), OUTGOING).with("level", equalTo("2")), 9);
            fail();
        } catch (UnableToCountException e) {
            //ok
        }

        try {
            count(wildcard(withName("FRIEND_OF"), OUTGOING).with("level", equalTo("3")), 9);
            fail();
        } catch (UnableToCountException e) {
            //ok
        }

        try {
            count(wildcard(withName("FRIEND_OF"), OUTGOING).with("level", equalTo("4")), 9);
            fail();
        } catch (UnableToCountException e) {
            //ok
        }

        try {
            count(wildcard(withName("FRIEND_OF"), OUTGOING).with("level", equalTo("5")), 9);
            fail();
        } catch (UnableToCountException e) {
            //ok
        }

        try {
            count(wildcard(withName("FRIEND_OF"), OUTGOING).with("level", equalTo("6")), 9);
            fail();
        } catch (UnableToCountException e) {
            //ok
        }

        try {
            count(wildcard(withName("FRIEND_OF"), OUTGOING).with("level", equalTo("7")), 9);
            fail();
        } catch (UnableToCountException e) {
            //ok
        }

        assertEquals(13, count(wildcard(withName("FRIEND_OF"), OUTGOING), 9));
    }

    private void createFirstRelationships() {
        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                Node node1 = database.getNodeById(0);
                Node node2 = database.getNodeById(1);
                Node node3 = database.getNodeById(2);
                Node node4 = database.getNodeById(3);
                Node node5 = database.getNodeById(4);
                Node node6 = database.getNodeById(5);

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
                Node node1 = database.getNodeById(0);
                Node node7 = database.getNodeById(6);
                Node node8 = database.getNodeById(7);
                Node node9 = database.getNodeById(8);
                Node node10 = database.getNodeById(9);

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

    private String serialize(RelationshipDescription description) {
        return Serializer.toString(description, FluentRuntimeConfiguration.defaultConfiguration().createPrefix(RelationshipCountModule.FULL_RELCOUNT_DEFAULT_ID));
    }
}
