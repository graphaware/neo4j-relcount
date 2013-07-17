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

package com.graphaware.neo4j;

import com.graphaware.neo4j.common.NullItem;
import com.graphaware.neo4j.tx.batch.IterableInputBatchExecutor;
import com.graphaware.neo4j.tx.batch.NoInputBatchExecutor;
import com.graphaware.neo4j.tx.batch.UnitOfWork;
import com.graphaware.neo4j.tx.single.SimpleTransactionExecutor;
import com.graphaware.neo4j.tx.single.TransactionCallback;
import com.graphaware.neo4j.tx.single.TransactionExecutor;
import com.graphaware.neo4j.utils.DeleteUtils;
import org.apache.log4j.Logger;
import org.neo4j.graphdb.*;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.Iterator;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static com.graphaware.neo4j.utils.IterableUtils.random;
import static org.neo4j.graphdb.Direction.OUTGOING;

public class AnotherRandomUsageSimulator {
    private static final Logger LOG = Logger.getLogger(AnotherRandomUsageSimulator.class);

    public static final String USER = "user";
    public static final String TYPE = "type";
    public static final RelationshipType FRIEND_OF = DynamicRelationshipType.withName("FRIEND_OF");
    public static final String TIMESTAMP = "timestamp";
    public static final String LEVEL = "level";
    public static final int NODES = 1000;

    private final GraphDatabaseService database;
    private final TransactionExecutor txExecutor;

    private final Random random = new Random(50L);
    private final AtomicLong noNodes = new AtomicLong(0);
    private final AtomicLong noRels = new AtomicLong(0);

    public AnotherRandomUsageSimulator(GraphDatabaseService database) {
        this.database = database;
        txExecutor = new SimpleTransactionExecutor(database);
    }

    public void batchSimulate(int steps) {
        deleteRoot();

        new NoInputBatchExecutor(database, 1000, NODES, new UnitOfWork<NullItem>() {
            @Override
            public void execute(GraphDatabaseService database, NullItem input) {
                doCreateUser();
            }
        }).execute();

        new IterableInputBatchExecutor<>(database, 10, GlobalGraphOperations.at(database).getAllNodes(), new UnitOfWork<Node>() {
            @Override
            public void execute(GraphDatabaseService database, Node node) {
                int friends = random.nextInt(901) + 100;
                for (Node potentialFriend : GlobalGraphOperations.at(database).getAllNodes()) {
                    if (random.nextInt(NODES / friends) == 0 && !potentialFriend.equals(node)) {
                        doCreateFriendship(node, potentialFriend);
                    }

                }
                LOG.info("Created " + friends + " friends for node " + node.getId());
            }
        }).execute();

        new NoInputBatchExecutor(database, 10, steps, new UnitOfWork<NullItem>() {
            @Override
            public void execute(GraphDatabaseService database, NullItem input) {
                performStep();
            }
        }).execute();
    }

    private void performStep() {
        int r = random.nextInt(100);
        if (r < 20) {
            createUser();
        } else if (r < 80) {
            createFriendship();
        } else if (r < 85) {
            updateUser();
        } else if (r < 90) {
            updateFriendship();
        } else if (r < 95) {
            deleteUser();
        } else {
            deleteFriendship();
        }
    }

    private void createUser() {
        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                doCreateUser();
                return null;
            }
        });
    }

    private void doCreateUser() {
        Node user = database.createNode();
        user.setProperty(TYPE, USER);
        user.setProperty("firstName", randomString());
        user.setProperty("lastName", randomString());
        user.setProperty("age", random.nextInt(50) + 18);
        user.setProperty("location", randomString());
        user.setProperty(TIMESTAMP, System.currentTimeMillis());
        noNodes.incrementAndGet();
    }

    private void deleteUser() {
        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                doDeleteUser();
                return null;
            }
        });
    }

    private void doDeleteUser() {
        noRels.addAndGet(-DeleteUtils.deleteNodeAndRelationships(findRandomNode(USER)));
        noNodes.decrementAndGet();
    }

    private void updateUser() {
        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                doUpdateUser();
                return null;
            }
        });
    }

    private void doUpdateUser() {
        findRandomNode(USER).setProperty("location", randomString());
    }

    private void createFriendship() {
        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                Node node1 = findRandomNode(USER);
                Node node2 = findRandomNodeOtherThan(USER, node1);
                doCreateFriendship(node1, node2);
                return null;
            }
        });
    }

    private void doCreateFriendship(Node node1, Node node2) {
        Relationship friendship = node1.createRelationshipTo(node2, FRIEND_OF);
        friendship.setProperty(TIMESTAMP, System.currentTimeMillis());
        friendship.setProperty(LEVEL, random.nextInt(5));

        noRels.incrementAndGet();
    }

    private void deleteFriendship() {
        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                return doDeleteFriendship();
            }
        });
    }

    private Void doDeleteFriendship() {
        while (true) {
            Node user = findRandomNode(USER);
            if (user.hasRelationship(OUTGOING, FRIEND_OF)) {
                random(user.getRelationships(OUTGOING, FRIEND_OF)).delete();
                noRels.decrementAndGet();
                return null;
            }
        }
    }

    private void updateFriendship() {
        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
            @Override
            public Void doInTransaction(GraphDatabaseService database) {
                return doUpdateFriendship();
            }
        });
    }

    private Void doUpdateFriendship() {
        while (true) {
            Node user = findRandomNode(USER);
            if (user.hasRelationship(OUTGOING, FRIEND_OF)) {
                random(user.getRelationships(OUTGOING, FRIEND_OF)).setProperty(LEVEL, random.nextInt(5));
                return null;
            }
        }
    }

    private String randomString() {
        return UUID.randomUUID().toString();
    }

    private Node findRandomNodeOtherThan(String type, Node otherNode) {
        Node node = findRandomNode(type);

        if (!otherNode.equals(node)) {
            return node;
        }

        return findRandomNodeOtherThan(type, otherNode);
    }

    private Node findRandomNode(String type) {
        long number = random.nextInt(noNodes.intValue());

        Iterator<Node> nodes = GlobalGraphOperations.at(database).getAllNodes().iterator();
        for (int i = 0; i < number; i++) {
            if (nodes.hasNext()) {
                nodes.next();
            }
        }

        while (nodes.hasNext()) {
            Node next = nodes.next();
            if (next.getProperty(TYPE, "").equals(type)) {  //todo investigate why some nodes don't have this, tx isolation?
                return next;
            }
        }

        return findRandomNode(type);
    }

    private void deleteRoot() {
        txExecutor.executeInTransaction(new TransactionCallback<Object>() {
            @Override
            public Object doInTransaction(GraphDatabaseService database) {
                database.getNodeById(0).delete();
                return null;
            }
        });
    }

//    private void tt() {
//        txExecutor.executeInTransaction(new TransactionCallback<Void>() {
//            @Override
//            public Void doInTransaction(GraphDatabaseService database) {
//                return null;
//            }
//        });
//    }

}