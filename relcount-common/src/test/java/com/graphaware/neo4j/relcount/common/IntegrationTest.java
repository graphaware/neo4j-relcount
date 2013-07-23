package com.graphaware.neo4j.relcount.common;

import com.graphaware.neo4j.framework.config.BaseFrameworkConfiguration;
import com.graphaware.neo4j.tx.single.SimpleTransactionExecutor;
import com.graphaware.neo4j.tx.single.VoidReturningCallback;
import org.junit.After;
import org.junit.Before;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.nio.ByteBuffer;

import static com.graphaware.neo4j.relcount.common.IntegrationTest.RelationshipTypes.ONE;
import static com.graphaware.neo4j.relcount.common.IntegrationTest.RelationshipTypes.TWO;
import static org.neo4j.graphdb.Direction.INCOMING;

/**
 * Base class for integration tests.
 */
public abstract class IntegrationTest {

    public static final String WEIGHT = "weight";
    public static final String NAME = "name";
    public static final String TIMESTAMP = "timestamp";
    public static final String K1 = "K1";
    public static final String K2 = "K2";

    public enum RelationshipTypes implements RelationshipType {
        ONE,
        TWO
    }

    protected GraphDatabaseService database;

    @Before
    public void setUp() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();
    }

    @After
    public void tearDown() {
        database.shutdown();
    }

    protected void simulateUsage() {
        new SimpleTransactionExecutor(database).executeInTransaction(new VoidReturningCallback() {
            @Override
            protected void doInTx(GraphDatabaseService database) {
                Node one = database.getNodeById(1);
                Node two = database.getNodeById(2);

                Relationship cycle = one.createRelationshipTo(one, ONE);
                cycle.setProperty(WEIGHT, 2);

                Relationship oneToTwo = one.createRelationshipTo(two, ONE);
                oneToTwo.setProperty(WEIGHT, 2);
                oneToTwo.setProperty(TIMESTAMP, 123L);
                oneToTwo.setProperty(K1, "V1");

                oneToTwo = one.createRelationshipTo(two, ONE);
                oneToTwo.setProperty(WEIGHT, 1);
                oneToTwo.setProperty(K1, "V1");
            }
        });

        new SimpleTransactionExecutor(database).executeInTransaction(new VoidReturningCallback() {
            @Override
            protected void doInTx(GraphDatabaseService database) {
                Node one = database.getNodeById(1);
                Node two = database.getNodeById(2);

                Relationship oneToTwo = one.createRelationshipTo(two, ONE);
                oneToTwo.setProperty(K1, "V1");

                oneToTwo = one.createRelationshipTo(two, ONE);
                oneToTwo.setProperty(K1, "V1");

                oneToTwo = one.createRelationshipTo(two, ONE);
                oneToTwo.setProperty(K1, "V1");

                oneToTwo = one.createRelationshipTo(two, ONE);
                oneToTwo.setProperty(WEIGHT, 1);

                oneToTwo = one.createRelationshipTo(two, TWO);
                oneToTwo.setProperty(K1, "V1");
                oneToTwo.setProperty(K2, "V1");
            }
        });

        new SimpleTransactionExecutor(database).executeInTransaction(new VoidReturningCallback() {
            @Override
            protected void doInTx(GraphDatabaseService database) {
                Node one = database.getNodeById(1);
                Node two = database.getNodeById(2);

                Relationship twoToOne = two.createRelationshipTo(one, ONE);
                twoToOne.setProperty(K1, "V1");
                twoToOne.setProperty(WEIGHT, 5);

                twoToOne = two.createRelationshipTo(one, ONE);
                twoToOne.setProperty(WEIGHT, 3);
                twoToOne.setProperty("something long", "Some incredibly long text with many characters )(*&^%@£, we hope it's not gonna break the system. \n Just in case, we're also gonna check a long byte array as the next property.");
                twoToOne.setProperty("bytearray", ByteBuffer.allocate(8).putLong(1242352145243231L).array());

                twoToOne = two.createRelationshipTo(one, ONE);
                twoToOne.setProperty(K1, "V1");
                twoToOne.setProperty(K2, "V2");
            }
        });

        new SimpleTransactionExecutor(database).executeInTransaction(new VoidReturningCallback() {
            @Override
            protected void doInTx(GraphDatabaseService database) {
                Node one = database.getNodeById(1);

                for (Relationship r : one.getRelationships(ONE, INCOMING)) {
                    if (r.getProperty(WEIGHT, 0).equals(3)) {
                        r.delete();
                        continue;
                    }
                    if (r.getProperty(WEIGHT, 0).equals(5)) {
                        r.setProperty(WEIGHT, 2);
                    }
                    if (r.getStartNode().equals(r.getEndNode())) {
                        r.setProperty(WEIGHT, 7);
                    }
                }
            }
        });
    }

    protected void setUpTwoNodes() {
        new SimpleTransactionExecutor(database).executeInTransaction(new VoidReturningCallback() {
            @Override
            protected void doInTx(GraphDatabaseService database) {
                Node one = database.createNode();
                one.setProperty(NAME, "One");
                one.setProperty(WEIGHT, 1);

                Node two = database.createNode();
                two.setProperty(NAME, "Two");
                two.setProperty(WEIGHT, 2);
            }
        });
    }

    public class CustomConfig extends BaseFrameworkConfiguration {

        /**
         * {@inheritDoc}
         */
        @Override
        public String separator() {
            return ";";
        }
    }
}
