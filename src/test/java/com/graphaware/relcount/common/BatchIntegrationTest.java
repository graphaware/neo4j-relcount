package com.graphaware.relcount.common;

import com.graphaware.framework.config.BaseFrameworkConfiguration;
import com.graphaware.tx.event.batch.api.TransactionSimulatingBatchInserter;
import com.graphaware.tx.event.batch.api.TransactionSimulatingBatchInserterImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.unsafe.batchinsert.BatchRelationship;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.graphaware.relcount.common.IntegrationTest.RelationshipTypes.ONE;
import static com.graphaware.relcount.common.IntegrationTest.RelationshipTypes.TWO;

/**
 * Base class for integration tests.
 */
public abstract class BatchIntegrationTest {

    public static final String WEIGHT = "weight";
    public static final String NAME = "name";
    public static final String TIMESTAMP = "timestamp";
    public static final String K1 = "K1";
    public static final String K2 = "K2";

    public enum RelationshipTypes implements RelationshipType {
        ONE,
        TWO
    }

    protected final TemporaryFolder temporaryFolder = new TemporaryFolder();
    protected GraphDatabaseService database;
    protected TransactionSimulatingBatchInserter batchInserter;

    @Before
    public void setUp() throws IOException {
        temporaryFolder.create();
        batchInserter = new TransactionSimulatingBatchInserterImpl(BatchInserters.inserter(temporaryFolder.getRoot().getAbsolutePath()));
    }

    @After
    public void tearDown() {
        database.shutdown();
        temporaryFolder.delete();
    }

    protected void startDatabase() {
        batchInserter.shutdown();
        database = new GraphDatabaseFactory().newEmbeddedDatabase(temporaryFolder.getRoot().getAbsolutePath());
    }

    protected void simulateInserts() {
        Map<String, Object> props = new HashMap<>();
        props.put(WEIGHT, 2);

        batchInserter.createRelationship(1, 1, ONE, props);

        props = new HashMap<>();
        props.put(WEIGHT, 2);
        props.put(TIMESTAMP, 123L);
        props.put(K1, "V1");
        batchInserter.createRelationship(1, 2, ONE, props);

        props = new HashMap<>();
        props.put(WEIGHT, 1);
        props.put(K1, "V1");
        batchInserter.createRelationship(1, 2, ONE, props);

        props = new HashMap<>();
        props.put(K1, "V1");
        batchInserter.createRelationship(1, 2, ONE, props);
        batchInserter.createRelationship(1, 2, ONE, props);
        batchInserter.createRelationship(1, 2, ONE, props);

        props = new HashMap<>();
        props.put(WEIGHT, 1);
        batchInserter.createRelationship(1, 2, ONE, props);

        props = new HashMap<>();
        props.put(K1, "V1");
        props.put(K2, "V1");
        batchInserter.createRelationship(1, 2, TWO, props);

        props = new HashMap<>();
        props.put(K1, "V1");
        props.put(WEIGHT, 5);
        batchInserter.createRelationship(2, 1, ONE, props);

        props = new HashMap<>();
        props.put(K1, "V1");
        props.put(K2, "V2");
        batchInserter.createRelationship(2, 1, ONE, props);

        for (BatchRelationship r : batchInserter.getRelationships(1)) {
            if (r.getStartNode() == 1 && r.getEndNode() != 1) {
                continue;
            }
            if (((Integer) 5).equals(batchInserter.getRelationshipProperties(r.getId()).get(WEIGHT)) && 1 == r.getEndNode()) {
                batchInserter.setRelationshipProperty(r.getId(), WEIGHT, 2);
            }
            if (r.getStartNode() == r.getEndNode()) {
                batchInserter.setRelationshipProperty(r.getId(), WEIGHT, 7);
            }
        }
    }

    protected void setUpTwoNodes() {
        Map<String, Object> props = new HashMap<>();
        props.put(NAME, "One");
        props.put(WEIGHT, 1);
        batchInserter.createNode(1, props);

        props = new HashMap<>();
        props.put(NAME, "Two");
        props.put(WEIGHT, 2);
        batchInserter.createNode(2, props);
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
