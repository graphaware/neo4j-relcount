package com.graphaware.relcount.perf;

import com.graphaware.framework.GraphAwareFramework;
import com.graphaware.relcount.full.module.FullRelationshipCountModule;
import com.graphaware.relcount.simple.module.SimpleRelationshipCountModule;
import com.graphaware.test.TestUtils;
import com.graphaware.tx.executor.NullItem;
import com.graphaware.tx.executor.batch.NoInputBatchTransactionExecutor;
import com.graphaware.tx.executor.batch.UnitOfWork;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.io.IOException;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

/**
 *
 */
public class ThousandNodesTwoRelationshipPropertiesWritePerformanceTest extends WritePerformanceTest {

    private static final int THOUSAND = 1000;

    @Test
    public void plainDatabase() throws IOException {
        System.out.println("Plain Database:");
        measure(new DatabaseModifier() {
            @Override
            public void alterDatabase(GraphDatabaseService database) {
                //do nothing
            }
        }, "thousandNodesTwoRelationshipPropsPlainDatabase");
    }

    @Test
    public void emptyFramework() throws IOException {
        System.out.println("Empty Framework:");
        measure(new DatabaseModifier() {
            @Override
            public void alterDatabase(GraphDatabaseService database) {
                GraphAwareFramework framework = new GraphAwareFramework(database);
                framework.start();
            }
        }, "thousandNodesTwoRelationshipPropsEmptyFramework");
    }

    @Test
    public void simpleRelcount() throws IOException {
        System.out.println("Simple Relcount:");
        measure(new DatabaseModifier() {
            @Override
            public void alterDatabase(GraphDatabaseService database) {
                GraphAwareFramework framework = new GraphAwareFramework(database);
                framework.registerModule(new SimpleRelationshipCountModule());
                framework.start();
            }
        }, "thousandNodesTwoRelationshipPropsSimpleRelcount");
    }

    @Test
    public void fullRelcount() throws IOException {
        System.out.println("Full Relcount:");
        measure(new DatabaseModifier() {
            @Override
            public void alterDatabase(GraphDatabaseService database) {
                GraphAwareFramework framework = new GraphAwareFramework(database);
                framework.registerModule(new FullRelationshipCountModule());
                framework.start();
            }
        }, "thousandNodesTwoRelationshipPropsFullRelcount");
    }

    private void createThousandNodes(GraphDatabaseService databaseService) {
        new NoInputBatchTransactionExecutor(databaseService, THOUSAND, THOUSAND, new UnitOfWork<NullItem>() {
            @Override
            public void execute(GraphDatabaseService database, NullItem input, int batchNumber, int stepNumber) {
                database.createNode();
            }
        }).execute();
    }

    @Override
    protected long doMeasureCreatingRelationships(final GraphDatabaseService database, final int number, final int batchSize) {
        createThousandNodes(database);

        return TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                new NoInputBatchTransactionExecutor(database, batchSize, number, new UnitOfWork<NullItem>() {
                    @Override
                    public void execute(GraphDatabaseService database, NullItem input, int batchNumber, int stepNumber) {
                        final Node node1 = database.getNodeById(RANDOM.nextInt(THOUSAND) + 1);
                        final Node node2 = database.getNodeById(RANDOM.nextInt(THOUSAND) + 1);

                        Relationship rel = node1.createRelationshipTo(node2, withName("TEST"));
                        rel.setProperty("rating", RANDOM.nextInt(5) + 1);
                        rel.setProperty("timestamp", RANDOM.nextLong());
                    }
                }).execute();
            }
        });
    }
}