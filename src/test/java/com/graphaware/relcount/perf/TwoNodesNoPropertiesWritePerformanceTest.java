package com.graphaware.relcount.perf;

import com.graphaware.framework.GraphAwareFramework;
import com.graphaware.relcount.simple.module.SimpleRelationshipCountModule;
import com.graphaware.test.TestUtils;
import com.graphaware.tx.executor.NullItem;
import com.graphaware.tx.executor.batch.NoInputBatchTransactionExecutor;
import com.graphaware.tx.executor.batch.UnitOfWork;
import com.graphaware.tx.executor.single.SimpleTransactionExecutor;
import com.graphaware.tx.executor.single.VoidReturningCallback;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import java.io.IOException;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

/**
 *
 */
public class TwoNodesNoPropertiesWritePerformanceTest extends WritePerformanceTest {

    @Test
    public void plainDatabase() throws IOException {
        System.out.println("Plain Database:");
        measure(new DatabaseModifier() {
            @Override
            public void alterDatabase(GraphDatabaseService database) {
                //do nothing
            }
        });
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
        });
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
        });
    }

    @Test
    public void fullRelcount() throws IOException {
        System.out.println("Full Relcount:");
        measure(new DatabaseModifier() {
            @Override
            public void alterDatabase(GraphDatabaseService database) {
                GraphAwareFramework framework = new GraphAwareFramework(database);
                framework.registerModule(new SimpleRelationshipCountModule());
                framework.start();
            }
        });
    }

    private void createTwoNodes(GraphDatabaseService databaseService) {
        new SimpleTransactionExecutor(databaseService).executeInTransaction(new VoidReturningCallback() {
            @Override
            protected void doInTx(GraphDatabaseService database) {
                database.createNode();
                database.createNode();
            }
        });
    }

    @Override
    protected long doMeasureCreatingRelationships(final GraphDatabaseService database, final int number, final int batchSize) {
        createTwoNodes(database);

        final Node node1 = database.getNodeById(1);
        final Node node2 = database.getNodeById(2);

        return TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                new NoInputBatchTransactionExecutor(database, batchSize, number, new UnitOfWork<NullItem>() {
                    @Override
                    public void execute(GraphDatabaseService database, NullItem input, int batchNumber, int stepNumber) {
                        if (((batchSize - 1) * batchNumber + stepNumber) % 2 == 0) {
                            node1.createRelationshipTo(node2, withName("TEST"));
                        } else {
                            node2.createRelationshipTo(node1, withName("TEST"));
                        }
                    }
                }).execute();
            }
        });
    }
}
