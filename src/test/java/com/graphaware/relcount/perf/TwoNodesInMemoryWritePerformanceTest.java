package com.graphaware.relcount.perf;

import com.graphaware.framework.GraphAwareFramework;
import com.graphaware.relcount.simple.module.SimpleRelationshipCountModule;
import com.graphaware.test.TestUtils;
import com.graphaware.tx.executor.NullItem;
import com.graphaware.tx.executor.batch.NoInputBatchTransactionExecutor;
import com.graphaware.tx.executor.batch.UnitOfWork;
import com.graphaware.tx.executor.single.SimpleTransactionExecutor;
import com.graphaware.tx.executor.single.VoidReturningCallback;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

/**
 *
 */
@Ignore
public class TwoNodesInMemoryWritePerformanceTest {


    @Test
    public void noPropertiesPlainDatabase() throws IOException {
        System.out.println("Plain Database:");
        measure(new DatabaseModifier() {
            @Override
            public void alterDatabase(GraphDatabaseService database) {
                //do nothing
            }
        });
    }

    @Test
    public void noPropertiesEmptyFramework() throws IOException {
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
    public void noPropertiesSimpleRelcount() throws IOException {
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
    public void noPropertiesFullRelcount() throws IOException {
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

    private void measure(DatabaseModifier databaseModifier) throws IOException {
        Map<String, String> results = new HashMap<>();

        for (int i = 1; i <= 11; i++) {
            for (int number = 10; number <= 100000; number = number * 10) {
                for (int batchSize = 1; batchSize <= 100000; batchSize = batchSize * 10) {
                    long time = measureCreatingRelationships(databaseModifier, number, batchSize);

                    String key = number + ";" + batchSize + ";";
                    if (!results.containsKey(key)) {
                        results.put(key, "");
                    }
                    results.put(key, results.get(key) + time + ";");
                }
            }
        }

        System.out.println("=== RESULTS ===");
        for (Map.Entry<String, String> entry : results.entrySet()) {
            System.out.println(entry.getKey() + entry.getValue());
        }
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

    private long measureCreatingRelationships(final DatabaseModifier databaseModifier, final int number, final int batchSize) throws IOException {
        TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();

        final GraphDatabaseService database = new GraphDatabaseFactory().newEmbeddedDatabase(temporaryFolder.getRoot().getPath());
        databaseModifier.alterDatabase(database);
        createTwoNodes(database);

        final Node node1 = database.getNodeById(1);
        final Node node2 = database.getNodeById(2);

        long time = TestUtils.time(new TestUtils.Timed() {
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

        System.out.println("Created " + number + " relationships with batch size " + batchSize + " in " + time + " ms");
//        System.out.println(number + ";" + batchSize + ";" + time);

        database.shutdown();
        temporaryFolder.delete();
        return time;
    }

    private interface DatabaseModifier {
        void alterDatabase(GraphDatabaseService database);
    }
}
