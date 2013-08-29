package com.graphaware.relcount.perf;

import com.graphaware.tx.executor.NullItem;
import com.graphaware.tx.executor.batch.NoInputBatchTransactionExecutor;
import com.graphaware.tx.executor.batch.UnitOfWork;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public abstract class WritePerformanceTest extends PerformanceTest {
    private static final String CONFIG = "src/test/resources/neo4j-perf.properties";

    protected static final int HUNDRED = 100;

    protected void measure(DatabaseModifier databaseModifier, String fileName) throws IOException {
        Map<String, String> results = new HashMap<>();

        for (int i = 1; i <= 3; i++) {
            for (int number = 100000; number <= 100000; number = number * 10) {
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

        resultsToFile(results, fileName);
    }

    private long measureCreatingRelationships(final DatabaseModifier databaseModifier, final int number, final int batchSize) throws IOException {
//        TemporaryFolder temporaryFolder = new TemporaryFolder();
//        temporaryFolder.create();

        final GraphDatabaseService database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder("/tmp/" + System.currentTimeMillis()).loadPropertiesFromFile(CONFIG).newGraphDatabase();
//        final GraphDatabaseService database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(temporaryFolder.getRoot().getPath()).loadPropertiesFromFile(CONFIG).newGraphDatabase();
        databaseModifier.alterDatabase(database);

        createThousandNodes(database);
        doMeasureCreatingRelationships(database, number, batchSize);
        long time = doMeasureCreatingRelationships(database, number, batchSize);

        System.out.println("Created " + number + " relationships with batch size " + batchSize + " in " + time + " ms");
//        System.out.println(number + ";" + batchSize + ";" + time);

        database.shutdown();
//        temporaryFolder.delete();
        return time;
    }

    private void createThousandNodes(GraphDatabaseService databaseService) {
        new NoInputBatchTransactionExecutor(databaseService, HUNDRED, HUNDRED, new UnitOfWork<NullItem>() {
            @Override
            public void execute(GraphDatabaseService database, NullItem input, int batchNumber, int stepNumber) {
                database.createNode();
            }
        }).execute();
    }

    protected abstract long doMeasureCreatingRelationships(final GraphDatabaseService database, final int number, final int batchSize);
}
