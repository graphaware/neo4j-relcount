package com.graphaware.relcount.perf;

import com.graphaware.test.TestUtils;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class RelationshipWritePerformanceTest extends PerformanceTest {
    private static final String CONFIG = "src/test/resources/neo4j-perf.properties";

    protected void measure(DatabaseModifier databaseModifier, String fileName) throws IOException {
        Map<String, String> results = new HashMap<>();

        for (int i = 1; i <= 10; i++) {
//            for (int batchSize = 1; batchSize <= THOUSAND; batchSize = batchSize * 10) {
            for (double j = 0; j <= 3; j += 0.25) {
                int batchSize = (int) (Math.round(Math.pow(10, j)));
                long time = measureCreatingRelationships(databaseModifier, TEN_K, batchSize);

                String key = TEN_K + ";" + batchSize + ";";
                if (!results.containsKey(key)) {
                    results.put(key, "");
                }
                results.put(key, results.get(key) + time + ";");
            }
        }

        System.out.println("=== RESULTS ===");
        for (Map.Entry<String, String> entry : results.entrySet()) {
            System.out.println(entry.getKey() + entry.getValue());
        }

        resultsToFile(results, fileName);
    }

    private long measureCreatingRelationships(final DatabaseModifier databaseModifier, final int number, final int batchSize) throws IOException {
        TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();

        final GraphDatabaseService database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(temporaryFolder.getRoot().getPath()).loadPropertiesFromFile(CONFIG).newGraphDatabase();
        databaseModifier.alterDatabase(database);

        createNodes(database);

        long time = TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                createRelationships(number, batchSize, database);
            }
        });

        System.out.println("Created " + number + " relationships with batch size " + batchSize + " in " + time + " microseconds");

        database.shutdown();
        temporaryFolder.delete();
        return time;
    }
}
