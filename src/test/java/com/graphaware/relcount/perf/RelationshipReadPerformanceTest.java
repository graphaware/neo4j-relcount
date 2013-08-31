package com.graphaware.relcount.perf;

import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class RelationshipReadPerformanceTest extends PerformanceTest {

    @Test
    public void measure() throws IOException {
        Map<String, String> results = new HashMap<>();

        for (int noRels = 10000; noRels <= 1000000; noRels = noRels * 10) {
            measureReadingRelationships(noRels, results);
        }

        System.out.println("=== RESULTS ===");
        for (Map.Entry<String, String> entry : results.entrySet()) {
            System.out.println(entry.getKey() + entry.getValue());
        }

        resultsToFile(results, fileName());
    }

    protected abstract String fileName();

    private void measureReadingRelationships(int noRels, Map<String, String> results) throws IOException {
        TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();

        GraphDatabaseService database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(temporaryFolder.getRoot().getAbsolutePath()).loadPropertiesFromFile(CONFIG).newGraphDatabase();
        startFramework(database);

        createNodes(database, THOUSAND);

        createRelationships(noRels, THOUSAND, database);

        for (int i = 1; i <= 11; i++) {
            putResult(results, measurePlain(database), "plain;" + noRels + ";");
//            putResult(results, measureBruteForce(database), "bruteforce;" + noRels + ";");
//            putResult(results, measureNaive(database), "naive;" + noRels + ";");
            putResult(results, measureCached(database), "cached;" + noRels + ";");
        }

        System.out.println("=== RESULTS ===");
        for (Map.Entry<String, String> entry : results.entrySet()) {
            System.out.println(entry.getKey() + entry.getValue());
        }

        database.shutdown();
        temporaryFolder.delete();
    }


    protected abstract void startFramework(GraphDatabaseService database);


    private void putResult(Map<String, String> results, long time, String key) {
        if (!results.containsKey(key)) {
            results.put(key, "");
        }
        results.put(key, results.get(key) + time + ";");
    }

    protected abstract long measurePlain(GraphDatabaseService database);

    protected abstract long measureBruteForce(GraphDatabaseService database);

    protected abstract long measureNaive(GraphDatabaseService database);

    protected abstract long measureCached(GraphDatabaseService database);
}
