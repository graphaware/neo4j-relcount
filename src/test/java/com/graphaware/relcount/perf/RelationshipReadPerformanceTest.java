package com.graphaware.relcount.perf;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public abstract class RelationshipReadPerformanceTest extends PerformanceTest {

    @Test
    public void measure() throws IOException {
        Map<String, String> results = new LinkedHashMap<>();

        //avg degree: 10 to 10,000
        for (double i = 1; i <= 4; i += 0.25) {
            measureReadingRelationships((int) (Math.pow(10, i) * 100 / 2), results);
        }

        results.clear();

        for (double i = 1; i <= 4; i += 0.25) {
            measureReadingRelationships((int) (Math.pow(10, i) * 100 / 2), results);
        }

        System.out.println("=== RESULTS ===");
        for (Map.Entry<String, String> entry : results.entrySet()) {
            System.out.println(entry.getKey() + entry.getValue());
        }

        resultsToFile(results, fileName());
    }

    protected abstract String fileName();

    protected abstract String dbFolder();

    private void measureReadingRelationships(int noRels, Map<String, String> results) throws IOException {
//        TemporaryFolder temporaryFolder = new TemporaryFolder();
//        temporaryFolder.create();

        GraphDatabaseService database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder("/tmp/reading/" + dbFolder() + "/" + noRels).loadPropertiesFromFile(CONFIG).newGraphDatabase();
        startFramework(database);

//        createNodes(database);

//        createRelationships(noRels, THOUSAND, database);

        //warm cache
        for (int i = 1; i <= 10000; i++) {
//            measurePlain(database);
            measureCached(database);
        }

        for (int i = 1; i <= 100; i++) {
//            putResult(results, measurePlain(database), "plain;" + noRels + ";");
            putResult(results, measureCached(database), "cached;" + noRels + ";");
        }

        System.out.println("=== RESULTS ===");
        for (Map.Entry<String, String> entry : results.entrySet()) {
            System.out.println(entry.getKey() + entry.getValue());
        }

        database.shutdown();
//        temporaryFolder.delete();
    }


    protected abstract void startFramework(GraphDatabaseService database);


    private void putResult(Map<String, String> results, long time, String key) {
        if (!results.containsKey(key)) {
            results.put(key, "");
        }
        results.put(key, results.get(key) + time + ";");
    }

    protected abstract long measurePlain(GraphDatabaseService database);

    protected abstract long measureCached(GraphDatabaseService database);
}
