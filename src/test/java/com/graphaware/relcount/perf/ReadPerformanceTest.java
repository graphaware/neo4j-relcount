package com.graphaware.relcount.perf;

import com.graphaware.tx.executor.NullItem;
import com.graphaware.tx.executor.batch.NoInputBatchTransactionExecutor;
import com.graphaware.tx.executor.batch.UnitOfWork;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

/**
 *
 */
public abstract class ReadPerformanceTest extends PerformanceTest {
    private static final String CONFIG = "src/test/resources/neo4j-perf.properties";
    public static final int HUNDRED = 100;

    @Test
    public void measure() throws IOException {
        Map<String, String> results = new HashMap<>();

        for (int noRels = 1000; noRels <= 1000000; noRels = noRels * 10) {
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

        GraphDatabaseService database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(temporaryFolder.getRoot().getPath()).loadPropertiesFromFile(CONFIG).newGraphDatabase();
        startFramework(database);

        //create 100 nodes
        new NoInputBatchTransactionExecutor(database, HUNDRED, HUNDRED, new UnitOfWork<NullItem>() {
            @Override
            public void execute(GraphDatabaseService database, NullItem input, int batchNumber, int stepNumber) {
                database.createNode();
            }
        }).execute();

        new NoInputBatchTransactionExecutor(database, 1000, noRels, new UnitOfWork<NullItem>() {
            @Override
            public void execute(GraphDatabaseService database, NullItem input, int batchNumber, int stepNumber) {
                final Node node1 = database.getNodeById(RANDOM.nextInt(HUNDRED) + 1);
                final Node node2 = database.getNodeById(RANDOM.nextInt(HUNDRED) + 1);

                Relationship rel = node1.createRelationshipTo(node2, withName("TEST" + ((1000 * (batchNumber - 1) + stepNumber) % 2)));
                setPropertiesIfNeeded(rel);
            }
        }).execute();

        for (int i = 1; i <= 10; i++) {
            putResult(results, measurePlain(database), "plain;" + noRels + ";");
            putResult(results, measureBruteForce(database), "bruteforce;" + noRels + ";");
            putResult(results, measureNaive(database), "naive;" + noRels + ";");
            putResult(results, measureFull(database), "full;" + noRels + ";");
        }

        System.out.println("=== RESULTS ===");
        for (Map.Entry<String, String> entry : results.entrySet()) {
            System.out.println(entry.getKey() + entry.getValue());
        }

        database.shutdown();
        temporaryFolder.delete();
    }

    protected abstract void startFramework(GraphDatabaseService database);

    protected void setPropertiesIfNeeded(Relationship rel) {
        //none by default
    }

    private void putResult(Map<String, String> results, long time, String key) {
        if (!results.containsKey(key)) {
            results.put(key, "");
        }
        results.put(key, results.get(key) + time + ";");
    }

    protected abstract long measurePlain(GraphDatabaseService database);

    protected abstract long measureBruteForce(GraphDatabaseService database);

    protected abstract long measureNaive(GraphDatabaseService database);

    protected abstract long measureFull(GraphDatabaseService database);
}
