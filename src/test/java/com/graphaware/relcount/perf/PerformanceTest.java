package com.graphaware.relcount.perf;

import com.graphaware.tx.executor.NullItem;
import com.graphaware.tx.executor.batch.NoInputBatchTransactionExecutor;
import com.graphaware.tx.executor.batch.UnitOfWork;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Random;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

public abstract class PerformanceTest {

    protected static final String CONFIG = "src/test/resources/neo4j-perf.properties";

    protected static final Random RANDOM = new Random(System.currentTimeMillis());

    protected static final int HUNDRED = 100;
    protected static final int THOUSAND = 1000;
    protected static final int TEN_K = 10 * 1000;

    protected void resultsToFile(Map<String, String> results, String fileName) {
        try {
            File file = new File("src/test/resources/perf/" + fileName + "-" + System.currentTimeMillis() + ".txt");
            file.createNewFile();

            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);

            for (Map.Entry<String, String> entry : results.entrySet()) {
                bw.write(entry.getKey() + entry.getValue());
                bw.newLine();
            }

            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected interface DatabaseModifier {
        void alterDatabase(GraphDatabaseService database);
    }

    protected void twoProps(Relationship rel) {
        rel.setProperty("rating", RANDOM.nextInt(4) + 1);
        rel.setProperty("timestamp", RANDOM.nextLong());
    }

    protected void twoMoreProps(Relationship rel) {
        rel.setProperty("3", RANDOM.nextLong());
        rel.setProperty("4", RANDOM.nextLong());
    }

    protected void createNodes(GraphDatabaseService databaseService) {
        new NoInputBatchTransactionExecutor(databaseService, THOUSAND, HUNDRED, new UnitOfWork<NullItem>() {
            @Override
            public void execute(GraphDatabaseService database, NullItem input, int batchNumber, int stepNumber) {
                database.createNode();
            }
        }).execute();
    }

    protected void createRelationships(int noRels, final int batchSize, GraphDatabaseService database) {
        new NoInputBatchTransactionExecutor(database, batchSize, noRels, new UnitOfWork<NullItem>() {
            @Override
            public void execute(GraphDatabaseService database, NullItem input, int batchNumber, int stepNumber) {
                final Node node1 = database.getNodeById(RANDOM.nextInt(HUNDRED) + 1);
                final Node node2 = database.getNodeById(RANDOM.nextInt(HUNDRED) + 1);

                Relationship rel = node1.createRelationshipTo(node2, withName("TEST" + ((batchSize * (batchNumber - 1) + stepNumber) % 2)));
                createRelPropsIfNeeded(rel);
            }
        }).execute();
    }

    protected void deleteRelationships(int noRels, final int batchSize, GraphDatabaseService database) {
        new NoInputBatchTransactionExecutor(database, batchSize, noRels, new UnitOfWork<NullItem>() {
            @Override
            public void execute(GraphDatabaseService database, NullItem input, int batchNumber, int stepNumber) {
                database.getRelationshipById((batchSize * (batchNumber - 1) + stepNumber) - 1).delete();
            }
        }).execute();
    }

    protected void createRelPropsIfNeeded(Relationship rel) {
        //none by default
    }
}
