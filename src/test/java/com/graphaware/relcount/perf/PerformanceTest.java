package com.graphaware.relcount.perf;

import org.neo4j.graphdb.GraphDatabaseService;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Random;

/**
 *
 */
public abstract class PerformanceTest {

    protected static final String CONFIG = "src/test/resources/neo4j-perf.properties";
    protected static final Random RANDOM = new Random(51823591465L);

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
}
