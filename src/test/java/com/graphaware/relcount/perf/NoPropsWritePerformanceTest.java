package com.graphaware.relcount.perf;

import com.graphaware.framework.GraphAwareFramework;
import com.graphaware.relcount.full.module.FullRelationshipCountModule;
import com.graphaware.relcount.simple.module.SimpleRelationshipCountModule;
import com.graphaware.test.TestUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;

import java.io.IOException;

/**
 *
 */
@Ignore
public class NoPropsWritePerformanceTest extends RelationshipWritePerformanceTest {

    @Test
    public void plainDatabase() throws IOException {
        System.out.println("Plain Database:");
        measure(new DatabaseModifier() {
            @Override
            public void alterDatabase(GraphDatabaseService database) {
                //do nothing
            }
        }, "hundredNodesNoPropsPlainDatabaseWrite");
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
        }, "hundredNodesNoPropsEmptyFrameworkWrite");
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
        }, "hundredNodesNoPropsSimpleRelcountWrite");
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
        }, "hundredNodesNoPropsFullRelcountWrite");
    }

    @Override
    protected long doMeasureCreatingRelationships(final GraphDatabaseService database, final int number, final int batchSize) {
        return TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                createRelationships(number, batchSize, database);
            }
        });
    }
}
