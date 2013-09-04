package com.graphaware.relcount.perf;

import com.graphaware.framework.GraphAwareFramework;
import com.graphaware.relcount.full.module.FullRelationshipCountModule;
import com.graphaware.relcount.simple.module.SimpleRelationshipCountModule;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;

import java.io.IOException;

//@Ignore
public class NoPropsCreatePerformanceTest extends RelationshipCreatePerformanceTest {

    @Test
    public void plainDatabase() throws IOException {
        System.out.println("Plain Database:");
        measure(new DatabaseModifier() {
            @Override
            public void alterDatabase(GraphDatabaseService database) {
                //do nothing
            }
        }, "noPropsPlainDatabaseWrite");
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
        }, "noPropsEmptyFrameworkWrite");
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
        }, "noPropsSimpleRelcountWrite");
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
        }, "noPropsFullRelcountWrite");
    }
}
