package com.graphaware.relcount.perf;

import com.graphaware.framework.GraphAwareFramework;
import com.graphaware.relcount.module.RelationshipCountModule;
import com.graphaware.relcount.module.RelationshipCountStrategiesImpl;
import com.graphaware.tx.event.improved.strategy.IncludeNoRelationshipProperties;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;

import java.io.IOException;

@Ignore
public class TwoPropsNoCompactWritePerformanceTest extends RelationshipCreatePerformanceTest {

    @Test
    public void plainDatabase() throws IOException {
        System.out.println("Plain Database:");
        measure(new DatabaseModifier() {
            @Override
            public void alterDatabase(GraphDatabaseService database) {
                //do nothing
            }
        }, "twoPropsPlainDatabaseWriteNoCompaction");
    }

    @Test
    public void simpleRelcount() throws IOException {
        System.out.println("Simple Relcount:");
        measure(new DatabaseModifier() {
            @Override
            public void alterDatabase(GraphDatabaseService database) {
                GraphAwareFramework framework = new GraphAwareFramework(database);
                framework.registerModule(new RelationshipCountModule(RelationshipCountStrategiesImpl.defaultStrategies().with(IncludeNoRelationshipProperties.getInstance())));
                framework.start();
            }
        }, "twoPropsSimpleRelcountWriteNoCompaction");
    }

    @Test
    public void fullRelcount() throws IOException {
        System.out.println("Full Relcount:");
        measure(new DatabaseModifier() {
            @Override
            public void alterDatabase(GraphDatabaseService database) {
                GraphAwareFramework framework = new GraphAwareFramework(database);
                framework.registerModule(new RelationshipCountModule());
                framework.start();
            }
        }, "twoPropsFullRelcountWriteNoCompaction");
    }

    @Override
    protected void createRelPropsIfNeeded(Relationship rel) {
        rel.setProperty("rating", RANDOM.nextInt(2));
        rel.setProperty("another", RANDOM.nextInt(2));
    }
}
