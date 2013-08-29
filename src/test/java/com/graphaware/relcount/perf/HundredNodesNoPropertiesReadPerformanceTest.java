package com.graphaware.relcount.perf;

import com.graphaware.framework.GraphAwareFramework;
import com.graphaware.relcount.common.counter.RelationshipCounter;
import com.graphaware.relcount.simple.counter.SimpleCachedRelationshipCounter;
import com.graphaware.relcount.simple.counter.SimpleNaiveRelationshipCounter;
import com.graphaware.relcount.simple.module.SimpleRelationshipCountModule;
import com.graphaware.test.TestUtils;
import org.junit.Ignore;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;

/**
 *
 */
@Ignore
public class HundredNodesNoPropertiesReadPerformanceTest extends ReadPerformanceTest {

    @Override
    protected long measurePlain(final GraphDatabaseService database) {
        return TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                long result = 0;
                for (Node node : GlobalGraphOperations.at(database).getAllNodes()) {
                    for (Relationship r : node.getRelationships(withName("TEST1"), OUTGOING)) {
                        result++;
                    }
                }
                System.out.println("Count: " + result);
            }
        });
    }

    @Override
    protected long measureBruteForce(final GraphDatabaseService database) {
        return TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                long result = 0;
                for (Relationship relationship : GlobalGraphOperations.at(database).getAllRelationships()) {
                    if ("TEST1".equals(relationship.getType().name())) {
                        result++;
                    }
                }
                System.out.println("Count: " + result);
            }
        });
    }

    @Override
    protected long measureNaive(final GraphDatabaseService database) {
        return TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                long result = 0;
                RelationshipCounter counter = new SimpleNaiveRelationshipCounter(withName("TEST1"), OUTGOING);
                for (Node node : GlobalGraphOperations.at(database).getAllNodes()) {
                    result += counter.count(node);
                }
                System.out.println("Count: " + result);
            }
        });
    }

    @Override
    protected long measureFull(final GraphDatabaseService database) {
        return TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                long result = 0;
                RelationshipCounter counter = new SimpleCachedRelationshipCounter(withName("TEST1"), OUTGOING);
                for (Node node : GlobalGraphOperations.at(database).getAllNodes()) {
                    result += counter.count(node);
                }
                System.out.println("Count: " + result);
            }
        });
    }

    @Override
    protected void startFramework(GraphDatabaseService database) {
        GraphAwareFramework framework = new GraphAwareFramework(database);
        framework.registerModule(new SimpleRelationshipCountModule());
        framework.start();
    }

    @Override
    protected String fileName() {
        return "hundredNodesNoPropsReading";
    }
}
