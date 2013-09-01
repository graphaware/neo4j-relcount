package com.graphaware.relcount.perf;

import com.graphaware.framework.GraphAwareFramework;
import com.graphaware.relcount.full.counter.FullCachedRelationshipCounter;
import com.graphaware.relcount.full.counter.FullNaiveRelationshipCounter;
import com.graphaware.relcount.full.counter.FullRelationshipCounter;
import com.graphaware.relcount.full.module.FullRelationshipCountModule;
import com.graphaware.test.TestUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;

//@Ignore
public class TwoPropsReadPerformanceTest extends RelationshipReadPerformanceTest {

    private static final int COUNT_NO = 1000;

    @Override
    protected long measurePlain(final GraphDatabaseService database) {
        return TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                long result = 0;
                for (int i = 0; i < COUNT_NO; i++) {
                    Node node = database.getNodeById(RANDOM.nextInt(THOUSAND) + 1);
                    for (Relationship r : node.getRelationships(withName("TEST1"), OUTGOING)) {
                        if (r.getProperty("rating") == 2) {
                            result++;
                        }
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
                    if ("TEST1".equals(relationship.getType().name()) && relationship.getProperty("rating") == 2) {
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
                FullRelationshipCounter counter = new FullNaiveRelationshipCounter(withName("TEST1"), OUTGOING).with("rating", 2);
                for (int i = 0; i < COUNT_NO; i++) {
                    Node node = database.getNodeById(RANDOM.nextInt(THOUSAND) + 1);
                    result += counter.count(node);
                }
                System.out.println("Count: " + result);
            }
        });
    }

    @Override
    protected long measureCached(final GraphDatabaseService database) {
        return TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                long result = 0;
                FullRelationshipCounter counter = new FullCachedRelationshipCounter(withName("TEST1"), OUTGOING).with("rating", 2);
                for (int i = 0; i < COUNT_NO; i++) {
                    Node node = database.getNodeById(RANDOM.nextInt(THOUSAND) + 1);
                    result += counter.count(node);
                }
                System.out.println("Count: " + result);
            }
        });
    }

    @Override
    protected void createRelPropsIfNeeded(Relationship rel) {
        twoProps(rel);
    }

    @Override
    protected void startFramework(GraphDatabaseService database) {
        GraphAwareFramework framework = new GraphAwareFramework(database);
        framework.registerModule(new FullRelationshipCountModule());
        framework.start();
    }

    @Override
    protected String fileName() {
        return "hundredNodesTwoPropsReading";
    }
}
