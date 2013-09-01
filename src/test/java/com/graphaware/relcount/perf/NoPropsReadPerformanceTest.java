package com.graphaware.relcount.perf;

import com.graphaware.framework.GraphAwareFramework;
import com.graphaware.relcount.common.counter.RelationshipCounter;
import com.graphaware.relcount.simple.counter.SimpleCachedRelationshipCounter;
import com.graphaware.relcount.simple.counter.SimpleNaiveRelationshipCounter;
import com.graphaware.relcount.simple.module.SimpleRelationshipCountModule;
import com.graphaware.test.TestUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;

//@Ignore
public class NoPropsReadPerformanceTest extends RelationshipReadPerformanceTest {

    private static final int COUNT_NO = 1000000;

    @Override
    protected long measurePlain(GraphDatabaseService database) {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }
//    @Override
//    protected long measurePlain(final GraphDatabaseService database) {
//        long result = 0;
//                long result = 0;
//                for (int i = 0; i < COUNT_NO; i++) {
//                    final Node node = database.getNodeById(RANDOM.nextInt(THOUSAND) + 1);
//                    result += TestUtils.time(new TestUtils.Timed() {
//                        @Override
//                        public void time() {
//                            for (Relationship r : node.getRelationships(withName("TEST1"), OUTGOING)) {
//                                result++;
//                            }
//                        }
//                    })
//                }
//                System.out.println("Count: " + result);
//            }
//    }

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
                RelationshipCounter counter = new SimpleCachedRelationshipCounter(withName("TEST1"), OUTGOING);
                for (int i = 0; i < COUNT_NO; i++) {
                    Node node = database.getNodeById(RANDOM.nextInt(THOUSAND) + 1);
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
