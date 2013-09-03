package com.graphaware.relcount.perf;

import com.graphaware.framework.GraphAwareFramework;
import com.graphaware.relcount.common.counter.RelationshipCounter;
import com.graphaware.relcount.simple.counter.SimpleCachedRelationshipCounter;
import com.graphaware.relcount.simple.module.SimpleRelationshipCountModule;
import com.graphaware.test.TestUtils;
import org.neo4j.graphdb.*;

import java.util.concurrent.atomic.AtomicLong;

import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;

//@Ignore
public class NoPropsReadPerformanceTest extends RelationshipReadPerformanceTest {

    private static final int COUNT_NO = 10;

    @Override
    protected String dbFolder() {
        return "noprops";
    }

    @Override
    protected long measurePlain(final GraphDatabaseService database) {
        long time = 0;
        final AtomicLong result = new AtomicLong(0);

        for (int i = 0; i < COUNT_NO; i++) {
            final Node node = database.getNodeById(RANDOM.nextInt(HUNDRED) + 1);
            final Direction direction = RANDOM.nextBoolean() ? INCOMING : OUTGOING;
            final RelationshipType type = withName("TEST" + RANDOM.nextInt(2));

            time += TestUtils.time(new TestUtils.Timed() {
                @Override
                public void time() {
                    for (Relationship r : node.getRelationships(type, direction)) {
                        result.incrementAndGet();
                    }
                }
            });
        }

        System.out.println("Count: " + result);
        return time;
    }

    @Override
    protected long measureCached(final GraphDatabaseService database) {
        long time = 0;
        final AtomicLong result = new AtomicLong(0);

        for (int i = 0; i < COUNT_NO; i++) {
            final Direction direction = RANDOM.nextBoolean() ? INCOMING : OUTGOING;
            final RelationshipType type = withName("TEST" + RANDOM.nextInt(2));
            final RelationshipCounter counter = new SimpleCachedRelationshipCounter(type, direction);

            final Node node = database.getNodeById(RANDOM.nextInt(HUNDRED) + 1);
            time += TestUtils.time(new TestUtils.Timed() {
                @Override
                public void time() {
                    result.set(result.get() + counter.count(node));
                }
            });
        }

        System.out.println("Count: " + result);
        return time;
    }

    @Override
    protected void startFramework(GraphDatabaseService database) {
        GraphAwareFramework framework = new GraphAwareFramework(database);
        framework.registerModule(new SimpleRelationshipCountModule());
        framework.start();
    }

    @Override
    protected String fileName() {
        return "noPropsReading-disk";
    }
}
