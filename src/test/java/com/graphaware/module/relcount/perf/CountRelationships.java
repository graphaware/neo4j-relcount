package com.graphaware.module.relcount.perf;

import com.graphaware.common.description.relationship.DetachedRelationshipDescription;
import com.graphaware.module.relcount.RelationshipCountConfigurationImpl;
import com.graphaware.module.relcount.RelationshipCountModule;
import com.graphaware.module.relcount.cache.NodePropertiesDegreeCachingStrategy;
import com.graphaware.module.relcount.count.CachedRelationshipCounter;
import com.graphaware.module.relcount.count.LegacyNaiveRelationshipCounter;
import com.graphaware.module.relcount.count.NaiveRelationshipCounter;
import com.graphaware.module.relcount.count.RelationshipCounter;
import com.graphaware.runtime.GraphAwareRuntime;
import com.graphaware.runtime.GraphAwareRuntimeFactory;
import com.graphaware.test.performance.*;
import com.graphaware.tx.executor.NullItem;
import com.graphaware.tx.executor.batch.NoInputBatchTransactionExecutor;
import com.graphaware.tx.executor.batch.UnitOfWork;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.graphaware.common.description.predicate.Predicates.equalTo;
import static com.graphaware.common.description.relationship.RelationshipDescriptionFactory.wildcard;
import static com.graphaware.test.util.TestUtils.Timed;
import static com.graphaware.test.util.TestUtils.time;

/**
 * Performance test for counting relationships.
 */
public class CountRelationships extends RelcountPerformanceTest {

    private static final String DEGREE = "degree";
    private static final String CACHE = "cache";
    private static final String SERIALIZATION = "serialization";

    private static final int NO_NODES = 100;
    private static final int COUNT_NO = 10;

    private int lastAvgDegree = 10;
    private RelationshipCountModule module;

    enum Serialization {
        SINGLE_PROP,
        MULTI_PROP
    }

    enum RuntimeInvolvement {
        NO_FRAMEWORK,
        NAIVE_LEGACY,
        NAIVE,
        CACHED
    }

    private enum Properties {
        NO_PROPS,
        TWO_PROPS,
    }

    @Override
    public String shortName() {
        return "countRelationships";
    }

    @Override
    public String longName() {
        return "Count degree of 10 random nodes";
    }

    @Override
    public List<Parameter> parameters() {
        List<Parameter> result = new LinkedList<>();

        result.add(new EnumParameter(SERIALIZATION, Serialization.class));
        result.add(new CacheParameter(CACHE));
        result.add(new ExponentialParameter(DEGREE, 10, 1, 4, 0.25));
        result.add(new EnumParameter(FW, RuntimeInvolvement.class));
        result.add(new EnumParameter(PROPS, Properties.class));

        return result;
    }

    @Override
    public int dryRuns(Map<String, Object> params) {
        return ((CacheConfiguration) params.get(CACHE)).needsWarmup() ? 10000 : 0;
    }

    @Override
    public int measuredRuns() {
        return 100;
    }

    @Override
    public Map<String, String> databaseParameters(Map<String, Object> params) {
        return ((CacheConfiguration) params.get(CACHE)).addToConfig(Collections.<String, String>emptyMap());
    }

    @Override
    public void prepareDatabase(GraphDatabaseService database, Map<String, Object> params) {
        RelationshipCountConfigurationImpl configuration = RelationshipCountConfigurationImpl.defaultConfiguration();
        if (Serialization.MULTI_PROP.equals(params.get(SERIALIZATION))) {
            configuration = configuration.with(new NodePropertiesDegreeCachingStrategy());
        }

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        module = new RelationshipCountModule(configuration);
        runtime.registerModule(module);
        runtime.start();

        new NoInputBatchTransactionExecutor(database, 1000, NO_NODES, new UnitOfWork<NullItem>() {
            @Override
            public void execute(GraphDatabaseService database, NullItem input, int batchNumber, int stepNumber) {
                database.createNode();
            }
        }).execute();

        int noRelationships = NO_NODES * (int) params.get(DEGREE) / 2;

        new NoInputBatchTransactionExecutor(database, 1000, noRelationships, new UnitOfWork<NullItem>() {
            @Override
            public void execute(GraphDatabaseService database, NullItem input, int batchNumber, int stepNumber) {
                final Node node1 = randomNode(database, NO_NODES);
                final Node node2 = randomNode(database, NO_NODES);
                Relationship relationship = node1.createRelationshipTo(node2, randomType());
                relationship.setProperty("rating", RANDOM.nextInt(2));
                relationship.setProperty("another", RANDOM.nextInt(2));
            }
        }).execute();
    }

    @Override
    public long run(final GraphDatabaseService database, final Map<String, Object> params) {
        long time = 0;

        final AtomicLong result = new AtomicLong(0);
        for (int i = 0; i < COUNT_NO; i++) {

            time += time(new Timed() {
                @Override
                public void time() {
                    RuntimeInvolvement runtimeInvolvement = ((RuntimeInvolvement) params.get(FW));
                    switch (runtimeInvolvement) {
                        case NO_FRAMEWORK:
                            countAsIfThereWasNoRuntime(database, params);
                            break;
                        case CACHED:
                            countUsingRuntime(database, params, new CachedRelationshipCounter(database));
                            break;
                        case NAIVE:
                            countUsingRuntime(database, params, new NaiveRelationshipCounter());
                            break;
                        default:
                            throw new RuntimeException("unknown option");
                    }
                }
            });
        }

        return time;
    }

    private long countAsIfThereWasNoRuntime(final GraphDatabaseService database, Map<String, Object> params) {
        final AtomicLong result = new AtomicLong(0);

        try (Transaction tx = database.beginTx()) {
            final Node node = randomNode(database, NO_NODES);
            if (Properties.TWO_PROPS.equals(params.get(PROPS))) {
                for (Relationship r : node.getRelationships(randomType(), randomDirection())) {
                    if (RANDOM.nextInt(2) == r.getProperty("rating", null) && RANDOM.nextInt(2) == r.getProperty("another", null)) {
                        result.incrementAndGet();
                    }
                }
            } else {
                result.addAndGet(node.getDegree(randomType(), randomDirection()));
            }

            tx.success();
        }

        return result.get();
    }

    protected long countUsingRuntime(final GraphDatabaseService database, Map<String, Object> params, RelationshipCounter counter) {
        int count;

        try (Transaction tx = database.beginTx()) {
            final Node node = randomNode(database, NO_NODES);
            DetachedRelationshipDescription description = wildcard(randomType(), randomDirection());
            if (Properties.TWO_PROPS.equals(params.get(PROPS))) {
                description = description.with("rating", equalTo(RANDOM.nextInt(2))).with("another", equalTo(RANDOM.nextInt(2)));
            }
            count = counter.count(node, description);
            tx.success();
        }

        return count;
    }

    @Override
    public RebuildDatabase rebuildDatabase() {
        return RebuildDatabase.TEST_DECIDES;
    }

    @Override
    public boolean rebuildDatabase(Map<String, Object> params) {
        int degree = (int) params.get(DEGREE);
        boolean result = lastAvgDegree != degree;
        lastAvgDegree = degree;
        return result;
    }
}
