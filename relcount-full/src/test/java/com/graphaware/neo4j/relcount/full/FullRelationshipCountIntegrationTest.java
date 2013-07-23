package com.graphaware.neo4j.relcount.full;

import com.graphaware.neo4j.framework.GraphAwareFramework;
import com.graphaware.neo4j.relcount.common.IntegrationTest;
import com.graphaware.neo4j.relcount.full.api.FullCachedRelationshipCounter;
import com.graphaware.neo4j.relcount.full.api.FullNaiveRelationshipCounter;
import com.graphaware.neo4j.relcount.full.api.FullRelationshipCounter;
import com.graphaware.neo4j.relcount.full.module.FullRelationshipCountModule;
import com.graphaware.neo4j.relcount.full.strategy.RelationshipCountStrategiesImpl;
import com.graphaware.neo4j.relcount.full.strategy.RelationshipWeighingStrategy;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import static com.graphaware.neo4j.relcount.common.IntegrationTest.RelationshipTypes.ONE;
import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Direction.*;

/**
 *
 */
@SuppressWarnings("PointlessArithmeticExpression")
public class FullRelationshipCountIntegrationTest extends IntegrationTest {

    @Test
    public void noFramework() {
        setUpTwoNodes();
        simulateUsage();

        CounterCreator naiveCounterCreator = new CounterCreator() {
            @Override
            public FullRelationshipCounter createCounter(RelationshipType type, Direction direction) {
                return new FullNaiveRelationshipCounter(type, direction);
            }
        };

        CounterCreator cachedCounterCreator = new CounterCreator() {
            @Override
            public FullRelationshipCounter createCounter(RelationshipType type, Direction direction) {
                return new FullCachedRelationshipCounter(type, direction);
            }
        };

        verifyCounts(1, naiveCounterCreator);
        verifyCounts(0, cachedCounterCreator);
    }

    @Test
    public void noFramework2() {
        setUpTwoNodes();
        simulateUsage();
        simulateUsage();

        CounterCreator naiveCounterCreator = new CounterCreator() {
            @Override
            public FullRelationshipCounter createCounter(RelationshipType type, Direction direction) {
                return new FullNaiveRelationshipCounter(type, direction);
            }
        };

        CounterCreator cachedCounterCreator = new CounterCreator() {
            @Override
            public FullRelationshipCounter createCounter(RelationshipType type, Direction direction) {
                return new FullCachedRelationshipCounter(type, direction);
            }
        };

        verifyCounts(2, naiveCounterCreator);
        verifyCounts(0, cachedCounterCreator);
    }

    @Test
    public void defaultFrameworkOnNewDatabase() {
        GraphAwareFramework framework = new GraphAwareFramework(database);
        final FullRelationshipCountModule module = new FullRelationshipCountModule();
        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateUsage();

        verifyCounts(1, naiveCounterCreator(module));
        verifyCounts(1, cachedCounterCreator(module));
    }

    @Test
    public void defaultFrameworkOnExistingDatabase() {
        setUpTwoNodes();
        simulateUsage();

        GraphAwareFramework framework = new GraphAwareFramework(database);
        final FullRelationshipCountModule module = new FullRelationshipCountModule();
        framework.registerModule(module);
        framework.start();

        verifyCounts(1, naiveCounterCreator(module));
        verifyCounts(1, cachedCounterCreator(module));
    }

    @Test
    public void customFrameworkOnNewDatabase() {
        GraphAwareFramework framework = new GraphAwareFramework(database, new CustomConfig());
        final FullRelationshipCountModule module = new FullRelationshipCountModule();
        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateUsage();

        verifyCounts(1, naiveCounterCreator(module));
        verifyCounts(1, cachedCounterCreator(module));
    }

    @Test
    public void customFrameworkOnExistingDatabase() {
        setUpTwoNodes();
        simulateUsage();

        GraphAwareFramework framework = new GraphAwareFramework(database, new CustomConfig());
        final FullRelationshipCountModule module = new FullRelationshipCountModule();
        framework.registerModule(module);
        framework.start();

        verifyCounts(1, naiveCounterCreator(module));
        verifyCounts(1, cachedCounterCreator(module));
    }

    @Test
    public void weightedRelationships() {
        GraphAwareFramework framework = new GraphAwareFramework(database, new CustomConfig());
        final FullRelationshipCountModule module = new FullRelationshipCountModule(
                RelationshipCountStrategiesImpl.defaultStrategies()
                        .with(new RelationshipWeighingStrategy() {
                            @Override
                            public int getRelationshipWeight(Relationship relationship, Node pointOfView) {
                                return (int) relationship.getProperty(WEIGHT, 1);
                            }
                        }));

        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateUsage();

        verifyWeightedCounts(1, naiveCounterCreator(module));
        verifyWeightedCounts(1, cachedCounterCreator(module));
    }

    @Test
    public void weightedRelationships2() {
        GraphAwareFramework framework = new GraphAwareFramework(database, new CustomConfig());
        final FullRelationshipCountModule module = new FullRelationshipCountModule(
                RelationshipCountStrategiesImpl.defaultStrategies()
                        .with(new RelationshipWeighingStrategy() {
                            @Override
                            public int getRelationshipWeight(Relationship relationship, Node pointOfView) {
                                return (int) relationship.getProperty(WEIGHT, 1);
                            }
                        }));

        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateUsage();
        simulateUsage();

        verifyWeightedCounts(2, naiveCounterCreator(module));
        verifyWeightedCounts(2, cachedCounterCreator(module));
    }

    @Test
    public void weightedRelationships3() {
        GraphAwareFramework framework = new GraphAwareFramework(database, new CustomConfig());
        final FullRelationshipCountModule module = new FullRelationshipCountModule(
                RelationshipCountStrategiesImpl.defaultStrategies()
                        .with(new RelationshipWeighingStrategy() {
                            @Override
                            public int getRelationshipWeight(Relationship relationship, Node pointOfView) {
                                return (int) relationship.getProperty(WEIGHT, 1);
                            }
                        }));

        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateUsage();
        simulateUsage();
        simulateUsage();

        verifyWeightedCounts(3, naiveCounterCreator(module));
        verifyWeightedCounts(3, cachedCounterCreator(module));
    }

    @Test
    public void weightedRelationships10() {
        GraphAwareFramework framework = new GraphAwareFramework(database, new CustomConfig());
        final FullRelationshipCountModule module = new FullRelationshipCountModule(
                RelationshipCountStrategiesImpl.defaultStrategies()
                        .with(new RelationshipWeighingStrategy() {
                            @Override
                            public int getRelationshipWeight(Relationship relationship, Node pointOfView) {
                                return (int) relationship.getProperty(WEIGHT, 1);
                            }
                        }));

        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateUsage();
        simulateUsage();
        simulateUsage();
        simulateUsage();
        simulateUsage();
        simulateUsage();
        simulateUsage();
        simulateUsage();
        simulateUsage();
        simulateUsage();

        verifyWeightedCounts(10, naiveCounterCreator(module));
        verifyWeightedCounts(10, cachedCounterCreator(module));
    }

    @Test
    public void fallbackWithCompaction() {
        GraphAwareFramework framework = new GraphAwareFramework(database, new CustomConfig());
        final FullRelationshipCountModule module = new FullRelationshipCountModule(
                RelationshipCountStrategiesImpl.defaultStrategies().with(5)
        );
        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateUsage();

        verifyCounts(1, fallbackCounterCreator(module));
    }

    @Test
    public void fallbackWithCompaction2() {
        GraphAwareFramework framework = new GraphAwareFramework(database);
        final FullRelationshipCountModule module = new FullRelationshipCountModule(
                RelationshipCountStrategiesImpl.defaultStrategies().with(3)
        );
        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateUsage();
        simulateUsage();

        verifyCounts(2, fallbackCounterCreator(module));
    }

    @Test
    public void fallbackWithCompaction3() {
        GraphAwareFramework framework = new GraphAwareFramework(database, new CustomConfig());
        final FullRelationshipCountModule module = new FullRelationshipCountModule(
                RelationshipCountStrategiesImpl.defaultStrategies().with(8)
        );
        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateUsage();
        simulateUsage();
        simulateUsage();

        verifyCounts(3, fallbackCounterCreator(module));
    }

    @Test
    public void fallbackWithCompactionAndWeighting() {
        GraphAwareFramework framework = new GraphAwareFramework(database, new CustomConfig());
        final FullRelationshipCountModule module = new FullRelationshipCountModule(
                RelationshipCountStrategiesImpl.defaultStrategies()
                        .with(new RelationshipWeighingStrategy() {
                            @Override
                            public int getRelationshipWeight(Relationship relationship, Node pointOfView) {
                                return (int) relationship.getProperty(WEIGHT, 1);
                            }
                        })
                        .with(10));

        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateUsage();
        simulateUsage();
        simulateUsage();
        simulateUsage();

        verifyWeightedCounts(4, fallbackCounterCreator(module));
    }

    //helpers

    private CounterCreator naiveCounterCreator(final FullRelationshipCountModule module) {
        return new CounterCreator() {
            @Override
            public FullRelationshipCounter createCounter(RelationshipType type, Direction direction) {
                return module.naiveCounter(type, direction);
            }
        };
    }

    private CounterCreator cachedCounterCreator(final FullRelationshipCountModule module) {
        return new CounterCreator() {
            @Override
            public FullRelationshipCounter createCounter(RelationshipType type, Direction direction) {
                return module.cachedCounter(type, direction);
            }
        };
    }

    private CounterCreator fallbackCounterCreator(final FullRelationshipCountModule module) {
        return new CounterCreator() {
            @Override
            public FullRelationshipCounter createCounter(RelationshipType type, Direction direction) {
                return module.fallingBackCounter(type, direction);
            }
        };
    }

    private void verifyCounts(int factor, CounterCreator counterCreator) {
        Node one = database.getNodeById(1);
        Node two = database.getNodeById(2);

        //Node one incoming

        assertEquals(3 * factor, counterCreator.createCounter(ONE, INCOMING).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).countLiterally(one));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 1).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 1).countLiterally(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 3).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 3).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 4).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 4).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 5).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 5).countLiterally(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 7).count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 7).countLiterally(one));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V2").countLiterally(one));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V1").countLiterally(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V2").countLiterally(one));

        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V2").count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V2").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V1").countLiterally(one));

        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).with(K1, "V1").count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).with(K1, "V1").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).with(K2, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).with(K2, "V2").countLiterally(one));

        //Node one outgoing

        assertEquals(7 * factor, counterCreator.createCounter(ONE, OUTGOING).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).countLiterally(one));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 1).count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 1).countLiterally(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 3).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 3).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 4).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 4).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 5).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 5).countLiterally(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 7).count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 7).countLiterally(one));

        assertEquals(5 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").count(one));
        assertEquals(3 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V2").countLiterally(one));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V1").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V2").countLiterally(one));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V2").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V1").countLiterally(one));

        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).with(K1, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).with(K1, "V1").countLiterally(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 1).with(K1, "V1").count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 1).with(K1, "V1").countLiterally(one));

        //Node one both

        assertEquals(10 * factor, counterCreator.createCounter(ONE, BOTH).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).countLiterally(one));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).countLiterally(one));
        assertEquals(2 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 3).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 3).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 4).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 4).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 5).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 5).countLiterally(one));
        assertEquals(2 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 7).count(one));
        assertEquals(2 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 7).countLiterally(one));

        assertEquals(7 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").count(one));
        assertEquals(3 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V2").countLiterally(one));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V1").countLiterally(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V2").countLiterally(one));

        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V2").count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V2").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V1").countLiterally(one));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K1, "V1").count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K1, "V1").countLiterally(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).with(K1, "V1").count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).with(K1, "V1").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K2, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K2, "V2").countLiterally(one));

        //Node two outgoing

        assertEquals(2 * factor, counterCreator.createCounter(ONE, OUTGOING).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).countLiterally(two));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 1).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 1).countLiterally(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 3).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 3).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 4).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 4).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 5).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 5).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 7).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 7).countLiterally(two));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V2").countLiterally(two));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V1").countLiterally(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V2").countLiterally(two));

        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V2").count(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V2").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V1").countLiterally(two));

        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).with(K1, "V1").count(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).with(K1, "V1").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).with(K2, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).with(K2, "V2").countLiterally(two));

        //Node two incoming

        assertEquals(6 * factor, counterCreator.createCounter(ONE, INCOMING).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).countLiterally(two));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 1).count(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 1).countLiterally(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 3).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 3).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 4).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 4).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 5).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 5).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 7).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 7).countLiterally(two));

        assertEquals(5 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").count(two));
        assertEquals(3 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V2").countLiterally(two));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V1").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V2").countLiterally(two));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V2").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V1").countLiterally(two));

        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).with(K1, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).with(K1, "V1").countLiterally(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 1).with(K1, "V1").count(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 1).with(K1, "V1").countLiterally(two));

        //Node two both

        assertEquals(8 * factor, counterCreator.createCounter(ONE, BOTH).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).countLiterally(two));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).count(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).countLiterally(two));
        assertEquals(2 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 3).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 3).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 4).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 4).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 5).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 5).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 7).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 7).countLiterally(two));

        assertEquals(7 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").count(two));
        assertEquals(3 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V2").countLiterally(two));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V1").countLiterally(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V2").countLiterally(two));

        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V2").count(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V2").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V1").countLiterally(two));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K1, "V1").count(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K1, "V1").countLiterally(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).with(K1, "V1").count(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).with(K1, "V1").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K2, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K2, "V2").countLiterally(two));
    }

    private void verifyWeightedCounts(int factor, CounterCreator counterCreator) {
        Node one = database.getNodeById(1);
        Node two = database.getNodeById(2);

        //Node one incoming

        assertEquals(10 * factor, counterCreator.createCounter(ONE, INCOMING).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).countLiterally(one));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 1).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 1).countLiterally(one));
        assertEquals(2 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 3).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 3).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 4).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 4).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 5).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 5).countLiterally(one));
        assertEquals(7 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 7).count(one));
        assertEquals(7 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 7).countLiterally(one));

        assertEquals(3 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V2").countLiterally(one));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V1").countLiterally(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V2").countLiterally(one));

        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V2").count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V2").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V1").countLiterally(one));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).with(K1, "V1").count(one));
        assertEquals(2 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).with(K1, "V1").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).with(K2, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).with(K2, "V2").countLiterally(one));

        //Node one outgoing

        assertEquals(14 * factor, counterCreator.createCounter(ONE, OUTGOING).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).countLiterally(one));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 1).count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 1).countLiterally(one));
        assertEquals(2 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 3).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 3).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 4).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 4).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 5).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 5).countLiterally(one));
        assertEquals(7 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 7).count(one));
        assertEquals(7 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 7).countLiterally(one));

        assertEquals(6 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").count(one));
        assertEquals(3 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V2").countLiterally(one));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V1").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V2").countLiterally(one));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V2").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V1").countLiterally(one));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).with(K1, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).with(K1, "V1").countLiterally(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 1).with(K1, "V1").count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 1).with(K1, "V1").countLiterally(one));

        //Node one both

        assertEquals(24 * factor, counterCreator.createCounter(ONE, BOTH).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).countLiterally(one));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).countLiterally(one));
        assertEquals(4 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 3).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 3).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 4).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 4).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 5).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 5).countLiterally(one));
        assertEquals(14 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 7).count(one));
        assertEquals(14 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 7).countLiterally(one));

        assertEquals(9 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").count(one));
        assertEquals(3 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V2").countLiterally(one));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V1").countLiterally(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V2").countLiterally(one));

        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V2").count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V2").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V1").countLiterally(one));

        assertEquals(4 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K1, "V1").count(one));
        assertEquals(2 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K1, "V1").countLiterally(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).with(K1, "V1").count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).with(K1, "V1").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K2, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K2, "V2").countLiterally(one));

        //Node two outgoing

        assertEquals(3 * factor, counterCreator.createCounter(ONE, OUTGOING).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).countLiterally(two));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 1).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 1).countLiterally(two));
        assertEquals(2 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 3).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 3).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 4).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 4).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 5).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 5).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 7).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 7).countLiterally(two));

        assertEquals(3 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V2").countLiterally(two));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V1").countLiterally(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V2").countLiterally(two));

        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V2").count(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V2").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V1").countLiterally(two));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).with(K1, "V1").count(two));
        assertEquals(2 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).with(K1, "V1").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).with(K2, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).with(K2, "V2").countLiterally(two));

        //Node two incoming

        assertEquals(7 * factor, counterCreator.createCounter(ONE, INCOMING).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).countLiterally(two));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 1).count(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 1).countLiterally(two));
        assertEquals(2 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 3).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 3).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 4).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 4).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 5).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 5).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 7).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 7).countLiterally(two));

        assertEquals(6 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").count(two));
        assertEquals(3 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V2").countLiterally(two));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V1").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V2").countLiterally(two));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V2").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V1").countLiterally(two));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).with(K1, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).with(K1, "V1").countLiterally(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 1).with(K1, "V1").count(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 1).with(K1, "V1").countLiterally(two));

        //Node two both

        assertEquals(10 * factor, counterCreator.createCounter(ONE, BOTH).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).countLiterally(two));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).count(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).countLiterally(two));
        assertEquals(4 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 3).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 3).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 4).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 4).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 5).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 5).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 7).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 7).countLiterally(two));

        assertEquals(9 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").count(two));
        assertEquals(3 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V2").countLiterally(two));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V1").countLiterally(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V2").countLiterally(two));

        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V2").count(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V2").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V1").countLiterally(two));

        assertEquals(4 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K1, "V1").count(two));
        assertEquals(2 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K1, "V1").countLiterally(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).with(K1, "V1").count(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).with(K1, "V1").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K2, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K2, "V2").countLiterally(two));
    }

    interface CounterCreator {
        FullRelationshipCounter createCounter(RelationshipType type, Direction direction);
    }
}
