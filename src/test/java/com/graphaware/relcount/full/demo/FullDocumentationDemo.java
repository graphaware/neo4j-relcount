package com.graphaware.relcount.full.demo;

import com.graphaware.framework.GraphAwareFramework;
import com.graphaware.propertycontainer.util.PropertyContainerUtils;
import com.graphaware.relcount.common.counter.RelationshipCounter;
import com.graphaware.relcount.common.counter.UnableToCountException;
import com.graphaware.relcount.common.demo.BaseDocumentationDemo;
import com.graphaware.relcount.full.counter.FullCachedRelationshipCounter;
import com.graphaware.relcount.full.counter.FullNaiveRelationshipCounter;
import com.graphaware.relcount.full.module.FullRelationshipCountModule;
import com.graphaware.relcount.full.strategy.RelationshipCountStrategies;
import com.graphaware.relcount.full.strategy.RelationshipCountStrategiesImpl;
import com.graphaware.relcount.full.strategy.RelationshipPropertiesExtractionStrategy;
import com.graphaware.relcount.full.strategy.RelationshipWeighingStrategy;
import com.graphaware.tx.event.improved.strategy.IncludeAllNodeProperties;
import com.graphaware.tx.event.improved.strategy.RelationshipInclusionStrategy;
import com.graphaware.tx.event.improved.strategy.RelationshipPropertyInclusionStrategy;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.Map;

import static com.graphaware.relcount.common.demo.BaseDocumentationDemo.Rels.FOLLOWS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

/**
 * Demonstration of different full relationship counting options.
 */
public class FullDocumentationDemo extends BaseDocumentationDemo {

    @Test
    public void demonstrateFullCachedRelationshipCounter() {
        GraphAwareFramework framework = new GraphAwareFramework(database);
        FullRelationshipCountModule module = new FullRelationshipCountModule();
        framework.registerModule(module);
        framework.start();

        populateDatabase();

        Node tracy = database.getNodeById(2);

        RelationshipCounter followers = module.cachedCounter(FOLLOWS, INCOMING);
        assertEquals(9, followers.count(tracy));

        RelationshipCounter followersStrength2 = module.cachedCounter(FOLLOWS, INCOMING).with(STRENGTH, 2);
        assertEquals(3, followersStrength2.count(tracy));

        //alternative counter instantiation
        RelationshipCounter following = new FullCachedRelationshipCounter(FOLLOWS, OUTGOING);
        assertEquals(9, following.count(tracy));

        RelationshipCounter followingStrength1 = new FullCachedRelationshipCounter(FOLLOWS, OUTGOING).with(STRENGTH, 1);
        assertEquals(4, followingStrength1.count(tracy));
    }

    @Test
    public void demonstrateFullCachedRelationshipCounterWithCustomThreshold() {
        GraphAwareFramework framework = new GraphAwareFramework(database);

        RelationshipCountStrategies relationshipCountStrategies = RelationshipCountStrategiesImpl.defaultStrategies().with(7);
        FullRelationshipCountModule module = new FullRelationshipCountModule(relationshipCountStrategies);

        framework.registerModule(module);
        framework.start();

        populateDatabase();

        Node tracy = database.getNodeById(2);

        RelationshipCounter followers = module.cachedCounter(FOLLOWS, INCOMING);
        assertEquals(9, followers.count(tracy));

        RelationshipCounter followersStrength2 = module.cachedCounter(FOLLOWS, INCOMING).with(STRENGTH, 2);
        assertEquals(3, followersStrength2.count(tracy));

        RelationshipCounter following = module.cachedCounter(FOLLOWS, OUTGOING);
        assertEquals(9, following.count(tracy));

        RelationshipCounter followingStrength1 = module.cachedCounter(FOLLOWS, OUTGOING).with(STRENGTH, 1);
        assertEquals(4, followingStrength1.count(tracy));
    }

    @Test
    public void demonstrateFullCachedRelationshipCounterWithCustomLowerThreshold() {
        GraphAwareFramework framework = new GraphAwareFramework(database);

        RelationshipCountStrategies relationshipCountStrategies = RelationshipCountStrategiesImpl.defaultStrategies().with(3);
        FullRelationshipCountModule module = new FullRelationshipCountModule(relationshipCountStrategies);

        framework.registerModule(module);
        framework.start();

        populateDatabase();

        Node tracy = database.getNodeById(2);

        RelationshipCounter followers = module.cachedCounter(FOLLOWS, INCOMING);
        assertEquals(9, followers.count(tracy));

        RelationshipCounter followersStrength2 = module.cachedCounter(FOLLOWS, INCOMING).with(STRENGTH, 2);

        try {
            followersStrength2.count(tracy);
            fail();
        } catch (UnableToCountException e) {
            //ok
        }

        RelationshipCounter following = module.cachedCounter(FOLLOWS, OUTGOING);
        assertEquals(9, following.count(tracy));
    }

    @Test
    public void demonstrateFullCachedRelationshipCounterWithCustomThresholdAndWeighingStrategy() {
        GraphAwareFramework framework = new GraphAwareFramework(database);

        RelationshipWeighingStrategy customWeighingStrategy = new RelationshipWeighingStrategy() {
            @Override
            public int getRelationshipWeight(Relationship relationship, Node pointOfView) {
                return (int) relationship.getProperty(STRENGTH, 1);
            }
        };

        RelationshipCountStrategies relationshipCountStrategies = RelationshipCountStrategiesImpl.defaultStrategies()
                .with(7) //threshold
                .with(customWeighingStrategy);

        FullRelationshipCountModule module = new FullRelationshipCountModule(relationshipCountStrategies);

        framework.registerModule(module);
        framework.start();

        populateDatabase();

        Node tracy = database.getNodeById(2);

        RelationshipCounter followers = module.cachedCounter(FOLLOWS, INCOMING);
        assertEquals(12, followers.count(tracy));

        RelationshipCounter followersStrength2 = module.cachedCounter(FOLLOWS, INCOMING).with(STRENGTH, 2);
        assertEquals(6, followersStrength2.count(tracy));

        RelationshipCounter following = module.cachedCounter(FOLLOWS, OUTGOING);
        assertEquals(11, following.count(tracy));

        RelationshipCounter followingStrength1 = module.cachedCounter(FOLLOWS, OUTGOING).with(STRENGTH, 1);
        assertEquals(4, followingStrength1.count(tracy));
    }

    @Test
    public void demonstrateFullCachedRelationshipCounterWithCustomRelationshipInclusionStrategy() {
        GraphAwareFramework framework = new GraphAwareFramework(database);

        RelationshipInclusionStrategy customRelationshipInclusionStrategy = new RelationshipInclusionStrategy() {
            @Override
            public boolean include(Relationship relationship) {
                return relationship.isType(FOLLOWS);
            }
        };

        RelationshipCountStrategies relationshipCountStrategies = RelationshipCountStrategiesImpl.defaultStrategies()
                .with(customRelationshipInclusionStrategy);

        FullRelationshipCountModule module = new FullRelationshipCountModule(relationshipCountStrategies);

        framework.registerModule(module);
        framework.start();

        populateDatabase();

        Node tracy = database.getNodeById(2);

        RelationshipCounter followers = module.cachedCounter(FOLLOWS, INCOMING);
        assertEquals(9, followers.count(tracy));

        RelationshipCounter followersStrength2 = module.cachedCounter(FOLLOWS, INCOMING).with(STRENGTH, 2);
        assertEquals(3, followersStrength2.count(tracy));

        RelationshipCounter following = module.cachedCounter(FOLLOWS, OUTGOING);
        assertEquals(9, following.count(tracy));

        RelationshipCounter followingStrength1 = module.cachedCounter(FOLLOWS, OUTGOING).with(STRENGTH, 1);
        assertEquals(4, followingStrength1.count(tracy));
    }

    @Test
    public void demonstrateFullCachedRelationshipCounterWithCustomRelationshipPropertyInclusionStrategy() {
        GraphAwareFramework framework = new GraphAwareFramework(database);

        RelationshipPropertyInclusionStrategy customRelationshipPropertyInclusionStrategy = new RelationshipPropertyInclusionStrategy() {
            @Override
            public boolean include(String key, Relationship propertyContainer) {
                return !"timestamp".equals(key);
            }
        };

        RelationshipCountStrategies relationshipCountStrategies = RelationshipCountStrategiesImpl.defaultStrategies()
                .with(customRelationshipPropertyInclusionStrategy);

        FullRelationshipCountModule module = new FullRelationshipCountModule(relationshipCountStrategies);

        framework.registerModule(module);
        framework.start();

        populateDatabase();

        Node tracy = database.getNodeById(2);

        RelationshipCounter followers = module.cachedCounter(FOLLOWS, INCOMING);
        assertEquals(9, followers.count(tracy));

        RelationshipCounter followersStrength2 = module.cachedCounter(FOLLOWS, INCOMING).with(STRENGTH, 2);
        assertEquals(3, followersStrength2.count(tracy));

        RelationshipCounter following = module.cachedCounter(FOLLOWS, OUTGOING);
        assertEquals(9, following.count(tracy));

        RelationshipCounter followingStrength1 = module.cachedCounter(FOLLOWS, OUTGOING).with(STRENGTH, 1);
        assertEquals(4, followingStrength1.count(tracy));
    }

    @Test
    public void demonstrateFullCachedRelationshipCounterWithCustomPropertyExtractionStrategy() {
        GraphAwareFramework framework = new GraphAwareFramework(database);

        RelationshipPropertiesExtractionStrategy customPropertiesExtractionStrategy = new RelationshipPropertiesExtractionStrategy() {
            @Override
            public Map<String, String> extractProperties(Relationship relationship, Node pointOfView) {
                //all real properties
                Map<String, String> result = PropertyContainerUtils.propertiesToStringMap(relationship);

                //derived property from the "other" node participating in the relationship
                if (relationship.isType(FOLLOWS)) {
                    result.put(GENDER, relationship.getOtherNode(pointOfView).getProperty(GENDER).toString());
                }

                return result;
            }
        };

        RelationshipCountStrategies relationshipCountStrategies = RelationshipCountStrategiesImpl.defaultStrategies()
                .with(IncludeAllNodeProperties.getInstance()) //no node properties included by default!
                .with(customPropertiesExtractionStrategy);

        FullRelationshipCountModule module = new FullRelationshipCountModule(relationshipCountStrategies);

        framework.registerModule(module);
        framework.start();

        populateDatabase();

        Node tracy = database.getNodeById(2);

        RelationshipCounter maleFollowers = module.cachedCounter(FOLLOWS, INCOMING).with(GENDER, MALE);
        assertEquals(6, maleFollowers.count(tracy));

        RelationshipCounter femaleFollowers = module.cachedCounter(FOLLOWS, INCOMING).with(GENDER, FEMALE);
        assertEquals(3, femaleFollowers.count(tracy));
    }

    @Test
    public void demonstrateFullNaiveRelationshipCounter() {
        populateDatabase();

        FullRelationshipCountModule module = new FullRelationshipCountModule();

        Node tracy = database.getNodeById(2);

        RelationshipCounter followers = module.naiveCounter(FOLLOWS, INCOMING);
        assertEquals(9, followers.count(tracy));

        RelationshipCounter followersStrength2 = module.naiveCounter(FOLLOWS, INCOMING).with(STRENGTH, 2);
        assertEquals(3, followersStrength2.count(tracy));

        //alternative counter instantiation
        RelationshipCounter following = new FullNaiveRelationshipCounter(FOLLOWS, OUTGOING);
        assertEquals(9, following.count(tracy));

        RelationshipCounter followingStrength1 = new FullNaiveRelationshipCounter(FOLLOWS, OUTGOING).with(STRENGTH, 1);
        assertEquals(4, followingStrength1.count(tracy));
    }

    @Test
    public void demonstrateFullFallingBackRelationshipCounterWithCustomLowerThreshold() {
        GraphAwareFramework framework = new GraphAwareFramework(database);

        RelationshipCountStrategies relationshipCountStrategies = RelationshipCountStrategiesImpl.defaultStrategies().with(3);
        FullRelationshipCountModule module = new FullRelationshipCountModule(relationshipCountStrategies);

        framework.registerModule(module);
        framework.start();

        populateDatabase();

        Node tracy = database.getNodeById(2);

        RelationshipCounter followers = module.fallingBackCounter(FOLLOWS, INCOMING);
        assertEquals(9, followers.count(tracy));           //uses cache

        RelationshipCounter followersStrength2 = module.fallingBackCounter(FOLLOWS, INCOMING).with(STRENGTH, 2);
        assertEquals(3, followersStrength2.count(tracy));  //falls back to naive
    }
}
