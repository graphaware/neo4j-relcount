package com.graphaware.relcount.full.internal.cache;

import com.graphaware.relcount.full.internal.dto.property.CompactiblePropertiesImpl;
import com.graphaware.relcount.full.internal.dto.relationship.CompactibleRelationship;
import com.graphaware.relcount.full.internal.dto.relationship.CompactibleRelationshipImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit test for {@link com.graphaware.relcount.full.internal.cache.AverageCardinalityGeneralizationStrategy}.
 */
public class AverageCardinalityGeneralizationStrategyTest {

    @Test
    public void shouldPreferEliminationOfFrequentlyChangingProperties() {
        Map<CompactibleRelationship, Integer> cachedCounts = new HashMap<>();
        cachedCounts.put(cached("T1#INCOMING#k1#v1#k2#v1"), 1);
        cachedCounts.put(cached("T1#INCOMING#k1#v2#k2#v1"), 1);
        cachedCounts.put(cached("T1#OUTGOING#k1#v3#k2#v2"), 1);
        cachedCounts.put(cached("T1#OUTGOING#k1#v4#k2#v2"), 1);

        List<CompactibleRelationship> result = new AverageCardinalityGeneralizationStrategy().produceGeneralizations(cachedCounts);
        Assert.assertEquals(cached("T1#INCOMING#k1#" + CompactiblePropertiesImpl.ANY_VALUE + "#k2#v1"), result.get(0));
        Assert.assertEquals(cached("T1#OUTGOING#k1#" + CompactiblePropertiesImpl.ANY_VALUE + "#k2#v2"), result.get(1));
        Assert.assertEquals(cached("T1#INCOMING#k1#" + CompactiblePropertiesImpl.ANY_VALUE + "#k2#" + CompactiblePropertiesImpl.ANY_VALUE), result.get(result.size() - 2));
        Assert.assertEquals(cached("T1#OUTGOING#k1#" + CompactiblePropertiesImpl.ANY_VALUE + "#k2#" + CompactiblePropertiesImpl.ANY_VALUE), result.get(result.size() - 1));
    }

    @Test
    public void shouldPreferEliminationOfTypeWithMoreCachedCounts() {
        Map<CompactibleRelationship, Integer> cachedCounts = new HashMap<>();
        cachedCounts.put(cached("T1#OUTGOING#k1#v1#k2#v1"), 1);
        cachedCounts.put(cached("T1#OUTGOING#k1#v2#k2#v1"), 1);
        cachedCounts.put(cached("T2#OUTGOING#k1#v1#k2#v1"), 1);
        cachedCounts.put(cached("T2#OUTGOING#k1#v2#k2#v1"), 1);
        cachedCounts.put(cached("T2#OUTGOING#k1#v3#k2#v2"), 1);
        cachedCounts.put(cached("T2#OUTGOING#k1#v4#k2#v2"), 1);

        List<CompactibleRelationship> result = new AverageCardinalityGeneralizationStrategy().produceGeneralizations(cachedCounts);
        Assert.assertEquals(cached("T2#OUTGOING#k1#" + CompactiblePropertiesImpl.ANY_VALUE + "#k2#v1"), result.get(0));
        Assert.assertEquals(cached("T2#OUTGOING#k1#" + CompactiblePropertiesImpl.ANY_VALUE + "#k2#v2"), result.get(1));
        Assert.assertEquals(cached("T1#OUTGOING#k1#" + CompactiblePropertiesImpl.ANY_VALUE + "#k2#" + CompactiblePropertiesImpl.ANY_VALUE), result.get(result.size() - 1));
    }

    @Test
    public void shouldPreferEliminationOfTypeWithFewerRelationships() {
        Map<CompactibleRelationship, Integer> cachedCounts = new HashMap<>();
        cachedCounts.put(cached("T1#OUTGOING#k1#v1#k2#v1"), 2);
        cachedCounts.put(cached("T1#OUTGOING#k1#v2#k2#v1"), 2);
        cachedCounts.put(cached("T1#OUTGOING#k1#v3#k2#v2"), 2);
        cachedCounts.put(cached("T1#OUTGOING#k1#v4#k2#v2"), 2);
        cachedCounts.put(cached("T2#OUTGOING#k1#v1#k2#v1"), 3);
        cachedCounts.put(cached("T2#OUTGOING#k1#v2#k2#v1"), 3);
        cachedCounts.put(cached("T2#OUTGOING#k1#v3#k2#v2"), 3);
        cachedCounts.put(cached("T2#OUTGOING#k1#v4#k2#v2"), 3);
        cachedCounts.put(cached("T3#OUTGOING#k1#v1#k2#v1"), 1);
        cachedCounts.put(cached("T3#OUTGOING#k1#v2#k2#v1"), 1);
        cachedCounts.put(cached("T3#OUTGOING#k1#v3#k2#v2"), 1);
        cachedCounts.put(cached("T3#OUTGOING#k1#v4#k2#v2"), 1);

        List<CompactibleRelationship> result = new AverageCardinalityGeneralizationStrategy().produceGeneralizations(cachedCounts);
        Assert.assertEquals(cached("T3#OUTGOING#k1#" + CompactiblePropertiesImpl.ANY_VALUE + "#k2#v1"), result.get(0));
        Assert.assertEquals(cached("T3#OUTGOING#k1#" + CompactiblePropertiesImpl.ANY_VALUE + "#k2#v2"), result.get(1));
        Assert.assertEquals(cached("T2#OUTGOING#k1#" + CompactiblePropertiesImpl.ANY_VALUE + "#k2#" + CompactiblePropertiesImpl.ANY_VALUE), result.get(result.size() - 1));
    }

    @Test
    public void shouldPreferEliminationOfAlreadyEliminatedProperties() {
        Map<CompactibleRelationship, Integer> cachedCounts = new HashMap<>();
        cachedCounts.put(cached("T1#OUTGOING#k1#" + CompactiblePropertiesImpl.ANY_VALUE + "#k2#v1"), 2);
        cachedCounts.put(cached("T1#OUTGOING#k1#v3#k2#v2"), 1);
        cachedCounts.put(cached("T1#OUTGOING#k1#v4#k2#v2"), 1);

        List<CompactibleRelationship> result = new AverageCardinalityGeneralizationStrategy().produceGeneralizations(cachedCounts);
        Assert.assertEquals(cached("T1#OUTGOING#k1#" + CompactiblePropertiesImpl.ANY_VALUE + "#k2#v2"), result.get(0));
        Assert.assertEquals(cached("T1#OUTGOING#k1#" + CompactiblePropertiesImpl.ANY_VALUE + "#k2#" + CompactiblePropertiesImpl.ANY_VALUE), result.get(result.size() - 1));
    }

    @Test
    public void shouldTreatMissingAsUndefined() {
        Map<CompactibleRelationship, Integer> cachedCounts = new HashMap<>();
        cachedCounts.put(cached("T1#INCOMING#k2#v1"), 1);
        cachedCounts.put(cached("T1#INCOMING#k1#v2#k2#v1"), 1);
        cachedCounts.put(cached("T1#OUTGOING#k1#v3#k2#v2#k3#v3"), 1);
        cachedCounts.put(cached("T1#OUTGOING#k1#v4#k2#v2"), 1);

        List<CompactibleRelationship> result = new AverageCardinalityGeneralizationStrategy().produceGeneralizations(cachedCounts);
        Assert.assertEquals(cached("T1#INCOMING#k1#" + CompactiblePropertiesImpl.ANY_VALUE + "#k2#v1"), result.get(0));
        Assert.assertEquals(cached("T1#OUTGOING#k1#" + CompactiblePropertiesImpl.ANY_VALUE + "#k2#v2"), result.get(1));
        Assert.assertEquals(cached("T1#OUTGOING#k1#" + CompactiblePropertiesImpl.ANY_VALUE + "#k2#v2#k3#v3"), result.get(2));
        Assert.assertEquals(cached("T1#OUTGOING#k1#" + CompactiblePropertiesImpl.ANY_VALUE + "#k2#" + CompactiblePropertiesImpl.ANY_VALUE + "#k3#" + CompactiblePropertiesImpl.ANY_VALUE), result.get(result.size() - 1));
    }

    private CompactibleRelationshipImpl cached(String s) {
        return new CompactibleRelationshipImpl(s, null, "#");
    }
}
