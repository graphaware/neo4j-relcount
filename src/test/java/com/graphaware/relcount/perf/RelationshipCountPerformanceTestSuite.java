package com.graphaware.relcount.perf;

import com.graphaware.performance.PerformanceTest;
import com.graphaware.performance.PerformanceTestSuite;

/**
 * Performance test suite for relationship count module.
 */
public class RelationshipCountPerformanceTestSuite extends PerformanceTestSuite {

    @Override
    protected PerformanceTest[] getPerfTests() {
        return new PerformanceTest[]{
                new CountRelationshipsNaive()
//                new CreateRelationships() ,
//                new CountRelationships()
        };
    }
}
