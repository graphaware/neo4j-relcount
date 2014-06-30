package com.graphaware.module.relcount;


import com.graphaware.module.relcount.cache.DegreeCachingStrategy;
import com.graphaware.module.relcount.compact.CompactionStrategy;
import com.graphaware.module.relcount.count.WeighingStrategy;
import com.graphaware.runtime.config.TxDrivenModuleConfiguration;

/**
 * Container for strategies and configuration related to relationship counting.
 */
public interface RelationshipCountConfiguration extends TxDrivenModuleConfiguration {

    /**
     * @return contained caching strategy.
     */
    DegreeCachingStrategy getDegreeCachingStrategy();

    /**
     * @return contained compaction strategy.
     */
    CompactionStrategy getCompactionStrategy();

    /**
     * @return contained relationship weighing strategy.
     */
    WeighingStrategy getWeighingStrategy();
}
