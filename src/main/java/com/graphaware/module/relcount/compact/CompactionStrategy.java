/*
 * Copyright (c) 2013-2015 GraphAware
 *
 * This file is part of the GraphAware Framework.
 *
 * GraphAware Framework is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

package com.graphaware.module.relcount.compact;

import com.graphaware.module.relcount.cache.DegreeCachingNode;

/**
 * Strategy for compacting cached degrees.
 * <p/>
 * For example, a node might have the following cached degrees:
 * - FRIEND_OF, OUTGOING, timestamp = 5.4.2013 : 1x
 * - FRIEND_OF, OUTGOING, timestamp = 6.4.2013 : 3x
 * - FRIEND_OF, OUTGOING, timestamp = 7.4.2013 : 5x
 * <p/>
 * The strategy might decide to compact this into:
 * - FRIEND_OF, OUTGOING, timestamp=anything - 9x
 */
public interface CompactionStrategy {

    /**
     * Compact cached degrees if needed.
     *
     * @param node to compact cached degrees for.
     */
    void compactRelationshipCounts(DegreeCachingNode node);
}
