/*
 * Copyright (c) 2013 GraphAware
 *
 * This file is part of GraphAware.
 *
 * GraphAware is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

package com.graphaware.relcount.full.internal.dto.relationship;

import com.graphaware.propertycontainer.dto.string.relationship.SerializableDirectedRelationship;
import com.graphaware.relcount.full.internal.dto.property.PropertiesQueryDescription;

/**
 * Description of a relationship to be used for relationship counting.
 */
public interface RelationshipQueryDescription extends SerializableDirectedRelationship<PropertiesQueryDescription> {
}
