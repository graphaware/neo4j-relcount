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

package com.graphaware.neo4j.relcount.full.dto.property;

import org.neo4j.graphdb.PropertyContainer;

import java.util.Map;

/**
 *
 */
public class CandidateGeneralizedProperties extends BaseCandidateProperties<CandidateProperties> implements CandidateProperties {

    public CandidateGeneralizedProperties(PropertyContainer propertyContainer) {
        super(propertyContainer);
    }

    public CandidateGeneralizedProperties(Map<String, String> properties) {
        super(properties);
    }

    public CandidateGeneralizedProperties(String string) {
        super(string);
    }

    @Override
    protected CandidateProperties self() {
        return this;
    }

    @Override
    protected CandidateProperties newInstance(Map<String, String> props) {
        return new CandidateGeneralizedProperties(props);
    }
}
