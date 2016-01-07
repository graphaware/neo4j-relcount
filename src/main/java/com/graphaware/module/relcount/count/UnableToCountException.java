/*
 * Copyright (c) 2013-2016 GraphAware
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

package com.graphaware.module.relcount.count;

/**
 * {@link RuntimeException} indicating that for some reason, relationships could not be counted. For example, when asking
 * for a count purely based on cached values and the cached values are not present (e.g. have been compacted-out).
 */
public class UnableToCountException extends RuntimeException {

    public UnableToCountException() {
    }

    public UnableToCountException(String message) {
        super(message);
    }

    public UnableToCountException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnableToCountException(Throwable cause) {
        super(cause);
    }

    public UnableToCountException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
