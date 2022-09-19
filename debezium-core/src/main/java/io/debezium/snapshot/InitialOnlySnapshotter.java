/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.snapshot;

import io.debezium.common.annotation.Incubating;

@Incubating
public class InitialOnlySnapshotter extends AbstractSnapshotter {

    @Override
    public boolean shouldStream() {
        return false;
    }
}
