/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.AbstractPartition;
import io.debezium.util.Strings;

public class OraclePartition extends AbstractPartition implements Partition {

    public OraclePartition(String serverName, String databaseName) {
        super(serverName, databaseName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final OraclePartition other = (OraclePartition) obj;
        return Objects.equals(serverName, other.serverName);
    }

    @Override
    public int hashCode() {
        return serverName.hashCode();
    }

    @Override
    public String toString() {
        return "OraclePartition [sourcePartition=" + getSourcePartition() + "]";
    }

    static class Provider implements Partition.Provider<OraclePartition> {
        private final OracleConnectorConfig connectorConfig;

        Provider(OracleConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Set<OraclePartition> getPartitions() {
            final String databaseName = Strings.isNullOrBlank(connectorConfig.getPdbName())
                    ? connectorConfig.getDatabaseName()
                    : connectorConfig.getPdbName();
            return Collections.singleton(new OraclePartition(connectorConfig.getLogicalName(), databaseName));
        }
    }
}
