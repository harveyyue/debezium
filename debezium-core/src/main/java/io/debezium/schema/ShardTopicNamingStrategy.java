/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;

/**
 * A topic naming strategy that consolidates sharded database tables into unified topic names.
 */
@Incubating
public class ShardTopicNamingStrategy extends AbstractTopicNamingStrategy<DataCollectionId> {

    public static final Field TOPIC_SHARD_SUFFIX = Field.create("topic.shard.suffix")
            .withDisplayName("Topic shard suffix")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault("_all_shards")
            .withValidation(AbstractTopicNamingStrategy::validateTopicName)
            .withDescription("The suffix name for shard topics, default value is _all_shards");

    private String topicShardSuffix;

    public ShardTopicNamingStrategy(Properties props) {
        super(props);
    }

    @Override
    public void configure(Properties props) {
        super.configure(props);
        Configuration config = Configuration.from(props);
        topicShardSuffix = config.getString(TOPIC_SHARD_SUFFIX);
    }

    @Override
    public String dataChangeTopic(DataCollectionId id) {
        return topicNames.computeIfAbsent(id, t -> {
            List<String> databaseParts = id.databaseParts();
            String databaseName = removeTailDigitOrUnderscore(databaseParts.get(0));
            String tableName = removeTailDigitOrUnderscore(databaseParts.get(1));
            if (!tableName.equals(databaseParts.get(1))) {
                // skip the table name end with "_": my_table_v5_region_b_
                if (databaseParts.get(1).endsWith("_")) {
                    tableName = databaseParts.get(1);
                }
                else {
                    tableName += topicShardSuffix;
                }
            }
            return sanitizedTopicName(mkString(Collect.arrayListOf(prefix, databaseName, tableName), delimiter));
        });
    }

    private String removeTailDigitOrUnderscore(String raw) {
        String reversed = new StringBuilder(raw).reverse().toString();
        char[] chars = reversed.toCharArray();
        StringBuilder result = new StringBuilder();
        boolean stop = false;
        for (char c : chars) {
            if ((c >= '0' && c <= '9') || c == '_') {
                if (stop) {
                    result.append(c);
                }
            }
            else {
                result.append(c);
                stop = true;
            }
        }
        return result.reverse().toString();
    }
}
