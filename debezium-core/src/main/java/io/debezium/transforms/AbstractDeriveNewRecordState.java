/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.util.BoundedConcurrentHashMap;

public abstract class AbstractDeriveNewRecordState<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDeriveNewRecordState.class);

    private final String BEFORE_FIELD = "before";
    private final String AFTER_FIELD = "after";
    private final BoundedConcurrentHashMap<Schema, Schema> schemaUpdateCache = new BoundedConcurrentHashMap<>(10000);
    private SmtManager<R> smtManager;
    private boolean dropTombstones;

    @Override
    public void configure(Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        smtManager = new SmtManager<>(config);
        dropTombstones = config.getBoolean(ExtractNewRecordStateConfigDefinition.DROP_TOMBSTONES);
    }

    @Override
    public R apply(R record) {
        if (record.value() == null) {
            // Ignoring tombstones
            if (dropTombstones) {
                LOGGER.debug("Tombstone message ignored. Message key: \"{}\"", record.key());
                return null;
            }
            return record;
        }

        if (!smtManager.isValidEnvelope(record)) {
            return record;
        }

        // Calculate value schema
        Schema derivedValueSchema = schemaUpdateCache.computeIfAbsent(record.valueSchema(),
                s -> {
                    List<Field> newFields = new ArrayList<>();
                    for (int i = 0; i < s.fields().size(); i++) {
                        Field field = s.fields().get(i);
                        if (field.name().equals(BEFORE_FIELD)) {
                            Schema beforeSchema = derivedBeforeOrAfterSchema(field.schema());
                            newFields.add(new Field(BEFORE_FIELD, i, beforeSchema));
                        }
                        else if (field.name().equals(AFTER_FIELD)) {
                            Schema afterSchema = derivedBeforeOrAfterSchema(field.schema());
                            newFields.add(new Field(AFTER_FIELD, i, afterSchema));
                        }
                        else {
                            newFields.add(field);
                        }
                    }

                    return new ConnectSchema(
                            s.type(),
                            s.isOptional(),
                            s.defaultValue(),
                            s.name(),
                            s.version(),
                            s.doc(),
                            s.parameters(),
                            newFields,
                            null,
                            null);
                });

        // Calculate value struct
        Struct derivedValue = new Struct(derivedValueSchema);
        Struct originalValue = (Struct) record.value();
        Struct sourceStruct = (Struct) originalValue.get("source");
        for (Field field : derivedValueSchema.fields()) {
            if (field.name().equals(BEFORE_FIELD)) {
                Struct beforeStruct = (Struct) originalValue.get(BEFORE_FIELD);
                if (beforeStruct != null) {
                    Struct derivedStruct = derivedBeforeOrAfterStruct(beforeStruct, sourceStruct, field.schema());
                    derivedValue.put(field.name(), derivedStruct);
                }
            }
            else if (field.name().equals(AFTER_FIELD)) {
                Struct afterStruct = (Struct) originalValue.get(AFTER_FIELD);
                if (afterStruct != null) {
                    Struct derivedStruct = derivedBeforeOrAfterStruct(afterStruct, sourceStruct, field.schema());
                    derivedValue.put(field.name(), derivedStruct);
                }
            }
            else {
                derivedValue.put(field.name(), originalValue.get(field.name()));
            }
        }

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                derivedValueSchema,
                derivedValue,
                record.timestamp());
    }

    public abstract Schema derivedBeforeOrAfterSchema(Schema originSchema);

    public abstract Struct derivedBeforeOrAfterStruct(Struct originStruct, Struct sourceStruct, Schema derivedSchema);

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        io.debezium.config.Field.group(config, null, ExtractNewRecordStateConfigDefinition.DROP_TOMBSTONES);
        return config;
    }

    @Override
    public void close() {
    }
}
