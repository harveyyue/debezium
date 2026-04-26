/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

public class DeriveMetadataNewRecordState<R extends ConnectRecord<R>> extends AbstractDeriveNewRecordState<R> {

    private static final String DERIVE_DB_FIELD = "__db";
    private static final String DERIVE_TABLE_FIELD = "__table";

    @Override
    public Schema derivedBeforeOrAfterSchema(Schema originSchema) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct().optional();
        SchemaUtil.copySchemaBasics(originSchema, schemaBuilder);
        for (Field innerField : originSchema.schema().fields()) {
            schemaBuilder.field(innerField.name(), innerField.schema());
        }
        // add db and table metadata field
        schemaBuilder.field(DERIVE_DB_FIELD, Schema.OPTIONAL_STRING_SCHEMA);
        schemaBuilder.field(DERIVE_TABLE_FIELD, Schema.OPTIONAL_STRING_SCHEMA);
        return schemaBuilder.build();
    }

    @Override
    public Struct derivedBeforeOrAfterStruct(Struct originStruct, Struct sourceStruct, Schema derivedSchema) {
        Struct derivedStruct = new Struct(derivedSchema);
        for (Field innerField : originStruct.schema().fields()) {
            derivedStruct.put(innerField.name(), originStruct.get(innerField.name()));
        }
        // add db and table metadata value
        derivedStruct.put(DERIVE_DB_FIELD, sourceStruct.get("db"));
        derivedStruct.put(DERIVE_TABLE_FIELD, sourceStruct.get("table"));
        return derivedStruct;
    }
}
