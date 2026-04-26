/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.fest.assertions.Assertions.assertThat;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.data.Envelope;
import io.debezium.pipeline.txmetadata.TransactionMonitor;

public class DeriveMetadataNewRecordStateTest {
    final Schema recordSchema = SchemaBuilder.struct()
            .field("id", Schema.INT8_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .build();

    final Schema sourceSchema = SchemaBuilder.struct()
            .field("lsn", Schema.INT32_SCHEMA)
            .field("ts_ms", Schema.OPTIONAL_INT32_SCHEMA)
            .field("db", Schema.STRING_SCHEMA)
            .field("table", Schema.STRING_SCHEMA)
            .build();

    final Envelope envelope = Envelope.defineSchema()
            .withName("dummy.Envelope")
            .withRecord(recordSchema)
            .withSource(sourceSchema)
            .build();

    private SourceRecord createDeleteRecord() {
        Envelope deleteEnvelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .build();

        final Struct before = new Struct(recordSchema);
        final Struct source = new Struct(sourceSchema);

        before.put("id", (byte) 1);
        before.put("name", "myRecord");
        source.put("lsn", 1234);
        source.put("db", "test_db");
        source.put("table", "test_table");
        final Struct payload = deleteEnvelope.delete(before, source, Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), payload);
    }

    private SourceRecord createTombstoneRecord() {
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", null, null);
    }

    private SourceRecord createCreateRecord() {
        final Struct before = new Struct(recordSchema);
        final Struct source = new Struct(sourceSchema);

        before.put("id", (byte) 1);
        before.put("name", "myRecord");
        source.put("lsn", 1234);
        source.put("ts_ms", 12836);
        source.put("db", "test_db");
        source.put("table", "test_table");
        final Struct payload = envelope.create(before, source, Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), payload);
    }

    private SourceRecord createUpdateRecord() {
        final Struct before = new Struct(recordSchema);
        final Struct after = new Struct(recordSchema);
        final Struct source = new Struct(sourceSchema);
        final Struct transaction = new Struct(TransactionMonitor.TRANSACTION_BLOCK_SCHEMA);

        before.put("id", (byte) 1);
        before.put("name", "myRecord");
        after.put("id", (byte) 1);
        after.put("name", "updatedRecord");
        source.put("lsn", 1234);
        source.put("db", "test_db");
        source.put("table", "test_table");
        transaction.put("id", "571");
        transaction.put("total_order", 42L);
        transaction.put("data_collection_order", 42L);
        final Struct payload = envelope.update(before, after, source, Instant.now());
        payload.put("transaction", transaction);
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), payload);
    }

    @Test
    public void testCreateRecord() {
        try (final DeriveMetadataNewRecordState<SourceRecord> transform = new DeriveMetadataNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecord();
            final SourceRecord derivedRecord = transform.apply(createRecord);

            Struct after = (Struct) ((Struct) derivedRecord.value()).get("after");

            assertThat(after.getInt8("id")).isEqualTo((byte) 1);
            assertThat(after.getString("name")).isEqualTo("myRecord");
            assertThat(after.getString("__db")).isEqualTo("test_db");
            assertThat(after.getString("__table")).isEqualTo("test_table");
        }
    }

    @Test
    public void testUpdateRecord() {
        try (final DeriveMetadataNewRecordState<SourceRecord> transform = new DeriveMetadataNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            final SourceRecord updateRecord = createUpdateRecord();
            final SourceRecord derivedRecord = transform.apply(updateRecord);

            Struct before = (Struct) ((Struct) derivedRecord.value()).get("before");
            Struct after = (Struct) ((Struct) derivedRecord.value()).get("after");

            // assert after
            assertThat(after.getInt8("id")).isEqualTo((byte) 1);
            assertThat(after.getString("name")).isEqualTo("updatedRecord");
            assertThat(after.getString("__db")).isEqualTo("test_db");
            assertThat(after.getString("__table")).isEqualTo("test_table");
            // assert before
            assertThat(before.getString("__db")).isEqualTo("test_db");
            assertThat(before.getString("__table")).isEqualTo("test_table");
        }
    }

    @Test
    public void testDeleteRecord() {
        try (final DeriveMetadataNewRecordState<SourceRecord> transform = new DeriveMetadataNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            final SourceRecord derivedRecord = transform.apply(deleteRecord);

            Struct before = (Struct) ((Struct) derivedRecord.value()).get("before");
            assertThat(before.getString("__db")).isEqualTo("test_db");
            assertThat(before.getString("__table")).isEqualTo("test_table");
        }
    }

    @Test
    public void testTombstoneRecord() {
        try (final DeriveMetadataNewRecordState<SourceRecord> transform = new DeriveMetadataNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            final SourceRecord tombstoneRecord = createTombstoneRecord();
            final SourceRecord derivedRecord = transform.apply(tombstoneRecord);

            assertThat(derivedRecord).isNull();
        }
    }

    @Test
    public void testKeepTombstoneRecord() {
        try (final DeriveMetadataNewRecordState<SourceRecord> transform = new DeriveMetadataNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put("drop.tombstones", "false");
            transform.configure(props);

            final SourceRecord tombstoneRecord = createTombstoneRecord();
            final SourceRecord derivedRecord = transform.apply(tombstoneRecord);

            assertThat(derivedRecord.value()).isNull();
        }
    }
}
