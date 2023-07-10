package io.debezium.transforms;

import static org.assertj.core.api.Assertions.assertThat;

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
import io.debezium.time.Date;
import io.debezium.time.MicroTime;
import io.debezium.time.NanoTime;
import io.debezium.time.Time;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import io.debezium.transforms.TimestampConverter.Value;

public class TimestampConverterTest {

    protected final static String TOPIC_NAME = "dummy.db.table";
    protected final Schema recordSchema = SchemaBuilder.struct()
            .field("id", Schema.INT8_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .field("dt", Timestamp.schema())
            .field("ts", ZonedTimestamp.builder().optional().schema())
            .field("d", Date.builder().optional().schema())
            .field("t", MicroTime.builder().optional().schema())
            .build();

    protected final Schema sourceSchema = SchemaBuilder.struct()
            .field("lsn", Schema.INT32_SCHEMA)
            .field("ts_ms", Schema.OPTIONAL_INT32_SCHEMA)
            .build();

    protected final Envelope envelope = Envelope.defineSchema()
            .withName(TOPIC_NAME + ".Envelope")
            .withRecord(recordSchema)
            .withSource(sourceSchema)
            .build();

    protected SourceRecord createCreateRecord() {
        final Struct after = new Struct(recordSchema);
        final Struct source = new Struct(sourceSchema);

        after.put("id", (byte) 1);
        after.put("dt", 1688716568000L);
        after.put("ts", "2018-06-20T13:37:03Z");
        after.put("d", 19547);
        after.put("t", 68027000000L);
        source.put("lsn", 1234);
        source.put("ts_ms", 12836);
        final Struct payload = envelope.create(after, source, Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), TOPIC_NAME, envelope.schema(), payload);
    }

    protected SourceRecord createUpdateRecord() {
        final Struct before = new Struct(recordSchema);
        final Struct after = new Struct(recordSchema);
        final Struct source = new Struct(sourceSchema);
        final Struct transaction = new Struct(TransactionMonitor.TRANSACTION_BLOCK_SCHEMA);

        before.put("id", (byte) 1);
        before.put("name", "myRecord");
        before.put("dt", 1688716568000L);
        after.put("id", (byte) 1);
        after.put("name", "updatedRecord");
        after.put("dt", 1688717868900L);
        source.put("lsn", 1234);
        transaction.put("id", "571");
        transaction.put("total_order", 42L);
        transaction.put("data_collection_order", 42L);
        final Struct payload = envelope.update(before, after, source, Instant.now());
        payload.put("transaction", transaction);
        return new SourceRecord(new HashMap<>(), new HashMap<>(), TOPIC_NAME, envelope.schema(), payload);
    }

    protected SourceRecord createDeleteRecord() {
        final Struct before = new Struct(recordSchema);
        final Struct source = new Struct(sourceSchema);

        before.put("id", (byte) 1);
        before.put("dt", 1688716568000L);
        source.put("lsn", 1234);
        source.put("ts_ms", 12836);
        final Struct payload = envelope.delete(before, source, Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), TOPIC_NAME, envelope.schema(), payload);
    }

    @Test
    public void testCreateConvertTimestampEpochToString() {
        try (Value<SourceRecord> transform = new Value()) {
            final Map<String, String> props = new HashMap<>();
            props.put("field", "db.table.dt");
            props.put("target.type", "string");
            props.put("target.timezone", "GMT+8");
            props.put("format", "yyyy-MM-dd HH:mm:ss");
            props.put("unix.precision", "milliseconds");
            transform.configure(props);

            final SourceRecord createdRecord = createCreateRecord();
            final SourceRecord transformRecord = transform.apply(createdRecord);
            final Struct after = ((Struct) transformRecord.value()).getStruct("after");
            assertThat(after.get("dt")).isEqualTo("2023-07-07 15:56:08");
        }
    }

    @Test
    public void testCreateConvertTimestampUtcStringToString() {
        try (Value<SourceRecord> transform = new Value()) {
            final Map<String, String> props = new HashMap<>();
            props.put("field", "db.table.ts");
            props.put("target.type", "string");
            props.put("target.timezone", "GMT+8");
            props.put("format", "yyyy-MM-dd HH:mm:ss");
            transform.configure(props);

            final SourceRecord createdRecord = createCreateRecord();
            final SourceRecord transformRecord = transform.apply(createdRecord);
            final Struct after = ((Struct) transformRecord.value()).getStruct("after");
            assertThat(after.get("ts")).isEqualTo("2018-06-20 21:37:03");
        }
    }

    @Test
    public void testCreateConvertDateEpochDayToString() {
        try (Value<SourceRecord> transform = new Value()) {
            final Map<String, String> props = new HashMap<>();
            props.put("field", "db.table.d");
            props.put("target.type", "string");
            props.put("target.timezone", "GMT+8");
            props.put("format", "yyyy-MM-dd");
            transform.configure(props);

            final SourceRecord createdRecord = createCreateRecord();
            final SourceRecord transformRecord = transform.apply(createdRecord);
            final Struct after = ((Struct) transformRecord.value()).getStruct("after");
            assertThat(after.get("d")).isEqualTo("2023-07-09");
        }
    }

    @Test
    public void testCreateConvertMicroTimeToString() {
        try (Value<SourceRecord> transform = new Value()) {
            final Map<String, String> props = new HashMap<>();
            props.put("field", "db.table.t");
            props.put("target.type", "string");
            props.put("target.timezone", "GMT+8");
            props.put("format", "HH:mm:ss.S");
            transform.configure(props);

            final SourceRecord createdRecord = createCreateRecord();
            final SourceRecord transformRecord = transform.apply(createdRecord);
            final Struct after = ((Struct) transformRecord.value()).getStruct("after");
            assertThat(after.get("t")).isEqualTo("18:53:47.0");
        }
    }

    @Test
    public void testCreateConvertNanoTimeToString() {
        Schema timestampSchema = SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("dt", Timestamp.schema())
                .field("ts", ZonedTimestamp.builder().optional().schema())
                .field("d", Date.builder().optional().schema())
                .field("t", NanoTime.builder().optional().schema())
                .build();
        Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(timestampSchema)
                .withSource(sourceSchema)
                .build();

        final Struct after = new Struct(timestampSchema);
        final Struct source = new Struct(sourceSchema);

        after.put("id", (byte) 1);
        after.put("dt", 1688716568000L);
        after.put("ts", "2018-06-20T13:37:03Z");
        after.put("d", 19547);
        after.put("t", 45030817724000L);
        source.put("lsn", 1234);
        source.put("ts_ms", 12836);
        final Struct payload = envelope.create(after, source, Instant.now());

        try (Value<SourceRecord> transform = new Value()) {
            final Map<String, String> props = new HashMap<>();
            props.put("field", "db.table.t");
            props.put("target.type", "string");
            props.put("target.timezone", "GMT+8");
            props.put("format", "HH:mm:ss.S");
            transform.configure(props);

            final SourceRecord createdRecord = new SourceRecord(new HashMap<>(), new HashMap<>(), TOPIC_NAME, envelope.schema(), payload);
            final SourceRecord transformRecord = transform.apply(createdRecord);
            final Struct afterStruct = ((Struct) transformRecord.value()).getStruct("after");
            assertThat(afterStruct.get("t")).isEqualTo("12:30:30.817");
        }
    }

    @Test
    public void testCreateConvertTimeToString() {
        Schema timestampSchema = SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("dt", Timestamp.schema())
                .field("ts", ZonedTimestamp.builder().optional().schema())
                .field("d", Date.builder().optional().schema())
                .field("t", Time.builder().optional().schema())
                .build();
        Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(timestampSchema)
                .withSource(sourceSchema)
                .build();

        final Struct after = new Struct(timestampSchema);
        final Struct source = new Struct(sourceSchema);

        after.put("id", (byte) 1);
        after.put("dt", 1688716568000L);
        after.put("ts", "2018-06-20T13:37:03Z");
        after.put("d", 19547);
        after.put("t", 45030817);
        source.put("lsn", 1234);
        source.put("ts_ms", 12836);
        final Struct payload = envelope.create(after, source, Instant.now());

        try (Value<SourceRecord> transform = new Value()) {
            final Map<String, String> props = new HashMap<>();
            props.put("field", "db.table.t");
            props.put("target.type", "string");
            props.put("target.timezone", "GMT+8");
            props.put("format", "HH:mm:ss.S");
            transform.configure(props);

            final SourceRecord createdRecord = new SourceRecord(new HashMap<>(), new HashMap<>(), TOPIC_NAME, envelope.schema(), payload);
            final SourceRecord transformRecord = transform.apply(createdRecord);
            final Struct afterStruct = ((Struct) transformRecord.value()).getStruct("after");
            assertThat(afterStruct.get("t")).isEqualTo("12:30:30.817");
        }
    }

    @Test
    public void testUpdateConvertTimestampEpochToString() {
        try (Value<SourceRecord> transform = new Value()) {
            final Map<String, String> props = new HashMap<>();
            props.put("field", "db.table.dt");
            props.put("target.type", "string");
            props.put("target.timezone", "GMT+8");
            props.put("format", "yyyy-MM-dd HH:mm:ss.S");
            props.put("unix.precision", "milliseconds");
            transform.configure(props);

            final SourceRecord updatedRecord = createUpdateRecord();
            final SourceRecord transformRecord = transform.apply(updatedRecord);
            final Struct before = ((Struct) transformRecord.value()).getStruct("before");
            final Struct after = ((Struct) transformRecord.value()).getStruct("after");
            assertThat(before.get("dt")).isEqualTo("2023-07-07 15:56:08.0");
            assertThat(after.get("dt")).isEqualTo("2023-07-07 16:17:48.900");
        }
    }

    @Test
    public void testDeleteConvertTimestampEpochToString() {
        try (Value<SourceRecord> transform = new Value()) {
            final Map<String, String> props = new HashMap<>();
            props.put("field", "db.table.dt");
            props.put("target.type", "string");
            props.put("target.timezone", "UTC");
            props.put("format", "yyyy-MM-dd HH:mm:ss.S");
            props.put("unix.precision", "milliseconds");
            transform.configure(props);

            final SourceRecord deletedRecord = createDeleteRecord();
            final SourceRecord transformRecord = transform.apply(deletedRecord);
            final Struct before = ((Struct) transformRecord.value()).getStruct("before");
            assertThat(before.get("dt")).isEqualTo("2023-07-07 07:56:08.0");
        }
    }
}
