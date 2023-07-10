/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStructOrNull;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.time.Conversions;
import io.debezium.time.MicroTime;
import io.debezium.time.NanoTime;
import io.debezium.time.ZonedTimestamp;

/**
 * Customize the {@link org.apache.kafka.connect.transforms.TimestampConverter} based on debezium data envelope.
 *
 * @param <R> ConnectRecord
 */
public abstract class TimestampConverter<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimestampConverter.class);

    private static final List<String> ENVELOPE_FIELD_NAMES = List.of("before", "after");
    public static final String FIELD_CONFIG = "field";

    public static final String TARGET_TYPE_CONFIG = "target.type";

    public static final String TARGET_TIMEZONE_CONFIG = "target.timezone";
    private static final String TARGET_TIMEZONE_DEFAULT = "UTC";

    public static final String FORMAT_CONFIG = "format";

    public static final String UNIX_PRECISION_CONFIG = "unix.precision";

    private static final String PURPOSE = "converting timestamp formats";

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    public static final Schema OPTIONAL_DATE_SCHEMA = org.apache.kafka.connect.data.Date.builder().optional().schema();
    public static final Schema OPTIONAL_DEBEZIUM_DATE_SCHEMA = io.debezium.time.Date.builder().optional().schema();
    public static final Schema OPTIONAL_TIMESTAMP_SCHEMA = Timestamp.builder().optional().schema();
    public static final Schema OPTIONAL_TIME_SCHEMA = Time.builder().optional().schema();
    public static final Schema OPTION_DEBEZIUM_TIME_SCHEMA = io.debezium.time.Time.builder().optional().schema();
    public static final Schema OPTION_DEBEZIUM_NANO_TIME_SCHEMA = NanoTime.builder().optional().schema();
    public static final Schema OPTION_DEBEZIUM_MICRO_TIME_SCHEMA = MicroTime.builder().optional().schema();

    private static final Field FIELD = Field.create(FIELD_CONFIG)
            .withDisplayName("Field name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withDescription("The field containing the timestamp, or empty if the entire value is a timestamp.");

    private static final Field TARGET_TYPE = Field.create(TARGET_TYPE_CONFIG)
            .withDisplayName("Target type")
            .withEnum(TargetTypeMode.class, TargetTypeMode.TYPE_STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withDescription("The desired timestamp representation: string, unix, Date, Time, or Timestamp.");

    private static final Field TARGET_TIMEZONE = Field.create(TARGET_TIMEZONE_CONFIG)
            .withDisplayName("Target timezone")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDefault(TARGET_TIMEZONE_DEFAULT)
            .withDescription("The desired timezone.");

    private static final Field FORMAT = Field.create(FORMAT_CONFIG)
            .withDisplayName("Format")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("A SimpleDateFormat-compatible format for the timestamp. Used to generate the output when type=string " +
                    "or used to parse the input if the input is a string.");

    private static final Field UNIX_PRECISION = Field.create(UNIX_PRECISION_CONFIG)
            .withDisplayName("Unix precision")
            .withEnum(UnixPrecisionMode.class, UnixPrecisionMode.UNIX_PRECISION_MILLIS)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The desired Unix precision for the timestamp: seconds, milliseconds, microseconds, or nanoseconds. " +
                    "Used to generate the output when type=unix or used to parse the input if the input is a Long." +
                    "Note: This SMT will cause precision loss during conversions from, and to, values with sub-millisecond components.");

    public enum TargetTypeMode implements EnumeratedValue {
        TYPE_STRING("string"),
        TYPE_UNIX("unix"),
        TYPE_DATE("Date"),
        TYPE_TIME("Time"),
        TYPE_TIMESTAMP("Timestamp"),
        TYPE_EPOCH_DAY("epoch_day"),
        TYPE_NANO_DAY("nano_day");

        private final String value;

        TargetTypeMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static TargetTypeMode parse(String value) {
            if (value == null) {
                return null;
            }

            value = value.trim();

            for (TargetTypeMode option : TargetTypeMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }

            return null;
        }
    }

    public enum UnixPrecisionMode implements EnumeratedValue {
        UNIX_PRECISION_SECONDS("seconds"),

        UNIX_PRECISION_MILLIS("milliseconds"),

        UNIX_PRECISION_MICROS("microseconds"),

        UNIX_PRECISION_NANOS("nanoseconds");

        private final String value;

        UnixPrecisionMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static UnixPrecisionMode parse(String value) {
            if (value == null) {
                return null;
            }

            value = value.trim();

            for (UnixPrecisionMode option : UnixPrecisionMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }

            return null;
        }
    }

    private interface TimestampTranslator {
        /**
         * Convert from the type-specific format to the universal java.util.Date format
         */
        Date toRaw(Config config, Object orig);

        /**
         * Get the schema for this format.
         */
        Schema typeSchema(boolean isOptional);

        /**
         * Convert from the universal java.util.Date format to the type-specific format
         */
        Object toType(Config config, Date orig);
    }

    private static final Map<TargetTypeMode, TimestampTranslator> TRANSLATORS = new HashMap<>();

    static {
        TRANSLATORS.put(TargetTypeMode.TYPE_STRING, new TimestampTranslator() {
            @Override
            public Date toRaw(Config config, Object orig) {
                if (!(orig instanceof String)) {
                    throw new DataException("Expected string timestamp to be a String, but found " + orig.getClass());
                }
                try {
                    // Parse iso date-time format string to date based on type of io.debezium.time.ZonedTimestamp
                    return new Date(ZonedDateTime.parse((String) orig).toInstant().toEpochMilli());
                }
                catch (Exception e) {
                    throw new DataException("Could not parse timestamp: value (" + orig + ") does not match pattern ("
                            + ZonedTimestamp.FORMATTER + ")", e);
                }
            }

            @Override
            public Schema typeSchema(boolean isOptional) {
                return isOptional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
            }

            @Override
            public String toType(Config config, Date orig) {
                synchronized (config.format) {
                    return config.format.format(orig);
                }
            }
        });

        TRANSLATORS.put(TargetTypeMode.TYPE_UNIX, new TimestampTranslator() {
            @Override
            public Date toRaw(Config config, Object orig) {
                if (!(orig instanceof Long)) {
                    throw new DataException("Expected Unix timestamp to be a Long, but found " + orig.getClass());
                }
                long unixTime = (Long) orig;
                switch (config.unixPrecision) {
                    case UNIX_PRECISION_SECONDS:
                        return Timestamp.toLogical(Timestamp.SCHEMA, TimeUnit.SECONDS.toMillis(unixTime));
                    case UNIX_PRECISION_MICROS:
                        return Timestamp.toLogical(Timestamp.SCHEMA, TimeUnit.MICROSECONDS.toMillis(unixTime));
                    case UNIX_PRECISION_NANOS:
                        return Timestamp.toLogical(Timestamp.SCHEMA, TimeUnit.NANOSECONDS.toMillis(unixTime));
                    case UNIX_PRECISION_MILLIS:
                    default:
                        return Timestamp.toLogical(Timestamp.SCHEMA, unixTime);
                }
            }

            @Override
            public Schema typeSchema(boolean isOptional) {
                return isOptional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA;
            }

            @Override
            public Long toType(Config config, Date orig) {
                long unixTimeMillis = Timestamp.fromLogical(Timestamp.SCHEMA, orig);
                switch (config.unixPrecision) {
                    case UNIX_PRECISION_SECONDS:
                        return TimeUnit.MILLISECONDS.toSeconds(unixTimeMillis);
                    case UNIX_PRECISION_MICROS:
                        return TimeUnit.MILLISECONDS.toMicros(unixTimeMillis);
                    case UNIX_PRECISION_NANOS:
                        return TimeUnit.MILLISECONDS.toNanos(unixTimeMillis);
                    case UNIX_PRECISION_MILLIS:
                    default:
                        return unixTimeMillis;
                }
            }
        });

        TRANSLATORS.put(TargetTypeMode.TYPE_DATE, new TimestampTranslator() {
            @Override
            public Date toRaw(Config config, Object orig) {
                if (!(orig instanceof Date)) {
                    throw new DataException("Expected Date to be a java.util.Date, but found " + orig.getClass());
                }
                // Already represented as a java.util.Date and Connect Dates are a subset of valid java.util.Date values
                return (Date) orig;
            }

            @Override
            public Schema typeSchema(boolean isOptional) {
                return isOptional ? OPTIONAL_DATE_SCHEMA : org.apache.kafka.connect.data.Date.SCHEMA;
            }

            @Override
            public Date toType(Config config, Date orig) {
                Calendar result = Calendar.getInstance(UTC);
                result.setTime(orig);
                result.set(Calendar.HOUR_OF_DAY, 0);
                result.set(Calendar.MINUTE, 0);
                result.set(Calendar.SECOND, 0);
                result.set(Calendar.MILLISECOND, 0);
                return result.getTime();
            }
        });

        TRANSLATORS.put(TargetTypeMode.TYPE_TIME, new TimestampTranslator() {
            @Override
            public Date toRaw(Config config, Object orig) {
                if (!(orig instanceof Date)) {
                    throw new DataException("Expected Time to be a java.util.Date, but found " + orig.getClass());
                }
                // Already represented as a java.util.Date and Connect Times are a subset of valid java.util.Date values
                return (Date) orig;
            }

            @Override
            public Schema typeSchema(boolean isOptional) {
                return isOptional ? OPTIONAL_TIME_SCHEMA : Time.SCHEMA;
            }

            @Override
            public Date toType(Config config, Date orig) {
                Calendar origCalendar = Calendar.getInstance(UTC);
                origCalendar.setTime(orig);
                Calendar result = Calendar.getInstance(UTC);
                result.setTimeInMillis(0L);
                result.set(Calendar.HOUR_OF_DAY, origCalendar.get(Calendar.HOUR_OF_DAY));
                result.set(Calendar.MINUTE, origCalendar.get(Calendar.MINUTE));
                result.set(Calendar.SECOND, origCalendar.get(Calendar.SECOND));
                result.set(Calendar.MILLISECOND, origCalendar.get(Calendar.MILLISECOND));
                return result.getTime();
            }
        });

        TRANSLATORS.put(TargetTypeMode.TYPE_TIMESTAMP, new TimestampTranslator() {
            @Override
            public Date toRaw(Config config, Object orig) {
                if (!(orig instanceof Date)) {
                    throw new DataException("Expected Timestamp to be a java.util.Date, but found " + orig.getClass());
                }
                return (Date) orig;
            }

            @Override
            public Schema typeSchema(boolean isOptional) {
                return isOptional ? OPTIONAL_TIMESTAMP_SCHEMA : Timestamp.SCHEMA;
            }

            @Override
            public Date toType(Config config, Date orig) {
                return orig;
            }
        });

        TRANSLATORS.put(TargetTypeMode.TYPE_EPOCH_DAY, new TimestampTranslator() {
            @Override
            public Date toRaw(Config config, Object orig) {
                if (!(orig instanceof Integer)) {
                    throw new DataException("Expected Timestamp to be a Integer, but found " + orig.getClass());
                }

                ZonedDateTime zonedDateTime = LocalDate.ofEpochDay((Integer) orig).atStartOfDay(ZoneId.of("UTC"));
                return Date.from(zonedDateTime.toInstant());
            }

            @Override
            public Schema typeSchema(boolean isOptional) {
                return isOptional ? OPTIONAL_DEBEZIUM_DATE_SCHEMA : io.debezium.time.Date.schema();
            }

            @Override
            public Integer toType(Config config, Date orig) {
                LocalDate date = Conversions.toLocalDate(orig);
                return (int) date.toEpochDay();
            }
        });

        TRANSLATORS.put(TargetTypeMode.TYPE_NANO_DAY, new TimestampTranslator() {
            @Override
            public Date toRaw(Config config, Object orig) {
                if (!(orig instanceof Integer || orig instanceof Long)) {
                    throw new DataException("Expected Timestamp to be a Integer/Long, but found " + orig.getClass());
                }

                String timeSchemaName = TIME_SCHEMA_NAME.get();
                if (timeSchemaName == null) {
                    throw new DataException("Expected Time schema name to be set");
                }

                long nanos;
                if (orig instanceof Integer) {
                    nanos = ((Integer) orig).longValue();
                }
                else {
                    nanos = (long) orig;
                }

                if (timeSchemaName.equals(MicroTime.SCHEMA_NAME)) {
                    nanos *= 1_000;
                }
                else if (timeSchemaName.equals(io.debezium.time.Time.SCHEMA_NAME)) {
                    nanos = Duration.ofMillis(nanos).toNanos();
                }

                LocalTime localTime = LocalTime.ofNanoOfDay(nanos);
                OffsetTime offsetTime = OffsetTime.of(localTime, ZoneOffset.UTC);
                Calendar result = Calendar.getInstance(config.format.getTimeZone());
                result.setTimeInMillis(0L);
                result.set(Calendar.HOUR_OF_DAY, offsetTime.getHour());
                result.set(Calendar.MINUTE, offsetTime.getMinute());
                result.set(Calendar.SECOND, offsetTime.getSecond());
                result.set(Calendar.MILLISECOND, (int) Duration.ofNanos(offsetTime.getNano()).toMillis());

                return result.getTime();
            }

            @Override
            public Schema typeSchema(boolean isOptional) {
                String timeSchemaName = TIME_SCHEMA_NAME.get();
                if (timeSchemaName == null) {
                    throw new DataException("Expected Time schema name to be set");
                }
                switch (timeSchemaName) {
                    case io.debezium.time.Time.SCHEMA_NAME:
                        return isOptional ? OPTION_DEBEZIUM_TIME_SCHEMA : io.debezium.time.Time.schema();
                    case MicroTime.SCHEMA_NAME:
                        return isOptional ? OPTION_DEBEZIUM_MICRO_TIME_SCHEMA : MicroTime.schema();
                    case NanoTime.SCHEMA_NAME:
                        return isOptional ? OPTION_DEBEZIUM_NANO_TIME_SCHEMA : NanoTime.schema();
                    default:
                        throw new ConfigException("Not support time type schema: " + timeSchemaName);
                }
            }

            @Override
            public Long toType(Config config, Date orig) {
                throw new ConfigException("Not support convert Date to be a Long of nano day");
            }
        });
    }

    // This is a bit unusual, but allows the transformation config to be passed to static anonymous classes to customize
    // their behavior
    private static class Config {
        Config(String field, TargetTypeMode type, SimpleDateFormat format, UnixPrecisionMode unixPrecision) {
            this.field = field;
            this.type = type;
            this.format = format;
            this.unixPrecision = unixPrecision;
        }

        String field;
        TargetTypeMode type;
        SimpleDateFormat format;
        UnixPrecisionMode unixPrecision;
    }

    private Config config;
    private Cache<Schema, Schema> schemaUpdateCache;
    private SmtManager<R> smtManager;
    private static final ThreadLocal<String> TIME_SCHEMA_NAME = new ThreadLocal<>();

    @Override
    public void configure(Map<String, ?> props) {
        Configuration simpleConfig = Configuration.from(props);
        smtManager = new SmtManager<>(simpleConfig);
        final Field.Set configFields = Field.setOf(FIELD, TARGET_TYPE, TARGET_TIMEZONE, FORMAT, UNIX_PRECISION);

        if (!simpleConfig.validateAndRecord(configFields, LOGGER::error)) {
            throw new ConnectException("Unable to validate config.");
        }

        final String field = simpleConfig.getString(FIELD);
        final TargetTypeMode type = TargetTypeMode.parse(simpleConfig.getString(TARGET_TYPE));
        String formatPattern = simpleConfig.getString(FORMAT);
        final UnixPrecisionMode unixPrecision = UnixPrecisionMode.parse(simpleConfig.getString(UNIX_PRECISION));
        final String timezone = simpleConfig.getString(TARGET_TIMEZONE);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(10000));

        if (type.equals(TargetTypeMode.TYPE_STRING) && Utils.isBlank(formatPattern)) {
            throw new ConfigException("TimestampConverter requires format option to be specified when using string timestamps");
        }
        SimpleDateFormat format = null;
        if (!Utils.isBlank(formatPattern)) {
            try {
                format = new SimpleDateFormat(formatPattern);
                format.setTimeZone(TimeZone.getTimeZone(timezone));
            }
            catch (IllegalArgumentException e) {
                throw new ConfigException("TimestampConverter requires a SimpleDateFormat-compatible pattern for string timestamps: "
                        + formatPattern, e);
            }
        }
        config = new Config(field, type, format, unixPrecision);
    }

    @Override
    public R apply(R record) {
        if (record.value() == null || !smtManager.isValidEnvelope(record)) {
            return record;
        }

        R newRecord = applyWithSchema(record);
        TIME_SCHEMA_NAME.remove();
        return newRecord;
    }

    @Override
    public ConfigDef config() {
        ConfigDef config = new ConfigDef();
        Field.group(config, null, FIELD, TARGET_TYPE, TARGET_TIMEZONE, FORMAT, UNIX_PRECISION);
        return config;
    }

    @Override
    public void close() {
    }

    public static class Key<R extends ConnectRecord<R>> extends TimestampConverter<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends TimestampConverter<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    private R applyWithSchema(R record) {
        final Schema schema = operatingSchema(record);
        final Struct value = requireStructOrNull(operatingValue(record), PURPOSE);
        Schema updatedSchema = schemaUpdateCache.get(schema);
        if (updatedSchema == null) {
            SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
            schema.fields().forEach(field -> {
                if (ENVELOPE_FIELD_NAMES.contains(field.name())) {
                    SchemaBuilder subBuilder = SchemaBuilder.struct().name(field.name());
                    field.schema().fields().forEach(subField -> {
                        if ((record.topic() + "." + subField.name()).endsWith(config.field)) {
                            subBuilder.field(subField.name(), TRANSLATORS.get(config.type).typeSchema(subField.schema().isOptional()));
                        }
                        else {
                            subBuilder.field(subField.name(), subField.schema());
                        }
                    });
                    builder.field(field.name(), subBuilder.schema());
                }
                else {
                    builder.field(field.name(), field.schema());
                }
            });

            if (schema.isOptional()) {
                builder.optional();
            }
            if (schema.defaultValue() != null) {
                Struct updatedDefaultValue = applyValueWithSchema(record.topic(), (Struct) schema.defaultValue(), builder);
                builder.defaultValue(updatedDefaultValue);
            }

            updatedSchema = builder.build();
            schemaUpdateCache.put(schema, updatedSchema);
        }

        Struct updatedValue = applyValueWithSchema(record.topic(), value, updatedSchema);
        return newRecord(record, updatedSchema, updatedValue);
    }

    private Struct applyValueWithSchema(String topic, Struct value, Schema updatedSchema) {
        if (value == null) {
            return null;
        }
        Struct updatedValue = new Struct(updatedSchema);
        value.schema().fields().forEach(field -> {
            final Object updatedFieldValue;
            if (value.get(field) != null && ENVELOPE_FIELD_NAMES.contains(field.name())) {
                updatedFieldValue = new Struct(updatedSchema.schema().field(field.name()).schema());
                Struct originFieldValue = (Struct) value.get(field);
                field.schema().fields().forEach(subField -> {
                    final Object subFieldValue;
                    if ((topic + "." + subField.name()).endsWith(config.field)) {
                        subFieldValue = convertTimestamp(originFieldValue.get(subField), timestampTypeFromSchema(subField.schema()));
                    }
                    else {
                        subFieldValue = originFieldValue.get(subField);
                    }
                    ((Struct) updatedFieldValue).put(subField.name(), subFieldValue);
                });
            }
            else {
                updatedFieldValue = value.get(field);
            }
            if (updatedFieldValue != null) {
                updatedValue.put(field.name(), updatedFieldValue);
            }
        });

        return updatedValue;
    }

    /**
     * Determine the type/format of the timestamp based on the schema
     */
    private TargetTypeMode timestampTypeFromSchema(Schema schema) {
        if (Timestamp.LOGICAL_NAME.equals(schema.name())) {
            return TargetTypeMode.TYPE_TIMESTAMP;
        }
        else if (org.apache.kafka.connect.data.Date.LOGICAL_NAME.equals(schema.name())) {
            return TargetTypeMode.TYPE_DATE;
        }
        else if (Time.LOGICAL_NAME.equals(schema.name())) {
            return TargetTypeMode.TYPE_TIME;
        }
        else if (io.debezium.time.Date.SCHEMA_NAME.equals(schema.name())) {
            return TargetTypeMode.TYPE_EPOCH_DAY;
        }
        else if (io.debezium.time.Time.SCHEMA_NAME.equals(schema.name())
                || NanoTime.SCHEMA_NAME.equals(schema.name())
                || MicroTime.SCHEMA_NAME.equals(schema.name())) {
            TIME_SCHEMA_NAME.set(schema.name());
            return TargetTypeMode.TYPE_NANO_DAY;
        }
        else if (schema.type().equals(Schema.Type.STRING)) {
            // If not otherwise specified, string == user-specified string format for timestamps
            return TargetTypeMode.TYPE_STRING;
        }
        else if (schema.type().equals(Schema.Type.INT64)) {
            // If not otherwise specified, long == unix time
            return TargetTypeMode.TYPE_UNIX;
        }
        throw new ConnectException("Schema " + schema + " does not correspond to a known timestamp type format");
    }

    /**
     * Infer the type/format of the timestamp based on the raw Java type
     */
    private TargetTypeMode inferTimestampType(Object timestamp) {
        // Note that we can't infer all types, e.g. Date/Time/Timestamp all have the same runtime representation as a
        // java.util.Date
        if (timestamp instanceof Date) {
            return TargetTypeMode.TYPE_TIMESTAMP;
        }
        else if (timestamp instanceof Long) {
            return TargetTypeMode.TYPE_UNIX;
        }
        else if (timestamp instanceof String) {
            return TargetTypeMode.TYPE_STRING;
        }
        throw new DataException("TimestampConverter does not support " + timestamp.getClass() + " objects as timestamps");
    }

    /**
     * Convert the given timestamp to the target timestamp format.
     *
     * @param timestamp       the input timestamp, may be null
     * @param timestampFormat the format of the timestamp, or null if the format should be inferred
     * @return the converted timestamp
     */
    private Object convertTimestamp(Object timestamp, TargetTypeMode timestampFormat) {
        if (timestamp == null) {
            return null;
        }
        if (timestampFormat == null) {
            timestampFormat = inferTimestampType(timestamp);
        }

        TimestampTranslator sourceTranslator = TRANSLATORS.get(timestampFormat);
        if (sourceTranslator == null) {
            throw new ConnectException("Unsupported timestamp type: " + timestampFormat);
        }
        Date rawTimestamp = sourceTranslator.toRaw(config, timestamp);

        TimestampTranslator targetTranslator = TRANSLATORS.get(config.type);
        if (targetTranslator == null) {
            throw new ConnectException("Unsupported timestamp type: " + config.type);
        }
        return targetTranslator.toType(config, rawTimestamp);
    }
}
