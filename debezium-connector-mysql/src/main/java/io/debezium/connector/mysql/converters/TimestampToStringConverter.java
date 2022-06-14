/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.converters;

import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import io.debezium.time.Conversions;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.Collect;

public class TimestampToStringConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private static final List<Integer> TIMESTAMP_FAMILY = Collect.arrayListOf(Types.DATE, Types.TIME, Types.TIMESTAMP,
            Types.TIMESTAMP_WITH_TIMEZONE);
    private static final String EPOCH_TIMESTAMP = "1970-01-01 00:00:00";

    public static final String DATETIME_FORMAT = "datetime.format";
    public static final String DATETIME_FORMAT_DEFAULT = "yyyy-MM-dd HH:mm:ss";
    public static final String DATE_FORMAT = "date.format";
    public static final String DATE_FORMAT_DEFAULT = "yyyy-MM-dd";
    public static final String TIME_FORMAT = "time.format";
    public static final String TIME_FORMAT_DEFAULT = "HH:mm:ss";
    public static final String TIMEZONE = "timezone";
    public static final String TIMEZONE_DEFAULT = "UTC";

    private DateTimeFormatter datetimeFormat;
    private DateTimeFormatter dateFormat;
    private DateTimeFormatter timeFormat;
    private String timezone;

    @Override
    public void configure(Properties props) {
        String datetimeFormatPattern = props.getProperty(DATETIME_FORMAT, DATETIME_FORMAT_DEFAULT);
        String dateFormatPattern = props.getProperty(DATE_FORMAT, DATE_FORMAT_DEFAULT);
        String timeFormatPattern = props.getProperty(TIME_FORMAT, TIME_FORMAT_DEFAULT);
        timezone = props.getProperty(TIMEZONE, TIMEZONE_DEFAULT);

        datetimeFormat = DateTimeFormatter.ofPattern(datetimeFormatPattern);
        dateFormat = DateTimeFormatter.ofPattern(dateFormatPattern);
        timeFormat = DateTimeFormatter.ofPattern(timeFormatPattern);
    }

    public void converterFor(RelationalColumn field, ConverterRegistration<SchemaBuilder> registration) {
        if (!TIMESTAMP_FAMILY.contains(field.jdbcType())) {
            return;
        }

        registration.register(SchemaBuilder.string(), value -> {
            if (value == null) {
                if (field.isOptional()) {
                    return null;
                } else if (field.hasDefaultValue()) {
                    return field.defaultValue();
                }
                return null;
            }
            switch (field.jdbcType()) {
                case Types.DATE:
                    if (value instanceof Integer) {
                        return LocalDate.ofEpochDay(((Integer) value).longValue()).format(dateFormat);
                    }
                    return ((LocalDate) value).format(dateFormat);
                case Types.TIME:
                    Duration duration;
                    if (value instanceof Long) {
                        duration = Duration.ofNanos(((Long) value).longValue() * 1_000);
                    }
                    else {
                        duration = (Duration) value;
                    }
                    LocalTime localTime = LocalTime.of(duration.toHoursPart(), duration.toMinutesPart(),
                            duration.toSecondsPart(), duration.toNanosPart());
                    return localTime.format(timeFormat);
                case Types.TIMESTAMP:
                    if (value instanceof Long && (Long) value == 0L) {
                        return LocalDateTime.from(Conversions.timestampFormat(field.length().orElse(-1)).parse(EPOCH_TIMESTAMP))
                                .format(datetimeFormat);
                    }
                    if (value instanceof Timestamp) {
                        return ((Timestamp) value).toLocalDateTime().format(datetimeFormat);
                    }
                    return ((LocalDateTime) value).format(datetimeFormat);
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    if (value instanceof String) {
                        return ZonedDateTime
                                .from(ZonedTimestamp.FORMATTER.parse((String) value))
                                .withZoneSameInstant(ZoneId.of(timezone))
                                .format(datetimeFormat);
                    }
                    return ((ZonedDateTime) value).withZoneSameInstant(ZoneId.of(timezone)).format(datetimeFormat);
                default:
                    return null;
            }
        });
    }
}
