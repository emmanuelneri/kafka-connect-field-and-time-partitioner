package com.canelmas.kafka.connect;

import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class FieldAndTimeBasedPartitionerTest {

    private static final long PARTITION_DURATION_MS = TimeUnit.HOURS.toHours(1);
    private static final String DATE_PATH_FORMAT = "YYYY-MM-dd";
    private static final DateTimeZone DATE_TIME_ZONE = DateTimeZone.UTC;
    private static final Locale LOCALE = Locale.US;

    @Test
    public void shouldEncodeByFieldAndRecordTimeWhenUseStructType() {
        FieldAndTimeBasedPartitioner<String> partitioner = new FieldAndTimeBasedPartitioner<>();

        List<String> fieldName = Collections.singletonList("name");
        partitioner.init(PARTITION_DURATION_MS, DATE_PATH_FORMAT, LOCALE, DATE_TIME_ZONE, createConfig(fieldName));

        Schema schema = SchemaBuilder.struct()
                .name("record")
                .version(1)
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();

        Struct struct = new Struct(schema)
                .put("id", 1)
                .put("name", "test");

        long recordTimestamp = Timestamp.valueOf(LocalDateTime.of(2020, 4, 20, 10, 0)).getTime();
        SinkRecord sinkRecord = createSinkRecord(schema, struct, recordTimestamp);

        String encodedPartition = partitioner.encodePartition(sinkRecord);
        Assertions.assertEquals("name=test/2020-04-20", encodedPartition);
    }

    @Test
    public void shouldEncodeByFieldAndRecordTimeWhenUseMapType() {
        FieldAndTimeBasedPartitioner<String> partitioner = new FieldAndTimeBasedPartitioner<>();

        List<String> fieldName = Collections.singletonList("name");
        partitioner.init(PARTITION_DURATION_MS, DATE_PATH_FORMAT, LOCALE, DATE_TIME_ZONE, createConfig(fieldName));


        Map<String, Object> record = new HashMap<>();
        record.put("id", 1);
        record.put("name", "test");

        long recordTimestamp = Timestamp.valueOf(LocalDateTime.of(2020, 4, 20, 10, 0)).getTime();
        SinkRecord sinkRecord = createSinkRecord(null, record, recordTimestamp);

        String encodedPartition = partitioner.encodePartition(sinkRecord);
        Assertions.assertEquals("name=test/2020-04-20", encodedPartition);
    }

    private SinkRecord createSinkRecord(Schema valueSchema, Object value, Long timestamp) {
        return new SinkRecord("topic", 0, Schema.STRING_SCHEMA, null, valueSchema, value, 0L, timestamp, TimestampType.CREATE_TIME);
    }

    private Map<String, Object> createConfig(List<String> fieldName) {
        Map<String, Object> config = new HashMap<>();
        config.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, StorageCommonConfig.DIRECTORY_DELIM_DEFAULT);
        config.put(PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "Record");
        config.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, PARTITION_DURATION_MS);
        config.put(PartitionerConfig.PATH_FORMAT_CONFIG, DATE_PATH_FORMAT);
        config.put(PartitionerConfig.LOCALE_CONFIG, LOCALE);
        config.put(PartitionerConfig.TIMEZONE_CONFIG, DATE_TIME_ZONE);
        config.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, fieldName);
        return config;
    }
}