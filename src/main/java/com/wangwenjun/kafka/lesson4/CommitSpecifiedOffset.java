package com.wangwenjun.kafka.lesson4;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class CommitSpecifiedOffset
{

    private static final Logger LOG = LoggerFactory.getLogger(CommitSpecifiedOffset.class);


    public static void main(String[] args)
    {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(loadProp());
        consumer.subscribe(Collections.singletonList("test12"));
        Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
        TopicPartition tp = new TopicPartition("test12", 1);
        OffsetAndMetadata om = new OffsetAndMetadata(14, "no meta data");
        offset.put(tp, om);

        for (; ; )
        {
            ConsumerRecords<String, String> records = consumer.poll(100);
            records.forEach(record ->
            {
                //biz handler.
                LOG.info("Partition:{},offset:{}", record.partition(), record.offset());
            });
            /**
             * 使用场景：如果在100条数据中，处理到第23条，发现处理有问题，不能再继续向下处理了，需要数据回放
             * 那就提交23
             */
            consumer.commitSync(offset);
        }
    }

    private static Properties loadProp()
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.88.108:9092,192.168.88.109:9092,192.168.88.110:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "test_group9");
        props.put("client.id", "demo-commit-consumer-client");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");
        return props;
    }
}
