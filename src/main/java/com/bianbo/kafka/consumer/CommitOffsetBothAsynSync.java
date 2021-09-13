package com.bianbo.kafka.sender;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;


public class CommitOffsetBothAsynSync
{
    private static final Logger LOG = LoggerFactory.getLogger(CommitOffsetBothAsynSync.class);


    public static void main(String[] args)
    {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(loadProp());
        consumer.subscribe(Collections.singletonList("test12"));
        try
        {
            for (; ; )
            {
                ConsumerRecords<String, String> records = consumer.poll(100);
                records.forEach(record ->
                {
                    //biz handler.
                    LOG.info("offset:{}", record.offset());
                    LOG.info("value:{}", record.value());
                    LOG.info("key:{}", record.key());
                });
                /**
                 * 即使时是可恢复的错误，也不会重复提交。
                 * 但是如果提交失败就会使用finally进行同步提交，会尝试多次提交
                 */
                consumer.commitAsync();
            }
        } finally {
            /**
             * 假设在处理过程中，jvm crash了，那么offset是无法提交的，需要程序再次消费时具有幂等性
             */
            consumer.commitSync();
        }
    }

    private static Properties loadProp()
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.88.108:9092,192.168.88.109:9092,192.168.88.110:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "test_group8");
        props.put("client.id", "demo-commit-consumer-client");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");
        return props;
    }
}