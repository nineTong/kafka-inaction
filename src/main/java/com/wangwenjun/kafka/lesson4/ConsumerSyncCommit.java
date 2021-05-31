package com.wangwenjun.kafka.lesson4;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;


public class ConsumerSyncCommit
{
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerSyncCommit.class);

    public static void main(String[] args)
    {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(loadProp());
        consumer.subscribe(Collections.singletonList("test12"));

        final AtomicInteger count = new AtomicInteger(0);

        for (; ; )
        {
            ConsumerRecords<String, String> records = consumer.poll(100);
            records.forEach(record ->
            {
                //biz handler.
                LOG.info("offset:{}", record.offset());
                LOG.info("value:{}", record.value());
                LOG.info("key:{}", record.key());
                /**
                 * 如果担心一批中处理了99条后程序终止了，没有提交offset，可以每条提交一次，这样最多有一条没有提交
                 * consumer.commitSync();
                 */
//                if (count.incrementAndGet() == 1000)
//                {
//                    consumer.commitSync();
//                    count.set(0);
//                }
            });

            /**
             * can retry
             * block
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
        props.put("group.id", "test_group4");
        props.put("client.id", "demo-commit-consumer-client");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");
        return props;
    }
}