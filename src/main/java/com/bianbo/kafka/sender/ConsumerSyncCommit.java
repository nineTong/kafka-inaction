package com.bianbo.kafka.sender;

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
import java.util.concurrent.atomic.AtomicInteger;


public class ConsumerSyncCommit
{
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerSyncCommit.class);

    public static void main(String[] args)
    {
        //
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
                 *
                 *TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                 *OffsetAndMetadata om = new OffsetAndMetadata(record.offset()+1);
                 *
                 *consumer.commitSync(Collections.singletonMap(tp,om));
                 */

//                if (count.incrementAndGet() == 1000)
//                {
//                    consumer.commitSync();
//                    count.set(0);
//                }
            });

            /**
             * can retry，同步提交相当于一个producer向__consumer_topic发送数据，如果达到重复次数，或不可恢复错误，就会失败
             * block，会阻塞，在commit期间不会消费或处理新数据
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