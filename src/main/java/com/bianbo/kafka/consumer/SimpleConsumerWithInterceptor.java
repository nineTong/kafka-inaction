package com.bianbo.kafka.sender;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;


public class SimpleConsumerWithInterceptor
{
    private static final Logger LOG = LoggerFactory.getLogger(SimpleConsumerWithInterceptor.class);


    public static void main(String[] args)
    {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(loadProp());
        consumer.subscribe(Collections.singletonList("test14"));

        for (; ; )
        {
            ConsumerRecords<String, String> records = consumer.poll(100);
            records.forEach(record ->
            {
                //biz handler.
                LOG.info("P:{},V:{},OFS:{}", record.partition(), record.value(), record.offset());
            });
        }


    }

    private static Properties loadProp()
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.88.108:9092,192.168.88.109:9092,192.168.88.110:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "G5");
        props.put("client.id", "demo-consumer-client");
        props.put("auto.offset.reset", "earliest");
        props.put("auto.commit.interval.ms", "10000");
        /**
         * 如果配置了拦截器，那么拦截器将会在fetch后poll之前使用拦截器
         * 配置拦截器后可以选择需要的record，但在提交commit时，依然会将本次fetch到的所有数据提交commit
         */
//        props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.wangwenjun.kafka.lesson4.MyConsumerInterceptor");
        return props;
    }
}