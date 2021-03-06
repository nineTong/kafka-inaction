package com.bianbo.kafka.consumer;

import com.bianbo.kafka.consumer.internal.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.IntStream;


public class CustomerSerDeseSender
{

    private final static Logger LOGGER = LoggerFactory.getLogger(CustomerSerDeseSender.class);

    public static void main(String[] args)
    {
        Properties properties = initProps();
        KafkaProducer<String, User> producer = new KafkaProducer<>(properties);
        IntStream.range(0, 10).forEach(i ->
        {
            ProducerRecord<String, User> record =
                    new ProducerRecord<>("test13", String.valueOf(i), new User(i, "name-" + i, "address-" + i));
            Future<RecordMetadata> future = producer.send(record);
            try
            {
                RecordMetadata metaData = future.get();
                LOGGER.info("The message is send done and the key is {},offset {}", i, metaData.offset());

            } catch (InterruptedException | ExecutionException e)
            {
                e.printStackTrace();
            }
        });
        producer.flush();
        producer.close();
    }

    private static Properties initProps()
    {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.88.108:9092,192.168.88.109:9092,192.168.88.110:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "com.wangwenjun.kafka.lesson4.internal.UserSerializer");
        props.put("acks", "all");
        return props;
    }
}
