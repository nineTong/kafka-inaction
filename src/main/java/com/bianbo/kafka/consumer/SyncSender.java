package com.bianbo.kafka.consumer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.IntStream;


public class SyncSender
{

    private final static Logger LOGGER = LoggerFactory.getLogger(SyncSender.class);

    public static void main(String[] args)
    {
        Properties properties = initProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        IntStream.range(10, 20).forEach(i ->
        {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("test12",String.valueOf(i), "hello " + i);
            Future<RecordMetadata> future = producer.send(record);
            try
            {
                RecordMetadata metaData = future.get();
                LOGGER.info("The message is send done and the key is {},offset {}", i, metaData.offset());

            } catch (InterruptedException e)
            {
                e.printStackTrace();
            } catch (ExecutionException e)
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
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        return props;
    }
}
