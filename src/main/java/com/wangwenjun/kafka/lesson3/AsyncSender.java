package com.wangwenjun.kafka.lesson3;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;


public class AsyncSender
{

    private final static Logger LOGGER = LoggerFactory.getLogger(AsyncSender.class);

    public static void main(String[] args)
    {
        Properties properties = initProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        IntStream.range(0, 10).forEach(i ->
        {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("fire_and_forget_sender", String.valueOf(i), "hello " + i);
            producer.send(record, (r, e) ->
            {
                if(e==null){
                    LOGGER.info("The message is send done and the key is {},offset {}", i, r.offset());
                }
                //异步发送会将消息直接发送到kafka消息收集器中，消息收集器在发送成功或失败后，会回调该函数，发送一个结果
                //正常发送过程中不会等待该回调函数运行
            });

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
        props.put("compression.type", "snappy");

        return props;
    }
}