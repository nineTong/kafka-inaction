package com.bianbo.kafka.sender;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;


public class SimpleConsumer
{
    private static final Logger LOG = LoggerFactory.getLogger(SimpleConsumer.class);


    public static void main(String[] args)
    {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(loadProp());
        consumer.subscribe(Collections.singletonList("test_c"));

        final AtomicInteger counter = new AtomicInteger();
        for (; ; )
        {
            //ConsumerRecords是一个ConsumerRecord的集合
            ConsumerRecords<String, String> records = consumer.poll(100);
            /*100ms后如果没有消息将返回一个空集,感觉这个数值和fetch.min.bytes配置冲突
            这个值不能是负数，这个时间是指即使在buffer不可用的情况下也要查询时间达到100ms再返回
            如果这个值设置为0那么在buffer不够的情况下，直接从buffer中返回现有记录
            如果设置为其他值，则返回空
            如果配置了自动提交offset，此处还有有一个异步提交commit操作，commit结束后有一个回调
            在自动提交offset的情况下，是先poll进去后先查看是否够提交时间间隔进行提交，然后再去拉取数据，
            也就是说poll下最多可能提交上次poll到记录的offset，如果接下来能拉取到数据，也需要等待下次提交offset
            */
            records.forEach(record ->
            {
                //biz handler.
                LOG.info("offset:{}", record.offset());
                LOG.info("value:{}", record.value());
                LOG.info("key:{}", record.key());
                LOG.info("record produced timestamp",record.timestamp());
                //consumer是一个结束数据的端，不要将业务处理逻辑放在这里，应该将业务处理逻辑
                //放到一个异步的处理工具里，这样可以避免消费不及时
                int cnt = counter.incrementAndGet();

                if (cnt >= 3)
                    Runtime.getRuntime().halt(-1);
                    //halt是为了直接终止程序jvm，不给kafka任何机会触发钩子
                    //这种情况下容易因为没有提交offset造成重复消费
                    //解决重复消费就是要解决消息消费的幂等性
            });
        }

//        consumer.close();
    }

    private static Properties loadProp()
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.88.108:9092,192.168.88.109:9092,192.168.88.110:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "test_group4");
        props.put("client.id", "demo-consumer-client");
        props.put("auto.offset.reset", "earliest");
        //这里设置了自动提交的时间间隔，但是什么时候去commit一次呢？对于commit的时机 1.poll 2.consumer.close
        props.put("auto.commit.interval.ms", "10000");

        return props;
    }
}