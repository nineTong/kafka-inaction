package com.wangwenjun.kafka.lesson3;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;


public class MyPartitioner implements Partitioner
{

    private final String LOGIN = "LOGIN";
    private final String LOGOFF = "LOGOFF";
    private final String ORDER = "ORDER";

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
                         Cluster cluster)
    {
        //如果显式指定了一个partition，那么partitioner是不会起任何作用的，
        //如果没有指定partitioner且使用默认partitioner则会1.如果没有指定key，会随机取一个partition2.如果指定了key则会对key的hash取模
        //Cluster可以获取partition信息
        if (keyBytes == null || keyBytes.length == 0)
        {
            throw new IllegalArgumentException("The key is required for BIZ.");
        }

        switch (key.toString().toUpperCase())
        {
            case LOGIN:
                return 0;
            case LOGOFF:
                return 1;
            case ORDER:
                return 2;
            default:
                throw new IllegalArgumentException("The key is invalid.");
        }
    }

    @Override
    public void close()
    {

    }

    @Override
    public void configure(Map<String, ?> configs)
    {

    }
}