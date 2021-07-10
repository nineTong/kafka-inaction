package com.bobian.kafka.lesson3;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;


public class MessageSerializer implements Serializer<Message>
{
    @Override
    public void configure(Map<String, ?> map, boolean b)
    {

    }

    @Override
    public byte[] serialize(String s, Message message)
    {
        if (null == message)
            return null;

        int id = message.getId();
        String name = message.getName();
        int nameLength = 0;
        if (name != null && !name.isEmpty())
        {
            nameLength = name.length();
        }

        return new byte[0];
    }

    @Override
    public void close()
    {

    }
}
