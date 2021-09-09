package com.wangwenjun.kafka.lesson4.internal;

import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;


public class UserSerializer implements Serializer<User>
{
    @Override
    public void configure(Map<String, ?> configs, boolean isKey)
    {
        //do nothing
        //生命周期中调用一次
    }

    /**
     * 生成序列化失败时不可恢复错误，比如给定一个不存在的字符集
     * @param topic
     * @param data
     * @return
     */
    @Override
    public byte[] serialize(String topic, User data)
    {
        if (data == null)
            return null;

        int id = data.getId();
        String name = data.getName();
        String address = data.getAddress();

        byte[] nameBytes;
        byte[] addrBytes;
        if (name != null)
            nameBytes = name.getBytes();
        else
            nameBytes = new byte[0];

        if (address != null)
            addrBytes = address.getBytes();
        else
            addrBytes = new byte[0];

        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + nameBytes.length + 4 + addrBytes.length);
        buffer.putInt(id);
        buffer.putInt(nameBytes.length);
        buffer.put(nameBytes);
        buffer.putInt(addrBytes.length);
        buffer.put(addrBytes);

        return buffer.array();
    }

    @Override
    public void close()
    {
        //do nothing
    }
}
