package com.bianbo.kafka.sender.internal;

/**
 * 4 byte
 *
 * 在内存中分配4个字节，这4个字节中存放string类型对应的长度上限
 * [4][4][10][4][20]
 *
 *
 * [4][4][10][4][20]
 *
 */
public class User
{
    /**
     * int 类型在java中占4字节
     * string则是可变长类型
     */
    private int id;

    private String name;

    private String address;

    public User(int id, String name, String address)
    {
        this.id = id;
        this.name = name;
        this.address = address;
    }

    public int getId()
    {
        return id;
    }

    public String getName()
    {
        return name;
    }

    public String getAddress()
    {
        return address;
    }

    @Override
    public String toString()
    {
        return "User{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", address='" + address + '\'' +
                '}';
    }
}
