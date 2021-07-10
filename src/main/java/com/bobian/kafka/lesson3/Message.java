package com.bobian.kafka.lesson3;


public class Message
{
    private final int id;
    private final String name;

    public Message(int id, String name)
    {
        this.id = id;
        this.name = name;
    }

    public int getId()
    {
        return id;
    }

    public String getName()
    {
        return name;
    }
}