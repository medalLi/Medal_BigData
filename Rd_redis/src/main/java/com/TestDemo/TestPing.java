package com.TestDemo;

import redis.clients.jedis.Jedis;

/**
 * @author medal
 * @create 2019-05-08 13:29
 **/
public class TestPing {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("hadoop.spark.com",6379);
        String value = jedis.get("hello");
        System.out.println(value);
    }
}
