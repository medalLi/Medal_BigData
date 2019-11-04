package com.TestDemo;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

/**
 * @author medal
 * @create 2019-05-08 13:39
 **/
public class TestTX {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("hadoop.spark.com",6379);
//        String value = jedis.get("hello");
//        System.out.println(value);
        Transaction transaction = jedis.multi();
        transaction.set("k2","k2");
        transaction.set("k3","k3");
        transaction.set("k4","k4");
        transaction.set("k5","k5");
        transaction.exec();
    }
}
