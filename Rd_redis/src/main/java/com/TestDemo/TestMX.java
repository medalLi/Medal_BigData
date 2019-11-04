package com.TestDemo;

import redis.clients.jedis.Jedis;

/**
 * @author medal
 * @create 2019-05-08 13:57
 **/
public class TestMX {
    public static void main(String[] args) {
        Jedis jedis_M = new Jedis("hadoop.spark.com",6379);
        Jedis jedis_S = new Jedis("hadoop.spark.com",6380);

        jedis_S.slaveof("hadoop.spark.com",6379);
        String result = jedis_S.get("class");
        System.out.println(result);
    }
}
