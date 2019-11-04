package com.SparkSql;

import java.io.Serializable;

/**
 * @author medal
 * @create 2019-04-16 10:46
 **/
public class Student implements Serializable {
    private int id;
    private String name;
    private int age;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return
                "id=" + id +
                ", name=" + name  +
                ", age=" + age ;
    }
}
