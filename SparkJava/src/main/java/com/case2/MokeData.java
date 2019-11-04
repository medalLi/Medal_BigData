package com.case2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

/**
 * @author medal
 * @create 2019-04-12 14:38
 **/

/*
生产1000W条数据
数据格式：第一列是ID，第二列是性别，第三列是身高
1 M 174
2 F 165
3 M 172
4 M 180
5 F 160
6 F 162
7 M 172

* */

public class MokeData {
    public static void createData(int size){
        int number = 0;
        int index = 0;
        long id = 1;
        char[] ch = new char[]{'M','F'};
        String result = "";
        ArrayList<String> array = new ArrayList<String>();
        while (size > 0) {
            number = (int) (Math.random() * 40)+160;
            index = (int)(Math.random()*2);
            result = ""+id+"\t"+ch[index]+"\t"+number;
            array.add(result);
            size --;
            id ++;
        }
        write(array);
    }

    public static void write(ArrayList<String> array){
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter("SparkJava" +
                    "\\src\\main\\java\\com\\case2\\data.txt"));
            for(String line : array){
                bw.write(line);
                bw.newLine();
                bw.flush();
            }
        } catch (IOException e) {
            System.out.println("write the data is filed");
        }
    }
}
