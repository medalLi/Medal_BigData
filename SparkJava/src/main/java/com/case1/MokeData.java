package com.case1;

import javax.sound.midi.Soundbank;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

/**
 * @author medal
 * @create 2019-04-09 17:38
 **/

/*
生产1000W条数据
数据格式：第一列是ID，第二列是年龄
1 16
2 74
3 51
4 35
5 44
6 95
7 5

*
* */
public class MokeData {
    public static void createData(int size){
        int number = 0;
        long id = 1;
        String result = "";
        ArrayList<String> array = new ArrayList<String>();
        while (size > 0) {
            number = (int) (Math.random() * 150);
            result = ""+id+"\t"+number;
            //write(result);
            array.add(result);
            size --;
            id ++;
//            if(array.size() == 100000){
//                write(array);
//                array.clear();
//            }
        }
        write(array);
    }

    public static void write(ArrayList<String> array){
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter("SparkJava" +
                    "\\src\\main\\java\\com\\case1\\data.txt"));
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
