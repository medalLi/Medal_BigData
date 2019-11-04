package com.test;

import org.junit.Test;

import java.io.*;

/**
 * @author medal
 * @create 2019-04-03 21:54
 **/
public class ShellOnJava {
    public static String exec(String command) throws InterruptedException {
        String returnString = "";
        Process pro = null;
        Runtime runTime = Runtime.getRuntime();
        if (runTime == null) {
            System.err.println("Create runtime false!");
        }
        try {
            pro = runTime.exec(command);
            BufferedReader input = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            PrintWriter output = new PrintWriter(new OutputStreamWriter(pro.getOutputStream()));
            String line;
            while ((line = input.readLine()) != null) {
                returnString = returnString + line + "\n";
            }
            input.close();
            output.close();
            pro.destroy();
        } catch (IOException ex) {
            //Logger.getLogger(Test001.class.getName()).log(Level.SEVERE, null, ex);
        }
        return returnString;
    }

//    public static void test() throws Exception {
//        System.out.println(exec("ls -al"));
//    }

    public static void main(String[] args) throws Exception {
        System.out.println(exec(args[0]));
    }

}
