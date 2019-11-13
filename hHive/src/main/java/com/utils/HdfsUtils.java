package com.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;

public class HdfsUtils {

    /**
     * 测试
     */
//    private static String HDFS_URI = "hdfs://hadoop01:8020";
    /**
     * 生产
     */
    private static String HDFS_URI = "hdfs://nameservice1";

    public static void main(String[] args )throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS",HDFS_URI);
        FileSystem fs = FileSystem.get(conf);
        System.out.println(ifExists(fs,"/test"));
//        String data = "123#1asd#asd#kasld";
//        writeToHdfs(fs,"/test/20191017.txt",data);

    }

    /**
     * 获取FS实例
     * @return
     */
    public static FileSystem getFS(){
        FileSystem fs = null;
        try{
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS",HDFS_URI);
            conf.setBoolean("dfs.support.append", true);
            conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
            conf.setBoolean("dfs.client.block.write.replace-datanode-on-failure.enable", true);
            fs = FileSystem.get(conf);
        }catch (Exception e){
            e.printStackTrace();
        }
        return fs;
    }

    public static void writeToHdfs(FileSystem fs,String path,String data)throws IOException{
        FSDataOutputStream out = fs.create(new Path(path));
        out.write(data.getBytes("UTF-8"));
        out.write("\n".getBytes("UTF-8"));
    }

    public static void getHdfsOutPutStream()throws Exception{
        Configuration conf = new Configuration();
        //相当于通过配置文件的key获取到value的值
//        conf.set("fs.defaultFS","hdfs://hadoop02:9000");
        //FileSystem 加载配置类的信息
        FileSystem fs = FileSystem.get(conf);
    }

    /**
     * 创建文件夹
     * @param fs
     * @param filename
     * @throws IOException
     */
    public static void mkdirs_file(FileSystem fs , String filename) throws IOException {
        //在hdfs上创建文件夹
        fs.mkdirs(new Path(filename));
    }

    /**
     * 递归在HDFS删除文件夹（子目录及子文件）
     * @param fs
     * @param dirPtth 需要删除的目录地址
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public static void delete_file(FileSystem fs,String dirPtth) throws IllegalArgumentException, IOException{
        fs.delete(new Path(dirPtth), true);
    }

    /**
     * 向HDFS上传文件(hdfs写的流程)
     * @param fs
     * @param conf
     * @throws IOException
     */
    public static void uploadFileToHdfs(FileSystem fs , Configuration conf) throws IOException{
        Path path = new Path("/hdfs/test/hdfsAPI.txt");			//文件上传到HDFS哪里
        File file = new File("D:/hdfsAPI.txt");				//上传的文件在本地路径的哪里
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));	//从哪里读
        FSDataOutputStream out = fs.create(path);   		//上传到哪里
        IOUtils.copyBytes(new FileInputStream(file), out, conf);	//文件从哪里读,读到哪里去 , 服务器的相关配置信息
    }

    /**
     * 从HDFS下载文件(hdfs读的流程)
     * @param fs
     * @param conf
     * @throws IOException
     */
    public static void downloadFileFromHdfs(FileSystem fs ,Configuration conf) throws IOException {
        Path path = new Path("/jc/hdfsAPI.txt"); 			//要下的文件在hdfs的位置
        File file = new File("d:/downloadToHdfs.txt");		//要下载到的本地路径
        FSDataInputStream in = fs.open(path);				//读文件流 , 从hdfs读
        BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file));	//写到本地磁盘的哪里
        IOUtils.copyBytes(in, out, conf);   				//文件从哪里读,写到哪里,服务器配置
    }

    /**
     * 检测该目录是否存在
     * @param fs
     * @param path
     * @return
     * @throws IOException
     */
    public static boolean ifExists(FileSystem fs ,String path)throws IOException{
        return fs.exists(new Path(path));
    }

}
