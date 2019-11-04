package examples.压缩;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * @author medal
 * @create 2019-01-03 21:42
 **/

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class TestCompress {

    public static void main(String[] args) throws Exception, IOException {
		//compress("d:/MoNi10_15-10_25.txt","org.apache.hadoop.io.compress.BZip2Codec");
        decompres("d:/MoNi10_15-10_25.txt.bz2");
    }

    /*
     * 压缩
     * filername：要压缩文件的路径
     * method：欲使用的压缩的方法（org.apache.hadoop.io.compress.BZip2Codec）
     */
    public static void compress(String filername, String method) throws ClassNotFoundException, IOException {

        // 1 创建压缩文件路径的输入流
        File fileIn = new File(filername);
        InputStream in = new FileInputStream(fileIn);

        // 2 获取压缩的方式的类
        Class codecClass = Class.forName(method);

        Configuration conf = new Configuration();
        // 3 通过名称找到对应的编码/解码器
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

        // 4 该压缩方法对应的文件扩展名
        File fileOut = new File(filername + codec.getDefaultExtension());
        fileOut.delete();

        OutputStream out = new FileOutputStream(fileOut);
        CompressionOutputStream cout = codec.createOutputStream(out);

        // 5 流对接
        IOUtils.copyBytes(in, cout, 1024 * 1024 * 5, false); // 缓冲区设为5MB

        // 6 关闭资源
        in.close();
        cout.close();
    }

    /*
     * 解压缩
     * filename：希望解压的文件路径
     */
    public static void decompres(String filename) throws FileNotFoundException, IOException {

        Configuration conf = new Configuration();
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);

        // 1 获取文件的压缩方法
        CompressionCodec codec = factory.getCodec(new Path(filename));

        // 2 判断该压缩方法是否存在
        if (null == codec) {
            System.out.println("Cannot find codec for file " + filename);
            return;
        }

        // 3 创建压缩文件的输入流
        InputStream cin = codec.createInputStream(new FileInputStream(filename));

        // 4 创建解压缩文件的输出流
        File fout = new File(filename + ".decoded");
        OutputStream out = new FileOutputStream(fout);

        // 5 流对接
        IOUtils.copyBytes(cin, out, 1024 * 1024 * 5, false);

        // 6 关闭资源
        cin.close();
        out.close();
    }
}

