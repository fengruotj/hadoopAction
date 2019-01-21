package com.basic.hdfs.io;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

import java.net.URI;

/**
 * MapFile读文件
 * 1.创建Configuration
 * 2.获取FileSystem
 * 3.创建文件输出路径Path
 * 4.new一个MapFile.Reader进行读取
 * 5.得到KeyClass和ValueClass
 * 6.关闭流
 * @version
 */
public class MapFileReader {

    //定义文件读取的路径
    private static final String INPATH = "hdfs://ubuntu1:9000";

    public static void main(String[] args) {

        try {
            //创建配置信息
            Configuration conf = new Configuration();
            //创建文件系统
            FileSystem fs = FileSystem.get(new URI(INPATH),conf);
            //创建Path对象
            Path path = new Path(INPATH + "/mapfile");
            //4.new一个MapFile.Reader进行读取
            MapFile.Reader reader = new MapFile.Reader(fs, path.toString(), conf);

            //创建Key和Value
            Text key = new Text();
            Text  value = new Text();

            //遍历获取结果
            while (reader.next(key, value)){
                System.out.println("key=" + key);
                System.out.println("value=" + value);
            }
            //关闭流
            IOUtils.closeStream(reader);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

