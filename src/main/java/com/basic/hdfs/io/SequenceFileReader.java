package com.basic.hdfs.io;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.net.URI;

/**
 * 读过程：
 * 1)：创建Configuration
 * 2)：获取FileSystem
 * 3)：创建文件输出路径Path
 * 4)：new一个SequenceFile.Reader获取对象
 * 5)：得到KeyClass和ValueClass
 * 6)：关闭流
 * @version
 */
public class SequenceFileReader {

    //定义文件读取的路径
    private static final String INPATH = "hdfs://root2:9000";

    public static void main(String[] args) {

        try {
            //创建配置信息
            Configuration conf = new Configuration();
            //创建文件系统
            FileSystem fs = FileSystem.get(new URI(INPATH),conf);
            //创建Path对象
            Path path = new Path(INPATH + "/tmp.seq");
            //创建SequenceFile.Reader对象
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);

            //创建Key和value
            Text key = new Text();
            Text value = new Text();
            //从tmp.seq中循环读取内容
            while (reader.next(key, value)){
                System.out.println("key=" + key);
                System.out.println("value=" + key);
                System.out.println("position=" + reader.getPosition());
            }

            //关闭资源
            IOUtils.closeStream(reader);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}


