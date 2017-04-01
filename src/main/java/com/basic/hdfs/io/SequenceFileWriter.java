package com.basic.hdfs.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;

import java.net.URI;

/**
 * 写过程：
 * 1)：创建Configuration
 * 2)：获取FileSystem
 * 3)：创建文件输出路径Path
 * 4)：调用SequenceFile.createWriter()方法得到SequenceFile.Writer对象
 * 5)：调用SequenceFile.Writer的append()方法将内容追加到文件。
 * 6)：关闭流
 * @version
 */
public class SequenceFileWriter {

    //定义压缩的路径
    private static final String URL = "hdfs://root2:9000";

    public static void main(String[] args) {

        try {
            //创建配置信息
            Configuration conf = new Configuration();
            //创建文件系统
            FileSystem fileSystem = FileSystem.get(new URI(URL), conf);

            //压缩后的文件名称
            Path path = new Path(URL + "/tmp.seq");

            //创建SequenceFile.Writer对象
            //Writer writer = SequenceFile.createWriter(fileSystem, conf, path, Text.class, Text.class);
            SequenceFile.Writer writer = SequenceFile.createWriter(fileSystem,conf, path, Text.class, Text.class, SequenceFile.CompressionType.BLOCK, new BZip2Codec());

            //通过writer想文档中写入记录
            writer.append(new Text("name"), new Text("lavimer"));
            //关闭writer
            IOUtils.closeStream(writer);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
