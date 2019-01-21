package com.basic.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URI;

/**
 * Created by 79875 on 2017/3/28.
 * FSDataInputStream的作用就是用DataInputStream 包装了一个输入流，并且使用BufferedInputStreaming实现对shrubs的缓冲
 */
public class HDFSDataInputExample {
    private static Configuration configuration = new Configuration();
    private static final String HADOOP_URL="hdfs://ubuntu1:9000";

    private static FileSystem fileSystem;
    private static Logger logger=Logger.getLogger(FileBlockLocation.class);

    static {
        try {
            fileSystem= FileSystem.get(URI.create(HADOOP_URL),configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createFile(String localPath,String hdfsPath) throws IOException {
        InputStream in=null;
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(hdfsPath));
        in=new BufferedInputStream(new FileInputStream(new File(localPath)));
        IOUtils.copyBytes(in,fsDataOutputStream,4096,true);
//        fsDataOutputStream.hsync();
        fsDataOutputStream.close();
        logger.info("create file in hdfs:"+hdfsPath);
    }

    public void getFile(String localfile,String hdfsPath,boolean print) throws IOException {
        FSDataInputStream inputStream = fileSystem.open(new Path(hdfsPath));
        OutputStream out=new BufferedOutputStream(new FileOutputStream(new File(localfile)));
        IOUtils.copyBytes(inputStream,out,4096,false);
        logger.info("get the file from hdfs to local:"+hdfsPath+","+localfile);
        if(print){
            inputStream.seek(0);//将当前文件指针定位到0
            IOUtils.copyBytes(inputStream,System.out,4096,true);
        }
    }
}
