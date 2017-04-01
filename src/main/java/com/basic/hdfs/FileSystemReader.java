package com.basic.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * Created by 79875 on 2017/3/28.
 * 根据filesystem读取Hdfs上面的数据
 */
public class FileSystemReader {

    public static void main(String[] args) throws IOException {
        Configuration configuration=new Configuration();
        String url="hdfs://root2:9000";
        FileSystem fileSystem=FileSystem.get(URI.create(url),configuration);
        InputStream inputStream=fileSystem.open(new Path("/user/root/wordcount/output/part-00000"));
        IOUtils.copyBytes(inputStream,System.out,4096,false);
        inputStream.close();
    }
}
