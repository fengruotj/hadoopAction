package com.basic.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * locate com.basic.hdfs
 * Created by MasterTj on 2019/1/21.
 */
public class HdfsReadTest {
    public static void main(String[] args) throws IOException {
        String url=args[0];
        Configuration configuration=new Configuration();
        FileSystem fileSystem=FileSystem.get(URI.create(url),configuration);
        FSDataInputStream inputStream = null;
        try {

            inputStream = fileSystem.open(new Path(url));
            byte buffer[] =new byte[1024];
            int read=0;
            while ((read=inputStream.read(buffer))!=-1){
                System.out.println(new String (buffer,0,read));
            }
        } finally {
            inputStream.close();
        }
    }
}
