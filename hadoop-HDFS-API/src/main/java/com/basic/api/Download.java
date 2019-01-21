package com.basic.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * locate com.basic.api
 * Created by MasterTj on 2019/1/21.
 */
public class Download {
    public static void main(String[] args) throws IOException {
        Configuration configuration=new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);

        FSDataInputStream open = fileSystem.open(new Path("tmp/test"));
        byte[] buffer=new byte[1024];
        int read=0;
        while ((read=open.read(buffer))!=-1){
            System.out.println(new String(buffer,0,read));
        }
        fileSystem.close();
    }
}
