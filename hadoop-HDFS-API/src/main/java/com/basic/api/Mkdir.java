package com.basic.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * locate com.basic.api
 * Created by MasterTj on 2019/1/21.
 */
public class Mkdir {
    public static void main(String[] args) throws IOException {
        Configuration configuration=new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);

        fileSystem.mkdirs(new Path("output/"));
        fileSystem.close();
    }
}
