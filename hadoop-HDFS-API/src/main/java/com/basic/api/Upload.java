package com.basic.api;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

/**
 * locate com.basic.api
 * Created by MasterTj on 2019/1/21.
 */
public class Upload {
    public static void main(String[] args) throws IOException {
        Configuration configuration=new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);

        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("tmp/test"));
        FileUtils.copyFile(new File("data/test.text"), fsDataOutputStream);
        fileSystem.close();
    }
}
