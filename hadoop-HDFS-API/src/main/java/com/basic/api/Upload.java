package com.basic.api;

import com.basic.common.Constant;
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
        Configuration conf=new Configuration();
        FileSystem.setDefaultUri(conf, Constant.HADOOP_URL);
        FileSystem fileSystem = FileSystem.get(conf);

        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/user/root/hadoop/input/wc"));
        FileUtils.copyFile(new File("data/wc"), fsDataOutputStream);
        fileSystem.close();
    }
}
