package com.basic.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;

/**
 * Created by 79875 on 2017/3/28.
 * 列出hdfs下所有目录的文件
 */
public class ListAllFile {
    private static Logger logger=Logger.getLogger(ListAllFile.class);

    public static void main(String[] args) throws IOException {
        Configuration configuration=new Configuration();
        String url="hdfs://ubuntu1:9000";
        FileSystem fileSystem= FileSystem.get(URI.create(url),configuration);
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/user"));
        for(int i=0;i<fileStatuses.length;i++){
            logger.info(fileStatuses[i].getPath());
        }
    }
}
