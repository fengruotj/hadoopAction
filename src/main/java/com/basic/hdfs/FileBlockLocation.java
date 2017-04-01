package com.basic.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;

/**
 * Created by 79875 on 2017/3/28.
 * 查找某个文件在HDFS集群中的位置
 */
public class FileBlockLocation {
    private static Logger logger=Logger.getLogger(FileBlockLocation.class);
    public static void main(String[] args) throws IOException {
        Configuration configuration=new Configuration();
        String url="hdfs://root2:9000";
        FileSystem fileSystem= FileSystem.get(URI.create(url),configuration);
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/user/root/wordcount/input/1.log"));
        BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for(int i=0;i<blockLocations.length;i++){
            String[] hosts = blockLocations[i].getHosts();
            logger.info("block_"+i+"_location "+hosts[0  ]);
        }
    }
}
