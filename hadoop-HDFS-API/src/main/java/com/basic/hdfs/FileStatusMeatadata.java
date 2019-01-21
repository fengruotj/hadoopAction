package com.basic.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.sql.Timestamp;

/**
 * Created by 79875 on 2017/3/28.
 * Hadoop中的FileStatus类可以用来查看Hdfs中文件或者目录的元信息，任意文件或者目录都可以得到对应的FileStatus
 * FileStatus类中文件和目录的源数据信息，包括长度、块大小、备份、修改时间、所有者和权限信息
 */
public class FileStatusMeatadata {
    private static Logger logger=Logger.getLogger(FileStatusMeatadata.class);
    public static void main(String[] args) throws IOException {
        Configuration configuration=new Configuration();
        String url="hdfs://ubuntu1:9000";
        FileSystem fileSystem= FileSystem.get(URI.create(url),configuration);
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/user/root/wordcount/output/part-00000"));
        if(fileStatus.isDirectory()==true){
            logger.info("this is a directory");
        }else {
            logger.info("this is a file");
        }
        logger.info("文件路径："+fileStatus.getPath());
        logger.info("文件长度："+fileStatus.getLen());
        logger.info("文件上次访问时间："+new Timestamp(fileStatus.getAccessTime()));
        logger.info("文件备份数量："+fileStatus.getReplication());
        logger.info("文件块大小："+fileStatus.getBlockSize());
        logger.info("文件所有者:"+fileStatus.getOwner());
        logger.info("文件所在分组:"+fileStatus.getGroup());
        logger.info("文件的权限:"+fileStatus.getPermission().toString());
    }
}
