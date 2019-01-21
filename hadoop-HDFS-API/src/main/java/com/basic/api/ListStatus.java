package com.basic.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * locate com.basic.api
 * Created by MasterTj on 2019/1/21.
 */
public class ListStatus {
    public static void main(String[] args) throws IOException {
        Configuration configuration=new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);

        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/user/"));
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println(fileStatus.getPath());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getPermission());
        }

        fileSystem.close();
    }
}
