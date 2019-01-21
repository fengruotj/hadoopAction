package com.basic.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.net.URI;

/**
 * locate com.basic.hdfs
 * Created by MasterTj on 2019/1/21.
 */
public class HdfsWriteTest {
    public static void main(String[] args) throws IOException {
        String url=args[0];
        Configuration configuration=new Configuration();
        FileSystem fileSystem= FileSystem.get(URI.create(url),configuration);
        LocalFileSystem localFileSystem  = FileSystem.getLocal(configuration);

        Path localDir=new Path(args[0]);
        Path hdfFile=new Path(args[1]);

        try {
            FileStatus[] fileStatuses = localFileSystem.listStatus(localDir);
            FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfFile, new Progressable() {
                @Override
                public void progress() {
                    System.out.println(".");
                }
            });

            for (FileStatus fileStatus : fileStatuses) {
                FSDataInputStream inputStream = localFileSystem.open(fileStatus.getPath());
                byte buffer[]=new byte[1024];
                int read=0;
                while ((read=inputStream.read(buffer))!=-1){
                    fsDataOutputStream.write(buffer,0,read);
                }
            }
        } finally {
            fileSystem.close();
            localFileSystem.close();
        }

    }
}
