package com.basic.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * locate com.basic.api
 * Created by MasterTj on 2019/1/21.
 * Hadoop提供的SequenceFile文件格式提供一对key,value形式的不可变的数据结构。
 */
public class DownloadSequenceFile {
    public static void main(String[] args) throws IOException {
        Configuration configuration=new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path=new Path("sequence/seq1");

        SequenceFile.Reader reader=new SequenceFile.Reader(fileSystem,path,configuration);
        Text key=new Text();
        Text value=new Text();

        while (reader.next(key,value)){
            System.out.println("key: "+key.toString());
            System.out.println("value: "+value.toString());
        }
    }
}
