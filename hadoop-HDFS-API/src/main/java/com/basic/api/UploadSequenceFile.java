package com.basic.api;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;

/**
 * locate com.basic.api
 * Created by MasterTj on 2019/1/21.
 * Hadoop提供的SequenceFile文件格式提供一对key,value形式的不可变的数据结构。
 *
 * 将小文件合并成大文件
 */
public class UploadSequenceFile {
    public static void main(String[] args) throws IOException {
        Configuration configuration=new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path=new Path("sequence/seq1");

        SequenceFile.Writer write=SequenceFile.createWriter(fileSystem,configuration,path, Text.class,Text.class);

        File file=new File("D://data");
        for (File file1 : file.listFiles()) {
            write.append(new Text(file1.getName()),new Text(FileUtils.readFileToString(file1)));
        }
    }
}
