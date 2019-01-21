package com.basic.hdfs;

import org.junit.Test;

import java.io.IOException;

/**
 * Created by 79875 on 2017/3/28.
 */
public class HDFSDataInputExampleTest {
    HDFSDataInputExample hdfsDataInputExample=new HDFSDataInputExample();

    @Test
    public void Test() throws IOException {
        hdfsDataInputExample.createFile("D:\\storm-word-wordcount.txt","/user/root/input/storm-word-wordcount.txt");
        hdfsDataInputExample.getFile("D:\\storm-word-wordcount2.txt","/user/root/input/storm-word-wordcount.txt",true);
    }
}
