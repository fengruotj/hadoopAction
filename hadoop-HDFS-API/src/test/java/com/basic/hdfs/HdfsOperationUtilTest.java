package com.basic.hdfs;


import com.basic.util.HdfsOperationUtil;
import org.junit.Test;

/**
 * Created by 79875 on 2017/3/18.
 */
public class HdfsOperationUtilTest {

    HdfsOperationUtil hdfsOperationUtil=new HdfsOperationUtil();

    @Test
    public void testListDataNodeInfo() throws Exception {
        hdfsOperationUtil.listDataNodeInfo();
    }
    @Test
    public void testCheckFileExist() throws Exception {
        hdfsOperationUtil.checkFileExist("/user/root/input");
    }
    @Test
    public void testCreateFile() throws Exception {
        hdfsOperationUtil.createFile("/user/root/input/a.log","Hello i am a test");
    }
    @Test
    public void testCopyFileToHDFS() throws Exception {
        hdfsOperationUtil.copyFileToHDFS("D:\\adcfg.json","/user/root/input/abcfg.json");
    }
    @Test
    public void testGetLocation() throws Exception {
        hdfsOperationUtil.getBolockHosts("/user/root/input/a.log");
    }
    @Test
    public void testReadFileFromHdfs() throws Exception {
        hdfsOperationUtil.readFileFromHdfs("/user/root/abcfg.json");
    }
    @Test
    public void testListFileStatus() throws Exception {
        hdfsOperationUtil.listFileStatus("/user/root/abcfg.json");
    }

    @Test
    public void testCreateDir() throws Exception {
        hdfsOperationUtil.createDir("/user/79875");
    }

    @Test
    public void testListFiles() throws Exception {
        hdfsOperationUtil.listFiles("/user");
    }

    @Test
    public void testDeleteFile() throws Exception {
        hdfsOperationUtil.deleteFile("/mapfile");
    }
}
