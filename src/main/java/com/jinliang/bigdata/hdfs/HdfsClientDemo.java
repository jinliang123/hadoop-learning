package com.jinliang.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;

/**
 * 客户端操作hdfs时，是有一个用户身份的
 * 默认情况下，hdfs客户端api会从jvm获取一个参数来作为自身的用户身份：-DHADOOP_USER_NAME=hadoop
 */

public class HdfsClientDemo {
    FileSystem fs = null;
    Configuration conf = null;

    @Before
    public void init() throws Exception {

        conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://node1:9000");
        conf.set("dfs.replication","4");
        //拿到一个文件系统操作的客户端实例对象
		fs = FileSystem.get(new URI("hdfs://node1:9000"),conf,"hadoop");
    }

    /**
     * 上传文件
     * @throws Exception
     */
    @Test
    public void testUpload() throws Exception {

        Thread.sleep(2000);
        fs.copyFromLocalFile(new Path("D:/access.log"), new Path("/access.log.copy"));
        fs.close();
    }

    /**
     * 下载文件
     * @throws IOException
     */

    @Test
    public void testDownload() throws IOException {
        fs.copyToLocalFile(new Path("/access.log.copy"),new Path("d:/"));
        fs.close();
    }

    /**
     * 打印连接参数
     */

    @Test
    public void testConf(){
        Iterator<Map.Entry<String, String>> iterator = conf.iterator();
        while (iterator.hasNext()){
            Map.Entry<String, String> entry = iterator.next();
            System.out.println(entry.getKey()+":"+entry.getValue());
        }
    }

    /**
     * 创建目录
     * @throws IOException
     */
    @Test
    public void testMkdir() throws IOException {
        boolean result = fs.mkdirs(new Path("/testmkdir/aaa/bbb"));
        System.out.println(result);
    }


    /**
     * 删除目录
     * @throws IOException
     */
    @Test
    public void testDeleteDir() throws IOException {
        //true 级联删除 删除 aaa/bbb
        boolean result = fs.delete(new Path("/testmkdir/aaa"), true);
        System.out.println(result);
    }

    /**
     * 递归列出指定目录下所有子文件夹中的文件属性
     * @throws IOException
     */
    @Test
    public void testLs() throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("blocksize:"+fileStatus.getBlockLocations());
            System.out.println("owner:"+fileStatus.getOwner());
            System.out.println("Replication:"+fileStatus.getReplication());
            System.out.println("Permisson:"+fileStatus.getPermission());
            System.out.println("Name:"+fileStatus.getPath().getName());
            System.out.println("-----------------------------");
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation b : blockLocations){
                System.out.println("块起始偏移量:"+b.getOffset());
                System.out.println("块长度:"+b.getLength());
                //块所在的datanode节点
                String[] datanodes = b.getHosts();
                for (String dn:datanodes){
                    System.out.println("datanode:"+dn);
                }
            }
        }
    }

    /**
     * 列出文件列表
     * @throws Exception
     */
    @Test
    public void testLs2() throws Exception {

        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for(FileStatus file :listStatus){

            System.out.println("name: " + file.getPath().getName());
            System.out.println((file.isFile()?"file":"directory"));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf  = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node1:9000");
        FileSystem fs = FileSystem.get(conf);
        fs.copyFromLocalFile(new Path("D:/access.log"), new Path("/access.log.copy"));
        fs.close();
    }
}