package com.hy.bigdata.modules.hdfs.common.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

public class HdfsUtils {


    private static FileSystem fileSystem = null;
    private static Configuration configuration = null;


    public static void SetUp(String HDFS_PATH, String userName) throws Exception {
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, userName);
        System.out.println(" hdfsapp start ");

    }


    public static void mkdir(String dirPath) throws Exception {
        fileSystem.mkdirs(new Path(dirPath));
    }


    public static void create(String pathAndFileName) throws Exception {
        FSDataOutputStream output = fileSystem.create(new Path(pathAndFileName));
        output.flush();
        output.close();
    }


    public static void rename(String beforName, String afterName) throws Exception {
        Path oldPath = new Path(beforName);
        Path newPath = new Path(afterName);
        fileSystem.rename(oldPath, newPath);
    }


    public static void copyFromLocalFile(String local, String to) throws Exception {
        Path localPath = new Path(local);
        //Path localPath2  = new Path(""C:\\Users\\sinitek\\Downloads\\hbase-1.2.0-cdh5.7.0-src\\sparkJob\\target\\sparkJob-1.0-SNAPSHOT.jar"");
        Path hdfsPath = new Path(to);
        //Path hdfsPath2 = new Path(""/user/feng/jar/sparkJob-1.0-SNAPSHOT.jar");
        fileSystem.copyFromLocalFile(localPath, hdfsPath);
        //   tearDown();

    }


    public void copyFromLocalFileWithLine() throws Exception {
        Path localPath = new Path("");
        Path hdfsPath = new Path("/hdfsapi");
        InputStream in = new BufferedInputStream(new FileInputStream(new File("D:\\第5章 分布式计算框架MapReduce.mp4")));
        FSDataOutputStream output = fileSystem.create(new Path("video.mp4"), new Progressable() {
            @Override
            public void progress() {
                System.out.print(".");
            }
        });

        IOUtils.copyBytes(in, output, 4096);
    }


    public void cat() throws Exception {
        FSDataInputStream in = fileSystem.open(new Path("/hdfsapi/test/a.txt"));
        IOUtils.copyBytes(in, System.out, 1024);
        in.close();
    }


    public void download(String hdfsPath, String localPath) throws Exception {
        Path localPath1 = new Path(localPath);
        Path hdfsPath1 = new Path(hdfsPath);
        fileSystem.copyToLocalFile(hdfsPath1, localPath1);
    }


    public void listFiles(String dir) throws Exception {
        FileStatus[] files = fileSystem.listStatus(new Path(dir));
        for (FileStatus status : files) {
            String isDir = status.isDirectory() ? "文件夹" : "文件";
            short replication = status.getReplication();
            //我们已经在hdfs-site.xml中设置了副本系数为1，为什么此时查询文件看到的3呢？
            //通过shell的方式上去的，是默认的系数1
            //如果是java api上传上去的，本地没有手工设置副本系数，所以采用的hadoop默认的
            long len = status.getLen();
            String path = status.getPath().toString();
            System.out.println(isDir + "\t" + replication + "\t" + len + "\t" + path);

        }
    }


    public static boolean delete(String path) throws Exception {
        Path filePath = new Path(path);
        return fileSystem.delete(filePath, true);

    }


    public static void tearDown() throws Exception {
        configuration = null;
        fileSystem = null;

        System.out.println(" hdfsapp stop ");
    }


}
