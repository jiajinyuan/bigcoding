package com.jf.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;


public class HdfsDemo {

    /**
     * 获取文件系统
     *
     * @return 文件系统
     */
    public FileSystem getFileSystem() {
        try {
            // 获取配置文件
            Configuration conf = new Configuration();
            // 设置访问hdfs的用户
            System.setProperty("HADOOP_USER_NAME", "hdfs");
            // 设置访问的文件系统URI
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://10.0.6.103:8020"), conf);
            return fileSystem;
        } catch (IOException e) {
            return null;
        } catch (URISyntaxException e) {
            return null;
        }
    }

    /**
     * 获取某一目录下的内容
     *
     * @throws IOException
     */
    @Test
    public void listFile() throws IOException {
        // 获取文件系统
        FileSystem fileSystem = getFileSystem();
        // 获取目录状态
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        // 遍历状态
        for (FileStatus status : fileStatuses) {
            // 获取路径 -> hdfs://10.0.10.10:8020/data/file.txt
            System.out.println(status.getPath());
            // 获取权限 -> drwxr-xr-x
            System.out.println(status.getPermission());
            // 获取大小 -> 104 (单位为B)
            System.out.println(status.getLen());
            // 获取块大小 - > 134217728 -> 128M
            System.out.println(status.getBlockSize());
        }
    }

    /**
     * 上传文件到HDFS（复制本地文件）
     *
     * @throws IOException
     */
    public void uploadFileByCopy() throws IOException {
        // 获取文件系统
        FileSystem fileSystem = getFileSystem();
        // 本地文件路径
        Path src = new Path("E:/hadoop.jar");
        // HDFS目录或文件路径
        Path dst = new Path("/data");
        // 复制本地文件到HDFS
        fileSystem.copyFromLocalFile(src, dst);
    }

    /**
     * 上传文件到HDFS（剪切本地文件）
     *
     * @throws IOException
     */
    public void uploadFileByMove() throws IOException {
        // 获取文件系统
        FileSystem fileSystem = getFileSystem();
        // 本地文件路径
        Path src = new Path("E:/hadoop.jar");
        // HDFS目录或文件路径
        Path dst = new Path("/data");
        // 剪切本地文件到HDFS
        fileSystem.moveFromLocalFile(src, dst);
    }

    /**
     * 上传文件到HDFS（字节流传输方式）
     *
     * @throws IOException
     */
    public void uploadFile() throws IOException {
        // 获取文件系统
        FileSystem fileSystem = getFileSystem();
        // HDFS文件系统的输出流
        FSDataOutputStream fos = fileSystem.create(new Path("/data/user.txt"));
        // 本地文件系统的输入流
        FileInputStream fis = new FileInputStream("D:/user.txt");
        // 参数：输入流，输出流，缓冲大小，是否关闭流
        IOUtils.copyBytes(fis, fos, 4096, true);
    }

    /**
     * 追加文件
     *
     * @throws IOException
     */
    public void appendFile() throws IOException {
        // 获取文件系统
        FileSystem fileSystem = getFileSystem();
        // HDFS文件系统的输出流
        FSDataOutputStream fos = fileSystem.append(new Path("/data/file.txt"));
        // 本地文件系统的输入流
        FileInputStream fis = new FileInputStream("D:/file.txt");
        // 参数：输入流，输出流，缓冲大小，是否关闭流
        IOUtils.copyBytes(fis, fos, 4096, true);
    }

    /**
     * 下载文件
     *
     * @throws IOException
     */
    public void downloadFileByCopy() throws IOException {
        // 获取文件系统
        FileSystem fileSystem = getFileSystem();
        // 复制HDFS文件到本地
        fileSystem.copyToLocalFile(new Path("/data/file.txt"), new Path("D:/data.txt"));
    }

    /**
     * 下载文件（剪切方式）
     *
     * @throws IOException
     */
    public void downloadFileByMove() throws IOException {
        // 获取文件系统
        FileSystem fileSystem = getFileSystem();
        // 剪切HDFS文件到本地
        fileSystem.moveToLocalFile(new Path("/data/file.txt"), new Path("D:/data.txt"));
    }

    /**
     * 下载文件（字节流传输方式）
     *
     * @throws IOException
     */
    public void downloadFile() throws IOException {
        // 获取文件系统
        FileSystem fileSystem = getFileSystem();
        // HDFS文件输入流
        FSDataInputStream fis = fileSystem.open(new Path("/data/file.txt"));
        // 本地文件输出流
        FileOutputStream fos = new FileOutputStream(new File("D:/file.txt"));
        // 参数：输入流，输出流，缓冲大小，是否关闭流
        IOUtils.copyBytes(fis, fos, 4096, true);
    }

    /**
     * 创建目录
     *
     * @throws IOException
     */
    public void mkdirs() throws IOException {
        // 获取文件系统
        FileSystem fileSystem = getFileSystem();
        // 创建文件夹
        fileSystem.mkdirs(new Path("/data/wordcount/in"));
    }

    /**
     * 删除目录或文件
     *
     * @throws IOException
     */
    public void delete() throws IOException {
        FileSystem fileSystem = getFileSystem();
        // 删除目录，如果删除不为空的目录，设置为true
        fileSystem.delete(new Path("/data/wordcount/in"), true);
        // 删除文件
        fileSystem.delete(new Path("/data/file.txt"), false);
    }

    /**
     * 创建空文件
     *
     * @throws IOException
     */
    public void createFile() throws IOException {
        // 获取文件系统
        FileSystem fileSystem = getFileSystem();
        // 创建文件
        fileSystem.createNewFile(new Path("/data/file.txt"));
    }

    /**
     * 查看HDFS文件内容，输出到控制台
     *
     * @throws Exception
     */
    public void viewFile() throws Exception {
        // 获取文件系统
        FileSystem fileSystem = getFileSystem();
        FSDataInputStream fis = fileSystem.open(new Path("/test/aa.txt"));
        IOUtils.copyBytes(fis, System.out, 4096, true);
    }

    /**
     * 更改文件或目录所属者和组
     *
     * @throws IOException
     */
    public void chown() throws IOException {
        // 获取文件系统
        FileSystem fileSystem = getFileSystem();
        // 更改所属用户，所属组
        fileSystem.setOwner(new Path("/test/test"), "hdfs", "hive");
    }

    /**
     * 更改文件权限
     *
     * @throws IOException
     */
    public void chmod() throws IOException {
        // 获取文件系统
        FileSystem fileSystem = getFileSystem();
        // 更改文件权限
        fileSystem.setPermission(new Path("/test/aa.txt"), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    }

    /**
     * 移动或者重命名
     *
     * @throws IOException
     */
    public void rename() throws IOException {
        FileSystem fileSystem = getFileSystem();
        // 重命名
        fileSystem.rename(new Path("/data"), new Path("/hdfs"));
        // 移动
        fileSystem.rename(new Path("/data/file.txt"), new Path("/hdfs/in/file.txt"));
    }

    /**
     * 判断文件或者目录
     *
     * @throws IOException
     */
    public void testPath() throws IOException {
        // 获取文件系统
        FileSystem fileSystem = getFileSystem();
        // 判断路径是否为文件
        System.out.println(fileSystem.isFile(new Path("/test/aa.txt")));
        // 判断路径是否为目录
        System.out.println(fileSystem.isDirectory(new Path("/test")));
    }

    /**
     * 获取文件大小,单位为B
     *
     * @throws IOException
     */
    public void getLength() throws IOException {
        // 获取文件系统
        FileSystem fileSystem = getFileSystem();
        // 获取文件大小
        System.out.println(fileSystem.getContentSummary(new Path("/data/file.txt")).getLength());
    }
}
