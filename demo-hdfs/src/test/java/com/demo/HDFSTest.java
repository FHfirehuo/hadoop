package com.demo;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class HDFSTest {

	private FileSystem fs;
	private Configuration conf;

	// YARN Web界面：http://192.168.66.9:8088/cluster
	// http://192.168.66.9:50070/dfshealth.html#tab-overview

	@Before
	public void init() throws IOException, InterruptedException, URISyntaxException {
		conf = new Configuration();

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.66.9:9000");
		System.setProperty("HADOOP_USER_NAME", "root");// 去掉会报Permission denied: user=liuyi27, access=WRITE,
														// inode="/":root:supergroup:drwxr-xr-x
		fs = FileSystem.get(conf);
	}

	/**
	 * 创建文件夹
	 * 
	 * @throws Exception
	 */
	@Test
	public void mkdir() throws Exception {
		boolean mkdirs = fs.mkdirs(new Path("/mkdir"));
		if (mkdirs) {
			System.out.println("创建文件夹成功");
		}
		fs.close();
	}

	/**
	 * 删除
	 * 
	 * @throws Exception
	 */
	@Test
	public void delete() throws Exception {
		// 递归删除
		boolean delete = fs.delete(new Path("/mkdir"), true);
		if (delete) {
			System.out.println("删除成功");
		}
		fs.close();
	}

	/**
	 * 上传
	 * 
	 * @throws Exception
	 */
	@Test
	public void upload() throws Exception {
		// 后面的true，是指如果文件存在，则覆盖
		FSDataOutputStream fout = fs.create(new Path("/xx.jpg"), true);
		InputStream in = new FileInputStream("C:/Users/Administrator/Pictures/1.png");

		// 复制流，并且完成之后关闭流
		IOUtils.copyBytes(in, fout, 1024, true);
	}

	/**
	 * 下载
	 * 
	 * @throws Exception
	 */
	@Test
	public void download() throws Exception {
		FSDataInputStream fin = fs.open(new Path("/xx.jpg"));

		OutputStream out = new FileOutputStream("d://axx.jpg");

		IOUtils.copyBytes(fin, out, 1024, true);
	}

	/**
	 * 在指定位置读写
	 * 
	 * @throws Exception
	 */
	@Test
	public void random() throws Exception {
		FSDataInputStream fin = fs.open(new Path("xx.jpg"));
		// 从12的位置开始读
		fin.seek(12);

		OutputStream out = new FileOutputStream("d://axx.jpg");

		IOUtils.copyBytes(fin, out, 1024, true);
	}

	/**
	 * 可以获取hadoop配置
	 * 
	 * @throws Exception
	 */
	@Test
	public void conf() throws Exception {
		Iterator<Entry<String, String>> iterator = conf.iterator();
		while (iterator.hasNext()) {
			Entry<String, String> entry = iterator.next();
			System.out.println(entry);
		}
	}

	/**
	 * 列出文件，可以递归所有文件，它是一个迭代器，因为客户端无法接收所有文件信息
	 */
	@Test
	public void listFile() throws Exception {
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		while (listFiles.hasNext()) {
			LocatedFileStatus lfs = listFiles.next();
			System.out.println("块大小:" + lfs.getBlockSize());
			System.out.println("所属组:" + lfs.getOwner());
			System.out.println("大小:" + lfs.getLen());
			System.out.println("文件名:" + lfs.getPath().getName());
			System.out.println("是否目录:" + lfs.isDirectory());
			System.out.println("是否文件:" + lfs.isFile());
			System.out.println();
			BlockLocation[] blockLocations = lfs.getBlockLocations();
			for (BlockLocation blockLocation : blockLocations) {
				System.out.println("块偏移数:" + blockLocation.getOffset());
				System.out.println("块长度:" + blockLocation.getLength());
				System.out.println("块名称:" + Arrays.toString(blockLocation.getNames()));
				System.out.println("块名称:" + Arrays.toString(blockLocation.getHosts()));
			}
			System.out.println("--------------------------");
		}
	}
	
	/**
     * 列出文件，但是不可以递归，所以可以直接用数组存储，
     * @throws Exception
     */
    @Test
    public void listFile2()throws Exception{
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : listStatus) {
            System.out.println("块大小:" + fileStatus.getBlockSize());
            System.out.println("所属组:" + fileStatus.getOwner());
            System.out.println("大小:" + fileStatus.getLen());
            System.out.println("文件名:" + fileStatus.getPath().getName());
            System.out.println("是否目录:" + fileStatus.isDirectory());
            System.out.println("是否文件:" + fileStatus.isFile());
        }
    }
}
